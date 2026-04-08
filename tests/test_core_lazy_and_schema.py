from __future__ import annotations

from dataclasses import dataclass
from functools import cmp_to_key
from typing import Any, cast

import pytest
from pydantic import BaseModel

from planframe.backend.adapter import (
    BackendAdapter,
    CompiledJoinKey,
    CompiledProjectItem,
    CompiledSortKey,
)
from planframe.backend.errors import (
    PlanFrameBackendError,
    PlanFrameExpressionError,
    PlanFrameSchemaError,
)
from planframe.expr import Expr, add, col, eq, lit, mul, over
from planframe.expr.api import Add, Col, Lit, Mul, StrLower, Sub
from planframe.frame import Frame
from planframe.plan.join_options import JoinOptions
from planframe.plan.nodes import Project
from planframe.schema.ir import collect_col_names_in_expr


@dataclass(frozen=True)
class UserDC:
    id: int
    age: int


class UserPD(BaseModel):
    id: int
    age: int


def _spy_sort_scalar_from_expr(row: dict[str, Any], expr: Any) -> Any:
    """Minimal PlanFrame expr evaluation for SpyAdapter.sort expression keys."""

    if isinstance(expr, Col):
        return row[expr.name]
    if isinstance(expr, Lit):
        return expr.value
    if isinstance(expr, Add):
        return _spy_sort_scalar_from_expr(row, expr.left) + _spy_sort_scalar_from_expr(
            row, expr.right
        )
    if isinstance(expr, Sub):
        return _spy_sort_scalar_from_expr(row, expr.left) - _spy_sort_scalar_from_expr(
            row, expr.right
        )
    if isinstance(expr, Mul):
        return _spy_sort_scalar_from_expr(row, expr.left) * _spy_sort_scalar_from_expr(
            row, expr.right
        )
    if isinstance(expr, StrLower):
        v = _spy_sort_scalar_from_expr(row, expr.value)
        return v.lower() if isinstance(v, str) else v
    raise NotImplementedError(f"SpyAdapter.sort expr not supported: {type(expr).__name__}")


class SpyAdapter(BackendAdapter[list[dict[str, Any]], object]):
    name = "spy"

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def select(self, df: list[dict[str, Any]], columns: tuple[str, ...]) -> list[dict[str, Any]]:
        self.calls.append(("select", columns))
        return [{k: row[k] for k in columns} for row in df]

    def project(
        self,
        df: list[dict[str, Any]],
        items: tuple[CompiledProjectItem[object], ...],
    ) -> list[dict[str, Any]]:
        self.calls.append(("project", tuple(items)))
        out: list[dict[str, Any]] = []
        for row in df:
            row2: dict[str, Any] = {}
            for it in items:
                if it.from_column is not None:
                    row2[it.name] = row[it.from_column]
                else:
                    row2[it.name] = "computed"
            out.append(row2)
        return out

    def drop(
        self, df: list[dict[str, Any]], columns: tuple[str, ...], *, strict: bool = True
    ) -> list[dict[str, Any]]:
        self.calls.append(("drop", (columns, strict)))
        keys = set(df[0].keys()) if df else set()
        drop_set = set(columns) if strict else set(columns) & keys
        return [{k: v for k, v in row.items() if k not in drop_set} for row in df]

    def rename(
        self,
        df: list[dict[str, Any]],
        mapping: dict[str, str],
        *,
        strict: bool = True,
    ) -> list[dict[str, Any]]:
        self.calls.append(("rename", (dict(mapping), strict)))
        keys = set(df[0].keys()) if df else set()
        if strict:
            missing = set(mapping.keys()).difference(keys)
            if missing:
                raise PlanFrameBackendError(f"rename: unknown columns: {sorted(missing)}")
            effective = dict(mapping)
        else:
            effective = {k: v for k, v in mapping.items() if k in keys}
        out: list[dict[str, Any]] = []
        for row in df:
            row2: dict[str, Any] = {}
            for k, v in row.items():
                row2[effective.get(k, k)] = v
            out.append(row2)
        return out

    def with_column(
        self, df: list[dict[str, Any]], name: str, expr: object
    ) -> list[dict[str, Any]]:
        self.calls.append(("with_column", name))
        return [{**row, name: "computed"} for row in df]

    def cast(self, df: list[dict[str, Any]], name: str, dtype: Any) -> list[dict[str, Any]]:
        self.calls.append(("cast", (name, dtype)))
        return df

    def filter(self, df: list[dict[str, Any]], predicate: object) -> list[dict[str, Any]]:
        self.calls.append(("filter", predicate))
        return df[:1]

    def compile_expr(self, expr: Any) -> object:
        self.calls.append(("compile_expr", type(expr).__name__))
        return expr

    def sort(
        self,
        df: list[dict[str, Any]],
        keys: tuple[CompiledSortKey[object], ...],
        *,
        descending: tuple[bool, ...],
        nulls_last: tuple[bool, ...],
    ) -> list[dict[str, Any]]:
        self.calls.append(("sort", (keys, descending, nulls_last)))
        if not keys:
            return list(df)
        if len(descending) != len(keys) or len(nulls_last) != len(keys):
            raise ValueError("SpyAdapter.sort: flag tuple lengths must match keys")

        def cmp_vals(va: Any, vb: Any, desc: bool, nl: bool) -> int:
            na, nb = va is None, vb is None
            if na and nb:
                return 0
            if na:
                return 1 if nl else -1
            if nb:
                return -1 if nl else 1
            if va == vb:
                return 0
            if not desc:
                return -1 if va < vb else 1
            return -1 if va > vb else 1

        def cmp_rows(ra: dict[str, Any], rb: dict[str, Any]) -> int:
            for k, desc, nl in zip(keys, descending, nulls_last, strict=True):
                if k.column is not None:
                    va, vb = ra.get(k.column), rb.get(k.column)
                else:
                    va = _spy_sort_scalar_from_expr(ra, k.expr)
                    vb = _spy_sort_scalar_from_expr(rb, k.expr)
                r = cmp_vals(va, vb, desc, nl)
                if r != 0:
                    return r
            return 0

        return sorted(df, key=cmp_to_key(cmp_rows))

    def unique(
        self,
        df: list[dict[str, Any]],
        subset: tuple[str, ...] | None,
        *,
        keep: str = "first",
        maintain_order: bool = False,
    ) -> list[dict[str, Any]]:
        self.calls.append(("unique", (subset, keep, maintain_order)))
        seen: set[tuple[Any, ...]] = set()
        out: list[dict[str, Any]] = []
        for row in df:
            key = tuple(row[c] for c in (subset or tuple(row.keys())))
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
        if keep == "last":
            # crude: reverse pass
            seen2: set[tuple[Any, ...]] = set()
            out2: list[dict[str, Any]] = []
            for row in reversed(df):
                key = tuple(row[c] for c in (subset or tuple(row.keys())))
                if key in seen2:
                    continue
                seen2.add(key)
                out2.append(row)
            out2.reverse()
            return out2
        return out

    def sample(
        self,
        df: list[dict[str, Any]],
        *,
        n: int | None = None,
        frac: float | None = None,
        with_replacement: bool = False,
        shuffle: bool = False,
        seed: int | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append(("sample", (n, frac, with_replacement, shuffle, seed)))
        # Deterministic/minimal: just take head(n) or keep original for frac.
        if n is not None:
            return df[:n]
        if frac is None:
            return df
        k = int(len(df) * frac)
        return df[:k]

    def duplicated(
        self,
        df: list[dict[str, Any]],
        subset: tuple[str, ...] | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> list[dict[str, Any]]:
        self.calls.append(("duplicated", (subset, keep, out_name)))
        # minimal: mark duplicates after first occurrence
        seen: set[tuple[Any, ...]] = set()
        out: list[dict[str, Any]] = []
        for row in df:
            key = tuple(row[c] for c in (subset or tuple(row.keys())))
            is_dup = key in seen
            seen.add(key)
            out.append({out_name: is_dup})
        return out

    def group_by_agg(
        self,
        df: list[dict[str, Any]],
        *,
        keys: tuple[str, ...],
        named_aggs: dict[str, tuple[str, str]],
    ) -> list[dict[str, Any]]:
        self.calls.append(("group_by_agg", (keys, dict(named_aggs))))
        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in df:
            k = tuple(row[x] for x in keys)
            groups.setdefault(k, []).append(row)
        out: list[dict[str, Any]] = []
        for k, rows in groups.items():
            base = {keys[i]: k[i] for i in range(len(keys))}
            for out_name, (op, col_name) in named_aggs.items():
                vals = [r[col_name] for r in rows]
                if op == "count":
                    base[out_name] = len(vals)
                elif op == "sum":
                    base[out_name] = sum(vals)  # type: ignore[arg-type]
                else:
                    base[out_name] = None
            out.append(dict(base))
        return out

    def drop_nulls(
        self, df: list[dict[str, Any]], subset: tuple[str, ...] | None
    ) -> list[dict[str, Any]]:
        self.calls.append(("drop_nulls", subset))
        cols = subset or tuple(df[0].keys())
        return [r for r in df if all(r.get(c) is not None for c in cols)]

    def fill_null(
        self, df: list[dict[str, Any]], value: Any, subset: tuple[str, ...] | None
    ) -> list[dict[str, Any]]:
        self.calls.append(("fill_null", (value, subset)))
        cols = subset or tuple(df[0].keys())
        out: list[dict[str, Any]] = []
        for r in df:
            r2 = dict(r)
            for c in cols:
                if r2.get(c) is None:
                    r2[c] = value
            out.append(r2)
        return out

    def melt(
        self,
        df: list[dict[str, Any]],
        *,
        id_vars: tuple[str, ...],
        value_vars: tuple[str, ...],
        variable_name: str,
        value_name: str,
    ) -> list[dict[str, Any]]:
        self.calls.append(("melt", (id_vars, value_vars, variable_name, value_name)))
        out: list[dict[str, Any]] = []
        for r in df:
            id_part = {k: r[k] for k in id_vars}
            for vv in value_vars:
                out.append({**id_part, variable_name: vv, value_name: r[vv]})
        return out

    def join(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
        *,
        left_on: tuple[CompiledJoinKey[object], ...],
        right_on: tuple[CompiledJoinKey[object], ...],
        how: str = "inner",
        suffix: str = "_right",
        options: JoinOptions | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append(("join", (left_on, right_on, how, suffix, options)))
        if how == "cross":
            out: list[dict[str, Any]] = []
            for lr in left:
                for rr in right:
                    row = dict(lr)
                    for rk, rv in rr.items():
                        nk = rk
                        if nk in row:
                            nk = f"{nk}{suffix}"
                        row[nk] = rv
                    out.append(row)
            return out
        if how != "inner":
            raise NotImplementedError("SpyAdapter only implements inner and cross join for tests")

        def join_key(row: dict[str, Any], keys: tuple[CompiledJoinKey[object], ...]) -> tuple[Any, ...]:
            parts: list[Any] = []
            for k in keys:
                if k.column is not None:
                    parts.append(row[k.column])
                else:
                    parts.append(_spy_sort_scalar_from_expr(row, k.expr))
            return tuple(parts)

        right_skip: set[str] = set()
        for k in right_on:
            if k.column is not None:
                right_skip.add(k.column)
            elif k.expr is not None:
                right_skip.update(collect_col_names_in_expr(k.expr))

        right_index: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for r in right:
            rk = join_key(r, right_on)
            right_index.setdefault(rk, []).append(r)
        out2: list[dict[str, Any]] = []
        for left_row in left:
            lk = join_key(left_row, left_on)
            matches = right_index.get(lk, [])
            for r in matches:
                row = dict(left_row)
                for rk, rv in r.items():
                    if rk in right_skip:
                        continue
                    out_key = rk
                    if out_key in row:
                        out_key = f"{out_key}{suffix}"
                    row[out_key] = rv
                out2.append(row)
        return out2

    def slice(
        self,
        df: list[dict[str, Any]],
        *,
        offset: int,
        length: int | None,
    ) -> list[dict[str, Any]]:
        self.calls.append(("slice", (offset, length)))
        return df[offset:] if length is None else df[offset : offset + length]

    def head(self, df: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
        self.calls.append(("head", n))
        return df[:n]

    def tail(self, df: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
        self.calls.append(("tail", n))
        return df[-n:] if n else []

    def concat_vertical(
        self, left: list[dict[str, Any]], right: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        self.calls.append(("concat_vertical", None))
        return [*left, *right]

    def concat_horizontal(
        self, left: list[dict[str, Any]], right: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        self.calls.append(("concat_horizontal", None))
        out: list[dict[str, Any]] = []
        for left_row, right_row in zip(left, right, strict=True):
            overlap = set(left_row.keys()).intersection(right_row.keys())
            if overlap:
                raise ValueError("overlap")
            out.append({**left_row, **right_row})
        return out

    def pivot(
        self,
        df: list[dict[str, Any]],
        *,
        index: tuple[str, ...],
        on: str,
        values: str,
        agg: str = "first",
        on_columns: tuple[str, ...] | None = None,
        separator: str = "_",
    ) -> list[dict[str, Any]]:
        self.calls.append(("pivot", (index, on, values, agg, on_columns, separator)))
        if on_columns is None:
            raise NotImplementedError("SpyAdapter pivot requires on_columns")
        out: dict[Any, dict[str, Any]] = {}
        for row in df:
            k = tuple(row[c] for c in index)
            rec = out.setdefault(k, {index[i]: k[i] for i in range(len(index))})
            colname = row[on]
            if colname in on_columns:
                rec[colname] = row[values]
        return list(out.values())

    def explode(self, df: list[dict[str, Any]], column: str) -> list[dict[str, Any]]:
        self.calls.append(("explode", column))
        out: list[dict[str, Any]] = []
        for r in df:
            vals = r.get(column) or []
            for v in vals:
                r2 = dict(r)
                r2[column] = v
                out.append(r2)
        return out

    def unnest(self, df: list[dict[str, Any]], column: str) -> list[dict[str, Any]]:
        self.calls.append(("unnest", column))
        out: list[dict[str, Any]] = []
        for r in df:
            s = r.get(column) or {}
            r2 = dict(r)
            r2.pop(column, None)
            if isinstance(s, dict):
                r2.update(s)
            out.append(r2)
        return out

    def drop_nulls_all(
        self, df: list[dict[str, Any]], subset: tuple[str, ...] | None
    ) -> list[dict[str, Any]]:
        self.calls.append(("drop_nulls_all", subset))
        cols = subset or tuple(df[0].keys())
        return [r for r in df if not all(r.get(c) is None for c in cols)]

    def write_parquet(
        self,
        df: list[dict[str, Any]],
        path: str,
        *,
        compression: str = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        self.calls.append(
            ("write_parquet", (path, compression, row_group_size, partition_by, storage_options))
        )

    def write_csv(
        self,
        df: list[dict[str, Any]],
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        self.calls.append(("write_csv", (path, separator, include_header, storage_options)))

    def write_ndjson(
        self, df: list[dict[str, Any]], path: str, *, storage_options: dict[str, Any] | None = None
    ) -> None:
        self.calls.append(("write_ndjson", (path, storage_options)))

    def write_ipc(
        self,
        df: list[dict[str, Any]],
        path: str,
        *,
        compression: str = "uncompressed",
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        self.calls.append(("write_ipc", (path, compression, storage_options)))

    def write_database(
        self,
        df: list[dict[str, Any]],
        *,
        table_name: str,
        connection: Any,
        if_table_exists: str = "fail",
        engine: str | None = None,
    ) -> None:
        self.calls.append(("write_database", (table_name, if_table_exists, engine)))

    def write_excel(
        self, df: list[dict[str, Any]], path: str, *, worksheet: str = "Sheet1"
    ) -> None:
        self.calls.append(("write_excel", (path, worksheet)))

    def write_delta(
        self,
        df: list[dict[str, Any]],
        target: str,
        *,
        mode: str = "error",
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        self.calls.append(("write_delta", (target, mode, storage_options)))

    def write_avro(
        self,
        df: list[dict[str, Any]],
        path: str,
        *,
        compression: str = "uncompressed",
        name: str = "",
    ) -> None:
        self.calls.append(("write_avro", (path, compression, name)))

    def collect(self, df: list[dict[str, Any]]) -> list[dict[str, Any]]:
        self.calls.append(("collect", None))
        return df

    def to_dicts(self, df: list[dict[str, Any]]) -> list[dict[str, object]]:
        self.calls.append(("to_dicts", None))
        return cast(list[dict[str, object]], df)

    def to_dict(self, df: list[dict[str, Any]]) -> dict[str, list[object]]:
        self.calls.append(("to_dict", None))
        if not df:
            return {}
        cols = {k: [] for k in df[0]}
        for row in df:
            for k, v in row.items():
                cols[k].append(v)
        return cols


def test_always_lazy_no_adapter_calls_until_collect() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": 2}, {"id": 2, "age": 3}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    out = (
        pf.select("id", "age")
        .with_column("age_plus_one", add(col("age"), lit(1)))
        .rename(age="years")
        .filter(eq(col("id"), lit(1)))
    )

    assert adapter.calls == []

    collected = out.collect()
    assert collected == [{"id": 1, "years": 2, "age_plus_one": "computed"}]

    # Ensure evaluation actually invoked the adapter at the boundary.
    assert [c[0] for c in adapter.calls] == [
        "select",
        "compile_expr",
        "with_column",
        "rename",
        "compile_expr",
        "filter",
        "collect",
    ]


def test_always_lazy_with_new_ops() -> None:
    adapter = SpyAdapter()
    data = [{"id": 2, "age": None}, {"id": 1, "age": 3}, {"id": 1, "age": 3}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    out = (
        pf.fill_null(0, "age")
        .drop_nulls("age")
        .sort("id", nulls_last=True)
        .unique("id", keep="first", maintain_order=True)
        .duplicated("id")
    )

    assert adapter.calls == []
    collected = out.collect()
    assert collected == [{"duplicated": False}, {"duplicated": False}]


def test_schema_from_pydantic_is_supported() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserPD)
    assert pf.schema().names() == ("id", "age")


def test_schema_errors_select_drop_rename() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    with pytest.raises(PlanFrameSchemaError):
        pf.select("missing")

    with pytest.raises(PlanFrameSchemaError):
        pf.drop("missing")

    with pytest.raises(PlanFrameSchemaError):
        pf.rename(missing="x")


def test_select_mixed_str_and_expr_single_project_node() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)
    out = pf.select("id", ("doubled", mul(col("age"), lit(2))))
    assert out.schema().names() == ("id", "doubled")
    assert isinstance(out.plan(), Project)
    assert adapter.calls == []
    collected = out.collect()
    assert collected == [{"id": 1, "doubled": "computed"}]
    assert [c[0] for c in adapter.calls[:-1]] == ["compile_expr", "project"]


def test_project_rejects_duplicate_output_names() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)
    with pytest.raises(PlanFrameSchemaError, match="repeats output column"):
        pf.select("id", ("id", lit(1)))


def test_rename_strict_false_ignores_unknown_columns() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    out = pf.rename(id="pk", ghost="y", strict=False)
    assert out.schema().names() == ("pk", "age")
    assert adapter.calls == []

    collected = out.collect()
    assert collected == [{"pk": 1, "age": 2}]
    assert adapter.calls == [
        ("rename", ({"id": "pk", "ghost": "y"}, False)),
        ("collect", None),
    ]


def test_rename_strict_false_only_renames_existing_keys() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    out = pf.rename(age="years", missing="x", strict=False)
    assert out.schema().names() == ("id", "years")
    collected = out.collect()
    assert collected == [{"id": 1, "years": 2}]
    assert adapter.calls[0] == (
        "rename",
        ({"age": "years", "missing": "x"}, False),
    )


def test_drop_strict_false_ignores_unknown_columns() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    out = pf.drop("only_if_present", strict=False)
    assert out.schema().names() == ("id", "age")
    assert adapter.calls == []

    collected = out.collect()
    assert collected == [{"id": 1, "age": 2}]
    assert adapter.calls == [
        ("drop", (("only_if_present",), False)),
        ("collect", None),
    ]


def test_drop_strict_false_drops_present_and_ignores_missing() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    out = pf.drop("age", "ghost", strict=False)
    assert out.schema().names() == ("id",)
    collected = out.collect()
    assert collected == [{"id": 1}]
    assert adapter.calls[0] == ("drop", (("age", "ghost"), False))

    # rename collision
    with pytest.raises(PlanFrameSchemaError):
        pf.rename(id="age")


def test_sort_accepts_per_key_descending_and_nulls_last() -> None:
    adapter = SpyAdapter()
    pf = Frame.source(
        [
            {"id": 1, "age": 10},
            {"id": 2, "age": 20},
            {"id": 1, "age": 30},
        ],
        adapter=adapter,
        schema=UserDC,
    )
    out = pf.sort("id", "age", descending=[False, True], nulls_last=[True, True])
    collected = out.collect()
    assert collected == [
        {"id": 1, "age": 30},
        {"id": 1, "age": 10},
        {"id": 2, "age": 20},
    ]
    assert adapter.calls[0] == (
        "sort",
        (
            (
                CompiledSortKey(column="id", expr=None),
                CompiledSortKey(column="age", expr=None),
            ),
            (False, True),
            (True, True),
        ),
    )


def test_sort_accepts_expression_keys() -> None:
    adapter = SpyAdapter()
    pf = Frame.source(
        [
            {"id": 1, "age": 10},
            {"id": 2, "age": 5},
            {"id": 3, "age": 20},
        ],
        adapter=adapter,
        schema=UserDC,
    )
    out = pf.sort(add(col("id"), col("age")))
    collected = out.collect()
    assert [r["id"] for r in collected] == [2, 1, 3]

    out2 = pf.sort("id", add(col("age"), lit(0)), descending=[True, False])
    collected2 = out2.collect()
    assert collected2[0]["id"] == 3


def test_sort_rejects_flag_sequences_with_wrong_length() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)
    with pytest.raises(ValueError, match="descending"):
        _ = pf.sort("id", "age", descending=[True])
    with pytest.raises(ValueError, match="nulls_last"):
        _ = pf.sort("id", "age", nulls_last=(True,))


def test_sort_rejects_non_bool_flag_sequence_elements() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)
    with pytest.raises(TypeError, match="descending"):
        _ = pf.sort("id", "age", descending=[True, "no"])  # type: ignore[list-item]


def test_schema_ordering_helpers_errors() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    with pytest.raises(PlanFrameSchemaError):
        pf.reorder_columns("id")  # missing age

    with pytest.raises(PlanFrameSchemaError):
        pf.move("id")  # neither before nor after

    with pytest.raises(PlanFrameSchemaError):
        pf.move("id", before="missing")


def test_row_ops_reject_negative_n_and_length() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)
    with pytest.raises(ValueError):
        pf.head(-1)
    with pytest.raises(ValueError):
        pf.tail(-1)
    with pytest.raises(ValueError):
        pf.slice(0, -1)


def test_join_is_always_lazy_and_schema_merge_rules() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class Left:
        id: int
        age: int
        name: str

    left = Frame.source(
        [{"id": 1, "age": 10, "name": "a"}, {"id": 2, "age": 20, "name": "b"}],
        adapter=adapter,
        schema=Left,
    )

    @dataclass(frozen=True)
    class Right:
        id: int
        name: str

    right = Frame.source([{"id": 1, "name": "x"}], adapter=adapter, schema=Right)

    out = left.join(right, on=("id",))
    assert adapter.calls == []
    assert out.schema().names() == ("id", "age", "name", "name_right")

    collected = out.collect()
    assert collected == [{"id": 1, "age": 10, "name": "a", "name_right": "x"}]
    assert [c[0] for c in adapter.calls] == [
        "join",
        "collect",
    ]


def test_join_asymmetric_keys_inner() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class Left:
        user_id: int
        x: int

    @dataclass(frozen=True)
    class Right:
        id: int
        y: int

    left = Frame.source(
        [{"user_id": 1, "x": 10}, {"user_id": 2, "x": 20}],
        adapter=adapter,
        schema=Left,
    )
    right = Frame.source([{"id": 1, "y": 100}], adapter=adapter, schema=Right)

    out = left.join(right, left_on=("user_id",), right_on=("id",), how="inner")
    assert adapter.calls == []
    assert out.schema().names() == ("user_id", "x", "y")

    collected = out.collect()
    assert collected == [{"user_id": 1, "x": 10, "y": 100}]
    assert adapter.calls[0][1][0] == (CompiledJoinKey(column="user_id", expr=None),)
    assert adapter.calls[0][1][1] == (CompiledJoinKey(column="id", expr=None),)


def test_join_expression_keys_inner() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class Left:
        id: int
        email: str

    @dataclass(frozen=True)
    class Right:
        id: int
        email_norm: str

    left = Frame.source(
        [{"id": 1, "email": "A@x.com"}, {"id": 2, "email": "b@x.com"}],
        adapter=adapter,
        schema=Left,
    )
    right = Frame.source(
        [{"id": 10, "email_norm": "a@x.com"}, {"id": 20, "email_norm": "b@x.com"}],
        adapter=adapter,
        schema=Right,
    )
    from planframe.expr import lower

    out = left.join(
        right,
        left_on=(lower(col("email")),),
        right_on=(lower(col("email_norm")),),
        how="inner",
    )
    collected = out.collect()
    assert len(collected) == 2
    assert {r["id"] for r in collected} == {1, 2}


def test_join_cross_is_lazy_and_cartesian_in_spy() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class A:
        a: int

    @dataclass(frozen=True)
    class B:
        b: int

    left = Frame.source([{"a": 1}, {"a": 2}], adapter=adapter, schema=A)
    right = Frame.source([{"b": 10}], adapter=adapter, schema=B)
    out = left.join(right, how="cross")
    assert adapter.calls == []
    assert out.schema().names() == ("a", "b")

    collected = out.collect()
    assert collected == [{"a": 1, "b": 10}, {"a": 2, "b": 10}]


def test_join_rejects_conflicting_key_arguments() -> None:
    adapter = SpyAdapter()
    left = Frame.source([{"id": 1}], adapter=adapter, schema=UserDC)
    right = Frame.source([{"id": 1}], adapter=adapter, schema=UserDC)
    with pytest.raises(ValueError, match="either on"):
        left.join(right, on=("id",), left_on=("id",), right_on=("id",))  # type: ignore[call-overload]


def test_row_ops_are_always_lazy() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": 2}, {"id": 2, "age": 3}, {"id": 3, "age": 4}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    out = pf.head(2).slice(1, 1).tail(1).limit(1)
    assert adapter.calls == []

    collected = out.collect()
    assert collected == [{"id": 2, "age": 3}]
    assert [c[0] for c in adapter.calls] == ["head", "slice", "tail", "head", "collect"]


def test_pattern_select_drop_compile_to_select_drop_and_are_lazy() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class S:
        id: int
        x_a: int
        x_b: int
        y: int

    data = [{"id": 1, "x_a": 10, "x_b": 20, "y": 30}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    out = pf.select_prefix("x_").drop_suffix("_b")
    assert adapter.calls == []
    assert out.schema().names() == ("x_a",)

    collected = out.collect()
    assert collected == [{"x_a": 10}]
    assert [c[0] for c in adapter.calls] == ["select", "drop", "collect"]


def test_concat_vertical_is_lazy_and_validates_schema() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class S:
        id: int
        age: int

    left = Frame.source([{"id": 1, "age": 10}], adapter=adapter, schema=S)
    right = Frame.source([{"id": 2, "age": 20}], adapter=adapter, schema=S)

    out = left.concat_vertical(right)
    assert adapter.calls == []
    assert out.schema().names() == ("id", "age")

    collected = out.collect()
    assert collected == [{"id": 1, "age": 10}, {"id": 2, "age": 20}]
    assert [c[0] for c in adapter.calls] == ["concat_vertical", "collect"]

    @dataclass(frozen=True)
    class S2:
        id: int
        x: int

    bad = Frame.source([{"id": 3, "x": 1}], adapter=adapter, schema=S2)
    with pytest.raises(PlanFrameSchemaError):
        left.concat_vertical(bad)  # type: ignore[arg-type]


def test_concat_vertical_rejects_dtype_mismatch() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class S1:
        id: int
        age: int

    @dataclass(frozen=True)
    class S2:
        id: int
        age: str

    left = Frame.source([{"id": 1, "age": 10}], adapter=adapter, schema=S1)
    right = Frame.source([{"id": 2, "age": "x"}], adapter=adapter, schema=S2)
    with pytest.raises(PlanFrameSchemaError):
        left.concat_vertical(right)  # type: ignore[arg-type]


def test_pivot_is_always_lazy() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class S:
        id: int
        k: str
        v: int

    data = [{"id": 1, "k": "a", "v": 10}, {"id": 1, "k": "b", "v": 20}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    out = pf.pivot(index=("id",), on="k", values="v", on_columns=("a", "b"))
    assert adapter.calls == []

    collected = out.collect()
    assert collected == [{"id": 1, "a": 10, "b": 20}]
    assert [c[0] for c in adapter.calls] == ["pivot", "collect"]


def test_call_order_for_mixed_ops_including_pivot_and_concat() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class S:
        id: int
        k: str
        v: int

    left = Frame.source(
        [{"id": 1, "k": "a", "v": 10}, {"id": 1, "k": "b", "v": 20}],
        adapter=adapter,
        schema=S,
    )
    right = Frame.source(
        [{"id": 2, "k": "a", "v": 30}, {"id": 2, "k": "b", "v": 40}],
        adapter=adapter,
        schema=S,
    )

    out = (
        left.concat_vertical(right)
        .pivot(index=("id",), on="k", values="v", on_columns=("a", "b"))
        .head(1)
    )
    assert adapter.calls == []
    collected = out.collect()
    assert collected == [{"id": 1, "a": 10, "b": 20}]
    assert [c[0] for c in adapter.calls] == ["concat_vertical", "pivot", "head", "collect"]


def test_new_transforms_are_lazy() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class S:
        id: int
        x: int
        lst: object
        s: object

    data = [{"id": 1, "x": 1, "lst": [1, 2], "s": {"a": 1, "b": None}}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    out = (
        pf.select("id", "x")
        .union_distinct(pf.select("id", "x"))
        .concat_horizontal(pf.select("lst", "s"))
        .concat_horizontal(pf.select("id").rename(id="id2"))
        .explode("lst")
        .unnest("s", fields=("a", "b"))
        .drop_nulls_all("a", "b")
    )
    assert adapter.calls == []
    _ = out.collect()


def test_write_methods_execute_and_are_boundaries(tmp_path: Any) -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)
    out = pf.select("id").head(1)
    assert adapter.calls == []

    out.write_parquet(str(tmp_path / "x.parquet"))
    assert [c[0] for c in adapter.calls] == ["select", "head", "write_parquet"]

    adapter.calls.clear()
    out.write_csv(str(tmp_path / "x.csv"))
    assert [c[0] for c in adapter.calls] == ["select", "head", "write_csv"]


def test_backend_compile_expr_type_guard() -> None:
    class StrictAdapter(SpyAdapter):
        def compile_expr(self, expr: Any) -> object:
            if not isinstance(expr, Expr):
                raise TypeError("Expected Expr")
            return super().compile_expr(expr)

    adapter = StrictAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    # Force a non-Expr into the plan by bypassing typing (runtime misuse).
    with pytest.raises(PlanFrameBackendError):
        # type: ignore[arg-type]
        pf.with_column("x", "not_an_expr").collect()


def test_over_rejects_empty_order_by_tuple() -> None:
    with pytest.raises(PlanFrameExpressionError):
        over(col("age"), partition_by=("id",), order_by=())


def test_to_dicts_and_to_dict_are_lazy_boundaries() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": 2}, {"id": 2, "age": 3}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    out = pf.select("id").sort("id", nulls_last=True)
    assert adapter.calls == []

    dicts = out.to_dicts()
    assert dicts == [{"id": 1}, {"id": 2}]
    assert [c[0] for c in adapter.calls] == ["select", "sort", "to_dicts"]

    adapter.calls.clear()
    d = out.to_dict()
    assert d == {"id": [1, 2]}
    assert [c[0] for c in adapter.calls] == ["select", "sort", "to_dict"]


def test_sample_validates_arguments() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    with pytest.raises(ValueError):
        pf.sample()
    with pytest.raises(ValueError):
        pf.sample(1, frac=0.5)
    with pytest.raises(ValueError):
        pf.sample(-1)
    with pytest.raises(ValueError):
        pf.sample(frac=-0.1)
    with pytest.raises(ValueError):
        pf.sample(frac=1.1)
    # allowed when with_replacement=True
    _ = pf.sample(frac=1.1, with_replacement=True)


def test_sample_is_lazy_until_collect() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": 2}, {"id": 2, "age": 3}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    out = pf.sample(1, seed=1, shuffle=True).select("id")
    assert adapter.calls == []
    collected = out.collect()
    assert collected == [{"id": 1}]
    assert [c[0] for c in adapter.calls] == ["sample", "select", "collect"]
