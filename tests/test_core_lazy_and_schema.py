from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from pydantic import BaseModel

from planframe.backend.adapter import BackendAdapter
from planframe.backend.errors import PlanFrameBackendError, PlanFrameExpressionError, PlanFrameSchemaError
from planframe.expr import Expr, add, col, eq, lit
from planframe.frame import Frame


@dataclass(frozen=True)
class UserDC:
    id: int
    age: int


class UserPD(BaseModel):
    id: int
    age: int


class SpyAdapter(BackendAdapter[list[dict[str, Any]], object]):
    name = "spy"

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def select(self, df: list[dict[str, Any]], columns: tuple[str, ...]) -> list[dict[str, Any]]:
        self.calls.append(("select", columns))
        return [{k: row[k] for k in columns} for row in df]

    def drop(self, df: list[dict[str, Any]], columns: tuple[str, ...]) -> list[dict[str, Any]]:
        self.calls.append(("drop", columns))
        drop_set = set(columns)
        return [{k: v for k, v in row.items() if k not in drop_set} for row in df]

    def rename(self, df: list[dict[str, Any]], mapping: dict[str, str]) -> list[dict[str, Any]]:
        self.calls.append(("rename", dict(mapping)))
        out: list[dict[str, Any]] = []
        for row in df:
            row2: dict[str, Any] = {}
            for k, v in row.items():
                row2[mapping.get(k, k)] = v
            out.append(row2)
        return out

    def with_column(self, df: list[dict[str, Any]], name: str, expr: object) -> list[dict[str, Any]]:
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

    def sort(self, df: list[dict[str, Any]], columns: tuple[str, ...], *, descending: bool = False) -> list[dict[str, Any]]:
        self.calls.append(("sort", (columns, descending)))
        out = sorted(df, key=lambda r: tuple(r[c] for c in columns), reverse=descending)
        return out

    def unique(
        self, df: list[dict[str, Any]], subset: tuple[str, ...] | None, *, keep: str = "first"
    ) -> list[dict[str, Any]]:
        self.calls.append(("unique", (subset, keep)))
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
        self, df: list[dict[str, Any]], *, keys: tuple[str, ...], named_aggs: dict[str, tuple[str, str]]
    ) -> list[dict[str, Any]]:
        self.calls.append(("group_by_agg", (keys, dict(named_aggs))))
        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in df:
            k = tuple(row[x] for x in keys)
            groups.setdefault(k, []).append(row)
        out: list[dict[str, Any]] = []
        for k, rows in groups.items():
            base = {keys[i]: k[i] for i in range(len(keys))}
            for out_name, (op, col) in named_aggs.items():
                vals = [r[col] for r in rows]
                if op == "count":
                    base[out_name] = len(vals)
                elif op == "sum":
                    base[out_name] = sum(vals)  # type: ignore[arg-type]
                else:
                    base[out_name] = None
            out.append(dict(base))
        return out

    def drop_nulls(self, df: list[dict[str, Any]], subset: tuple[str, ...] | None) -> list[dict[str, Any]]:
        self.calls.append(("drop_nulls", subset))
        cols = subset or tuple(df[0].keys())
        return [r for r in df if all(r.get(c) is not None for c in cols)]

    def fill_null(self, df: list[dict[str, Any]], value: Any, subset: tuple[str, ...] | None) -> list[dict[str, Any]]:
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
        on: tuple[str, ...],
        how: str = "inner",
        suffix: str = "_right",
    ) -> list[dict[str, Any]]:
        self.calls.append(("join", (on, how, suffix)))
        if how != "inner":
            raise NotImplementedError("SpyAdapter only implements inner join for tests")
        right_index: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for r in right:
            k = tuple(r[c] for c in on)
            right_index.setdefault(k, []).append(r)
        out: list[dict[str, Any]] = []
        for l in left:
            lk = tuple(l[c] for c in on)
            matches = right_index.get(lk, [])
            for r in matches:
                row = dict(l)
                for rk, rv in r.items():
                    if rk in on:
                        continue
                    out_key = rk
                    if out_key in row:
                        out_key = f"{out_key}{suffix}"
                    row[out_key] = rv
                out.append(row)
        return out

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

    def collect(self, df: list[dict[str, Any]]) -> list[dict[str, Any]]:
        self.calls.append(("collect", None))
        return df


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
        .sort("id")
        .unique("id", keep="first")
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

    # rename collision
    with pytest.raises(PlanFrameSchemaError):
        pf.rename(id="age")


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

    out = left.concat_vertical(right).pivot(index=("id",), on="k", values="v", on_columns=("a", "b")).head(1)
    assert adapter.calls == []
    collected = out.collect()
    assert collected == [{"id": 1, "a": 10, "b": 20}]
    assert [c[0] for c in adapter.calls] == ["concat_vertical", "pivot", "head", "collect"]


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

