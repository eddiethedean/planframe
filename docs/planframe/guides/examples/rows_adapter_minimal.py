"""Minimal `BaseAdapter` example for `list[dict]` "frames".

See `../creating-an-adapter.md` for the full adapter checklist. `group_by_agg` is
stubbed: a real implementation must handle `CompiledJoinKey` tuples for *keys*
and `named_aggs` values that are either `(op, column)` or a compiled aggregation
expression from `AggExpr` (see `planframe.backend.adapter.BaseAdapter.group_by_agg`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from planframe.backend.adapter import (
    BaseAdapter,
    CompiledJoinKey,
    CompiledProjectItem,
    CompiledSortKey,
)
from planframe.expr.api import Expr, Lit
from planframe.frame import Frame
from planframe.plan.join_options import JoinOptions

RowsFrame = list[dict[str, object]]


class RowsAdapter(BaseAdapter[RowsFrame, Expr[object]]):
    name = "rows"

    def compile_expr(self, expr: Expr[object], *, schema: Any = None) -> Expr[object]:
        return expr

    def select(self, df: RowsFrame, columns: tuple[str, ...]) -> RowsFrame:
        cols = set(columns)
        return [{k: v for k, v in row.items() if k in cols} for row in df]

    def project(
        self, df: RowsFrame, items: tuple[CompiledProjectItem[Expr[object]], ...]
    ) -> RowsFrame:
        out: RowsFrame = []
        for row in df:
            r: dict[str, object] = {}
            for it in items:
                if it.from_column is not None:
                    r[it.name] = row[it.from_column]
                elif it.expr is not None and isinstance(it.expr, Lit):
                    r[it.name] = it.expr.value
                else:
                    raise NotImplementedError(
                        "RowsAdapter example project only supports column picks or lit expr"
                    )
            out.append(r)
        return out

    def drop(self, df: RowsFrame, columns: tuple[str, ...], *, strict: bool = True) -> RowsFrame:
        keys = set(df[0].keys()) if df else set()
        cols = set(columns) if strict else set(columns) & keys
        return [{k: v for k, v in row.items() if k not in cols} for row in df]

    def rename(self, df: RowsFrame, mapping: dict[str, str], *, strict: bool = True) -> RowsFrame:
        keys = set(df[0].keys()) if df else set()
        effective = dict(mapping) if strict else {k: v for k, v in mapping.items() if k in keys}
        out: RowsFrame = []
        for row in df:
            r = {}
            for k, v in row.items():
                r[effective.get(k, k)] = v
            out.append(r)
        return out

    def with_column(self, df: RowsFrame, name: str, expr: Expr[object]) -> RowsFrame:
        # Minimal example: only supports `lit(...)` expressions.
        if getattr(expr, "kind", None) != "lit":
            raise NotImplementedError("RowsAdapter example only supports lit(...) expressions")
        value = expr.value
        return [{**row, name: value} for row in df]

    def cast(self, df: RowsFrame, name: str, dtype: Any) -> RowsFrame:
        # No-op for this minimal example.
        return df

    def filter(self, df: RowsFrame, predicate: Expr[object]) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement filter")

    def sort(
        self,
        df: RowsFrame,
        keys: tuple[CompiledSortKey[Expr[object]], ...],
        *,
        descending: tuple[bool, ...],
        nulls_last: tuple[bool, ...],
    ) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement sort")

    def unique(
        self,
        df: RowsFrame,
        subset: tuple[str, ...] | None,
        *,
        keep: str = "first",
        maintain_order: bool = False,
    ) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement unique")

    def duplicated(
        self,
        df: RowsFrame,
        subset: tuple[str, ...] | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement duplicated")

    def group_by_agg(
        self,
        df: RowsFrame,
        *,
        keys: tuple[CompiledJoinKey[Expr[object]], ...],
        named_aggs: dict[str, tuple[str, str] | Expr[object]],
    ) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement group_by_agg")

    def drop_nulls(self, df: RowsFrame, subset: tuple[str, ...] | None) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement drop_nulls")

    def fill_null(self, df: RowsFrame, value: Any, subset: tuple[str, ...] | None) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement fill_null")

    def melt(
        self,
        df: RowsFrame,
        *,
        id_vars: tuple[str, ...],
        value_vars: tuple[str, ...],
        variable_name: str,
        value_name: str,
    ) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement melt")

    def join(
        self,
        left: RowsFrame,
        right: RowsFrame,
        *,
        left_on: tuple[CompiledJoinKey[Expr[object]], ...],
        right_on: tuple[CompiledJoinKey[Expr[object]], ...],
        how: str = "inner",
        suffix: str = "_right",
        options: JoinOptions | None = None,
    ) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement join")

    def slice(self, df: RowsFrame, *, offset: int, length: int | None) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement slice")

    def head(self, df: RowsFrame, n: int) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement head")

    def tail(self, df: RowsFrame, n: int) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement tail")

    def concat_vertical(self, left: RowsFrame, right: RowsFrame) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement concat_vertical")

    def pivot(
        self,
        df: RowsFrame,
        *,
        index: tuple[str, ...],
        on: str,
        values: str,
        agg: str = "first",
        on_columns: tuple[str, ...] | None = None,
        separator: str = "_",
    ) -> RowsFrame:
        raise NotImplementedError("RowsAdapter example does not implement pivot")

    def write_parquet(self, df: RowsFrame, path: str, **kwargs: Any) -> None:
        raise NotImplementedError

    def write_csv(self, df: RowsFrame, path: str, **kwargs: Any) -> None:
        raise NotImplementedError

    def write_ndjson(self, df: RowsFrame, path: str, **kwargs: Any) -> None:
        raise NotImplementedError

    def write_ipc(self, df: RowsFrame, path: str, **kwargs: Any) -> None:
        raise NotImplementedError

    def write_database(self, df: RowsFrame, **kwargs: Any) -> None:
        raise NotImplementedError

    def write_excel(self, df: RowsFrame, path: str, **kwargs: Any) -> None:
        raise NotImplementedError

    def write_delta(self, df: RowsFrame, target: str, **kwargs: Any) -> None:
        raise NotImplementedError

    def write_avro(self, df: RowsFrame, path: str, **kwargs: Any) -> None:
        raise NotImplementedError

    def explode(self, df: RowsFrame, column: str) -> RowsFrame:
        raise NotImplementedError

    def unnest(self, df: RowsFrame, column: str, *, fields: tuple[str, ...]) -> RowsFrame:
        raise NotImplementedError

    def concat_horizontal(self, left: RowsFrame, right: RowsFrame) -> RowsFrame:
        raise NotImplementedError

    def drop_nulls_all(self, df: RowsFrame, subset: tuple[str, ...] | None) -> RowsFrame:
        raise NotImplementedError

    def sample(
        self,
        df: RowsFrame,
        *,
        n: int | None = None,
        frac: float | None = None,
        with_replacement: bool = False,
        shuffle: bool = False,
        seed: int | None = None,
    ) -> RowsFrame:
        raise NotImplementedError

    def collect(self, df: RowsFrame) -> RowsFrame:
        return df

    def to_dicts(self, df: RowsFrame) -> list[dict[str, object]]:
        return list(df)

    def to_dict(self, df: RowsFrame) -> dict[str, list[object]]:
        out: dict[str, list[object]] = {}
        for row in df:
            for k, v in row.items():
                out.setdefault(k, []).append(v)
        return out


@dataclass(frozen=True)
class Users:
    id: int
    age: int


def main() -> None:
    adapter = RowsAdapter()
    src: RowsFrame = [{"id": 1, "age": 10}, {"id": 2, "age": 20}]
    pf = Frame.source(src, adapter=adapter, schema=Users)

    out = pf.select("id", "age")
    print(f"schema={out.schema().names()}")
    print(f"collect={out.collect()}")
    print(f"dicts={out.to_dicts()}")
    print(f"dict={out.to_dict()}")


if __name__ == "__main__":
    main()
