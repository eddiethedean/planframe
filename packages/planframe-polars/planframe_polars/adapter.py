from __future__ import annotations

from typing import Any

import polars as pl

from planframe.backend.adapter import BackendAdapter
from planframe.expr.api import Expr
from planframe_polars.compile_expr import compile_expr

PolarsFrame = pl.DataFrame | pl.LazyFrame


class PolarsAdapter(BackendAdapter[PolarsFrame, pl.Expr]):
    name = "polars"

    def select(self, df: PolarsFrame, columns: tuple[str, ...]) -> PolarsFrame:
        return df.select(list(columns))

    def drop(self, df: PolarsFrame, columns: tuple[str, ...]) -> PolarsFrame:
        return df.drop(list(columns))

    def rename(self, df: PolarsFrame, mapping: dict[str, str]) -> PolarsFrame:
        return df.rename(mapping)

    def with_column(self, df: PolarsFrame, name: str, expr: pl.Expr) -> PolarsFrame:
        return df.with_columns(expr.alias(name))

    def cast(self, df: PolarsFrame, name: str, dtype: Any) -> PolarsFrame:
        return df.with_columns(pl.col(name).cast(dtype))

    def filter(self, df: PolarsFrame, predicate: pl.Expr) -> PolarsFrame:
        return df.filter(predicate)

    def sort(self, df: PolarsFrame, columns: tuple[str, ...], *, descending: bool = False) -> PolarsFrame:
        if not columns:
            return df
        return df.sort(list(columns), descending=descending)

    def unique(
        self, df: PolarsFrame, subset: tuple[str, ...] | None, *, keep: str = "first"
    ) -> PolarsFrame:
        kwargs: dict[str, Any] = {"keep": keep}
        if subset is not None:
            kwargs["subset"] = list(subset)
        return df.unique(**kwargs)  # type: ignore[arg-type]

    def duplicated(
        self,
        df: PolarsFrame,
        subset: tuple[str, ...] | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> PolarsFrame:
        if keep is False:
            raise NotImplementedError("duplicated(..., keep=False) is not supported in this adapter yet")
        if keep not in {"first", "last"}:
            raise ValueError("keep must be 'first', 'last', or False")

        cols = list(subset) if subset is not None else None
        expr = pl.struct(cols) if cols is not None else pl.struct(pl.all())
        mask_expr = expr.is_duplicated()

        if isinstance(df, pl.LazyFrame):
            return df.select(mask_expr.alias(out_name))
        mask = df.select(mask_expr.alias(out_name))[out_name]
        return pl.DataFrame({out_name: mask})

    def compile_expr(self, expr: Any) -> pl.Expr:
        if not isinstance(expr, Expr):
            raise TypeError(f"Expected PlanFrame Expr, got {type(expr)!r}")
        return compile_expr(expr)

    def group_by_agg(
        self,
        df: PolarsFrame,
        *,
        keys: tuple[str, ...],
        named_aggs: dict[str, tuple[str, str]],
    ) -> PolarsFrame:
        if not keys:
            raise ValueError("keys must be non-empty")
        agg_exprs: list[pl.Expr] = []
        for out_name, (op, col) in named_aggs.items():
            e = pl.col(col)
            if op == "count":
                ex = e.count()
            elif op == "sum":
                ex = e.sum()
            elif op == "mean":
                ex = e.mean()
            elif op == "min":
                ex = e.min()
            elif op == "max":
                ex = e.max()
            elif op == "n_unique":
                ex = e.n_unique()
            else:
                raise ValueError(f"Unsupported agg op: {op!r}")
            agg_exprs.append(ex.alias(out_name))
        return df.group_by(list(keys)).agg(agg_exprs)

    def drop_nulls(self, df: PolarsFrame, subset: tuple[str, ...] | None) -> PolarsFrame:
        if subset is None:
            return df.drop_nulls()
        return df.drop_nulls(list(subset))

    def fill_null(self, df: PolarsFrame, value: Any, subset: tuple[str, ...] | None) -> PolarsFrame:
        if subset is None:
            return df.fill_null(value)
        exprs = [pl.col(c).fill_null(value) for c in subset]
        return df.with_columns(exprs)

    def melt(
        self,
        df: PolarsFrame,
        *,
        id_vars: tuple[str, ...],
        value_vars: tuple[str, ...],
        variable_name: str,
        value_name: str,
    ) -> PolarsFrame:
        # Prefer unpivot (polars deprecates melt on LazyFrame).
        return df.unpivot(
            index=list(id_vars),
            on=list(value_vars),
            variable_name=variable_name,
            value_name=value_name,
        )

    def join(
        self,
        left: PolarsFrame,
        right: PolarsFrame,
        *,
        on: tuple[str, ...],
        how: str = "inner",
        suffix: str = "_right",
    ) -> PolarsFrame:
        if not on:
            raise ValueError("on must be non-empty")
        return left.join(right, on=list(on), how=how, suffix=suffix)

    def slice(self, df: PolarsFrame, *, offset: int, length: int | None) -> PolarsFrame:
        return df.slice(offset, length)

    def head(self, df: PolarsFrame, n: int) -> PolarsFrame:
        return df.head(n)

    def tail(self, df: PolarsFrame, n: int) -> PolarsFrame:
        return df.tail(n)

    def concat_vertical(self, left: PolarsFrame, right: PolarsFrame) -> PolarsFrame:
        return pl.concat([left, right], how="vertical")

    def pivot(
        self,
        df: PolarsFrame,
        *,
        index: tuple[str, ...],
        on: str,
        values: str,
        agg: str = "first",
        on_columns: tuple[str, ...] | None = None,
        separator: str = "_",
    ) -> PolarsFrame:
        if isinstance(df, pl.LazyFrame) and on_columns is None:
            raise ValueError("Lazy pivot requires on_columns to be provided")
        return df.pivot(
            index=list(index),
            on=on,
            values=values,
            aggregate_function=agg,
            on_columns=list(on_columns) if on_columns is not None else None,
            separator=separator,
        )

    def collect(self, df: PolarsFrame) -> PolarsFrame:
        return df.collect() if isinstance(df, pl.LazyFrame) else df

