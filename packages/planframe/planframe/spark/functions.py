"""PySpark `functions` subset backed by PlanFrame expression IR."""

from __future__ import annotations

from typing import cast

from planframe.expr.api import (
    Expr,
    agg_count,
    agg_max,
    agg_mean,
    agg_min,
    agg_n_unique,
    agg_sum,
)
from planframe.expr.api import (
    coalesce as pf_coalesce,
)
from planframe.expr.api import (
    col as pf_col,
)
from planframe.expr.api import (
    if_else as pf_if_else,
)
from planframe.expr.api import (
    lit as pf_lit,
)

from .column import Column, lit_value, unwrap_expr

__all__ = [
    "coalesce",
    "col",
    "count",
    "if_else",
    "lit",
    "max",
    "mean",
    "min",
    "n_unique",
    "sum",
]


def col(name: str) -> Column:
    return Column(pf_col(name))


def lit(value: object) -> Column:
    return Column(pf_lit(value))


def sum(name_or_col: str | Column) -> Column:  # noqa: A001
    inner = pf_col(name_or_col) if isinstance(name_or_col, str) else unwrap_expr(name_or_col)
    return Column(agg_sum(cast(Expr[object], inner)))


def mean(name_or_col: str | Column) -> Column:
    inner = pf_col(name_or_col) if isinstance(name_or_col, str) else unwrap_expr(name_or_col)
    return Column(agg_mean(cast(Expr[object], inner)))


def max(name_or_col: str | Column) -> Column:  # noqa: A001
    inner = pf_col(name_or_col) if isinstance(name_or_col, str) else unwrap_expr(name_or_col)
    return Column(agg_max(cast(Expr[object], inner)))


def min(name_or_col: str | Column) -> Column:  # noqa: A001
    inner = pf_col(name_or_col) if isinstance(name_or_col, str) else unwrap_expr(name_or_col)
    return Column(agg_min(cast(Expr[object], inner)))


def n_unique(name_or_col: str | Column) -> Column:
    inner = pf_col(name_or_col) if isinstance(name_or_col, str) else unwrap_expr(name_or_col)
    return Column(agg_n_unique(cast(Expr[object], inner)))


def count(name_or_col: str | Column | None = None) -> Column:
    if name_or_col is None:
        return Column(agg_count(pf_lit(1)))
    inner = pf_col(name_or_col) if isinstance(name_or_col, str) else unwrap_expr(name_or_col)
    return Column(agg_count(cast(Expr[object], inner)))


def coalesce(*cols: Column | Expr[object]) -> Column:
    parts = [unwrap_expr(c) for c in cols]
    return Column(pf_coalesce(*parts))


def if_else(condition: Column[bool], then_val: object, else_val: object) -> Column:
    return Column(
        pf_if_else(
            unwrap_expr(condition),
            lit_value(then_val),
            lit_value(else_val),
        )
    )
