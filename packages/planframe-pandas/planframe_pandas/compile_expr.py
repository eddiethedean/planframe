from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeAlias, cast

import pandas as pd

from planframe.backend.errors import PlanFrameExpressionError
from planframe.expr.api import (
    Add,
    AggExpr,
    And,
    Coalesce,
    Col,
    Eq,
    Expr,
    Ge,
    Gt,
    IfElse,
    IsNull,
    Le,
    Lit,
    Lt,
    Mul,
    Ne,
    Not,
    Or,
    Pow,
    Sub,
    TrueDiv,
)

PandasExpr: TypeAlias = Callable[[pd.DataFrame], pd.Series | object]


@dataclass(frozen=True, slots=True)
class AggExprSpec:
    """A compiled aggregation expression for pandas backends."""

    op: str
    inner: PandasExpr


def compile_expr(expr: Expr[Any]) -> PandasExpr | AggExprSpec:
    """Compile PlanFrame expression IR into a callable pandas expression."""
    if isinstance(expr, Col):
        name = expr.name
        return lambda df: df[name]
    if isinstance(expr, Lit):
        value = expr.value
        return lambda _df: value

    # Aggregations are represented as a wrapper; group_by_agg handles execution.
    if isinstance(expr, AggExpr):
        op = expr.op
        inner = compile_expr(expr.inner)
        if isinstance(inner, AggExprSpec):
            raise PlanFrameExpressionError("Nested AggExpr is not supported")
        return AggExprSpec(op=op, inner=cast(PandasExpr, inner))

    if isinstance(expr, (Add, Sub, Mul, TrueDiv, Eq, Ne, Lt, Le, Gt, Ge, And, Or)):
        left = compile_expr(expr.left)
        right = compile_expr(expr.right)
        if isinstance(left, AggExprSpec) or isinstance(right, AggExprSpec):
            raise PlanFrameExpressionError(
                "AggExpr is only supported inside group_by(...).agg(...)"
            )

        left_fn = cast(PandasExpr, left)
        right_fn = cast(PandasExpr, right)

        if isinstance(expr, Add):
            return lambda df: cast(Any, left_fn(df)) + cast(Any, right_fn(df))
        if isinstance(expr, Sub):
            return lambda df: cast(Any, left_fn(df)) - cast(Any, right_fn(df))
        if isinstance(expr, Mul):
            return lambda df: cast(Any, left_fn(df)) * cast(Any, right_fn(df))
        if isinstance(expr, TrueDiv):
            return lambda df: cast(Any, left_fn(df)) / cast(Any, right_fn(df))
        if isinstance(expr, Eq):
            return lambda df: left_fn(df) == right_fn(df)
        if isinstance(expr, Ne):
            return lambda df: left_fn(df) != right_fn(df)
        if isinstance(expr, Lt):
            return lambda df: cast(Any, left_fn(df)) < cast(Any, right_fn(df))
        if isinstance(expr, Le):
            return lambda df: cast(Any, left_fn(df)) <= cast(Any, right_fn(df))
        if isinstance(expr, Gt):
            return lambda df: cast(Any, left_fn(df)) > cast(Any, right_fn(df))
        if isinstance(expr, Ge):
            return lambda df: cast(Any, left_fn(df)) >= cast(Any, right_fn(df))

        if isinstance(expr, And):
            return lambda df: cast(pd.Series, left_fn(df)) & cast(pd.Series, right_fn(df))
        if isinstance(expr, Or):
            return lambda df: cast(pd.Series, left_fn(df)) | cast(pd.Series, right_fn(df))

    if isinstance(expr, Pow):
        base = compile_expr(expr.base)
        exponent = compile_expr(expr.exponent)
        if isinstance(base, AggExprSpec) or isinstance(exponent, AggExprSpec):
            raise PlanFrameExpressionError(
                "AggExpr is only supported inside group_by(...).agg(...)"
            )
        base_fn = cast(PandasExpr, base)
        exp_fn = cast(PandasExpr, exponent)
        return lambda df: cast(Any, base_fn(df)) ** cast(Any, exp_fn(df))

    if isinstance(expr, Not):
        inner = compile_expr(expr.value)
        if isinstance(inner, AggExprSpec):
            raise PlanFrameExpressionError(
                "AggExpr is only supported inside group_by(...).agg(...)"
            )
        inner_fn = cast(PandasExpr, inner)
        return lambda df: ~cast(pd.Series, inner_fn(df))

    if isinstance(expr, Coalesce):
        values = [compile_expr(v) for v in expr.values]
        if any(isinstance(v, AggExprSpec) for v in values):
            raise PlanFrameExpressionError(
                "AggExpr is only supported inside group_by(...).agg(...)"
            )
        fns = [cast(PandasExpr, v) for v in values]

        def _coalesce(df: pd.DataFrame) -> pd.Series | object:
            out: Any = fns[0](df)
            for fn in fns[1:]:
                v = fn(df)
                if isinstance(out, pd.Series) and not isinstance(v, pd.Series):
                    v = pd.Series([v] * len(df), index=df.index)
                if isinstance(v, pd.Series) and not isinstance(out, pd.Series):
                    out = pd.Series([out] * len(df), index=df.index)
                out = cast(pd.Series, out).combine_first(cast(pd.Series, v))
            return out

        return _coalesce

    if isinstance(expr, IsNull):
        inner = compile_expr(expr.value)
        if isinstance(inner, AggExprSpec):
            raise PlanFrameExpressionError(
                "AggExpr is only supported inside group_by(...).agg(...)"
            )
        inner_fn = cast(PandasExpr, inner)

        def _is_null(df: pd.DataFrame) -> pd.Series | bool:
            v = inner_fn(df)
            if isinstance(v, pd.Series):
                return v.isna()
            return bool(pd.isna(v))

        return _is_null

    if isinstance(expr, IfElse):
        cond = compile_expr(expr.cond)
        if_true = compile_expr(expr.then_value)
        if_false = compile_expr(expr.else_value)
        if (
            isinstance(cond, AggExprSpec)
            or isinstance(if_true, AggExprSpec)
            or isinstance(if_false, AggExprSpec)
        ):
            raise PlanFrameExpressionError(
                "AggExpr is only supported inside group_by(...).agg(...)"
            )
        cond_fn = cast(PandasExpr, cond)
        t_fn = cast(PandasExpr, if_true)
        f_fn = cast(PandasExpr, if_false)

        def _if_else(df: pd.DataFrame) -> pd.Series:
            c = cond_fn(df)
            if not isinstance(c, pd.Series):
                raise PlanFrameExpressionError("if_else predicate must be a Series")
            t = t_fn(df)
            f = f_fn(df)
            t_ser = t if isinstance(t, pd.Series) else pd.Series([t] * len(df), index=df.index)
            f_ser = f if isinstance(f, pd.Series) else pd.Series([f] * len(df), index=df.index)
            return t_ser.where(c, other=f_ser)

        return _if_else

    raise PlanFrameExpressionError(
        f"Unsupported expression node type={type(expr).__name__!r} for pandas backend"
    )
