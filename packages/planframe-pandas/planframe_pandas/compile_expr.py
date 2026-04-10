from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, TypeAlias, cast

import numpy as np
import pandas as pd

from planframe.backend.errors import PlanFrameExpressionError
from planframe.expr.api import (
    Abs,
    Add,
    AggExpr,
    Alias,
    And,
    Between,
    Ceil,
    Clip,
    Coalesce,
    Col,
    DtDay,
    DtMonth,
    DtYear,
    Eq,
    Exp,
    Expr,
    Floor,
    Ge,
    Gt,
    IfElse,
    IsFinite,
    IsNotNull,
    IsNull,
    Le,
    Lit,
    Log,
    Lt,
    Mul,
    Ne,
    Not,
    Or,
    Over,
    Pow,
    Round,
    Sqrt,
    StrContains,
    StrEndsWith,
    StrLen,
    StrLower,
    StrReplace,
    StrSplit,
    StrStartsWith,
    StrStrip,
    Sub,
    TrueDiv,
    Xor,
)

PandasExpr: TypeAlias = Callable[[pd.DataFrame], pd.Series | object]


@dataclass(frozen=True, slots=True)
class AggExprSpec:
    """A compiled aggregation expression for pandas backends."""

    op: str
    inner: PandasExpr


_dtype_hook: ContextVar[Callable[[str], object | None] | None] = ContextVar(
    "_dtype_hook", default=None
)


def compile_expr(
    expr: Expr[Any], *, dtype_for: Callable[[str], object | None] | None = None
) -> PandasExpr | AggExprSpec:
    tok = _dtype_hook.set(dtype_for)
    try:
        return _pe_impl(expr)
    finally:
        _dtype_hook.reset(tok)


def _pe_impl(expr: Expr[Any]) -> PandasExpr | AggExprSpec:
    """Compile PlanFrame expression IR into a callable pandas expression."""

    def _as_expr(e: Expr[Any]) -> PandasExpr:
        out = _pe_impl(e)
        if isinstance(out, AggExprSpec):
            raise PlanFrameExpressionError(
                "AggExpr is only supported inside group_by(...).agg(...)"
            )
        return cast(PandasExpr, out)

    if isinstance(expr, Alias):
        return _pe_impl(expr.expr)
    if isinstance(expr, Col):
        cb = _dtype_hook.get()
        if cb is not None:
            _ = cb(expr.name)
        name = expr.name
        return lambda df: df[name]
    if isinstance(expr, Lit):
        value = expr.value
        return lambda _df: value

    # Aggregations are represented as a wrapper; group_by_agg handles execution.
    if isinstance(expr, AggExpr):
        op = expr.op
        inner = _pe_impl(expr.inner)
        if isinstance(inner, AggExprSpec):
            raise PlanFrameExpressionError("Nested AggExpr is not supported")
        return AggExprSpec(op=op, inner=cast(PandasExpr, inner))

    if isinstance(expr, (Add, Sub, Mul, TrueDiv, Eq, Ne, Lt, Le, Gt, Ge, And, Or)):
        left = _pe_impl(expr.left)
        right = _pe_impl(expr.right)
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
        base = _pe_impl(expr.base)
        exponent = _pe_impl(expr.exponent)
        if isinstance(base, AggExprSpec) or isinstance(exponent, AggExprSpec):
            raise PlanFrameExpressionError(
                "AggExpr is only supported inside group_by(...).agg(...)"
            )
        base_fn = cast(PandasExpr, base)
        exp_fn = cast(PandasExpr, exponent)
        return lambda df: cast(Any, base_fn(df)) ** cast(Any, exp_fn(df))

    if isinstance(expr, Not):
        inner = _pe_impl(expr.value)
        if isinstance(inner, AggExprSpec):
            raise PlanFrameExpressionError(
                "AggExpr is only supported inside group_by(...).agg(...)"
            )
        inner_fn = cast(PandasExpr, inner)
        return lambda df: ~cast(pd.Series, inner_fn(df))

    if isinstance(expr, Coalesce):
        values = [_pe_impl(v) for v in expr.values]
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
        inner = _pe_impl(expr.value)
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
        cond = _pe_impl(expr.cond)
        if_true = _pe_impl(expr.then_value)
        if_false = _pe_impl(expr.else_value)
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

    if isinstance(expr, IsNotNull):
        inner_fn = _as_expr(expr.value)
        return lambda df: cast(pd.Series, inner_fn(df)).notna()

    if isinstance(expr, Xor):
        left_fn = _as_expr(expr.left)
        right_fn = _as_expr(expr.right)
        return lambda df: cast(pd.Series, left_fn(df)) ^ cast(pd.Series, right_fn(df))

    if isinstance(expr, Abs):
        inner = _as_expr(expr.value)
        return lambda df: cast(pd.Series, inner(df)).abs()

    if isinstance(expr, Round):
        inner = _as_expr(expr.value)
        ndigits = 0 if expr.ndigits is None else int(expr.ndigits)
        return lambda df: cast(pd.Series, inner(df)).round(ndigits)

    if isinstance(expr, (Ceil,)):
        inner = _as_expr(expr.value)
        return lambda df: np.ceil(cast(pd.Series, inner(df)))

    if isinstance(expr, Floor):
        inner = _as_expr(expr.value)
        return lambda df: np.floor(cast(pd.Series, inner(df)))

    if isinstance(expr, Between):
        v = _as_expr(expr.value)
        lo = _as_expr(expr.low)
        hi = _as_expr(expr.high)

        def _between(df: pd.DataFrame) -> pd.Series:
            vv = cast(pd.Series, v(df))
            lowv = cast(Any, lo(df))
            hiv = cast(Any, hi(df))
            low_ser = (
                lowv if isinstance(lowv, pd.Series) else pd.Series([lowv] * len(df), index=df.index)
            )
            hi_ser = (
                hiv if isinstance(hiv, pd.Series) else pd.Series([hiv] * len(df), index=df.index)
            )
            return (vv >= low_ser) & (vv <= hi_ser)

        return _between

    if isinstance(expr, Clip):
        v = _as_expr(expr.value)
        lower = _as_expr(expr.lower) if expr.lower is not None else None
        upper = _as_expr(expr.upper) if expr.upper is not None else None

        def _clip(df: pd.DataFrame) -> pd.Series:
            vv = cast(pd.Series, v(df))
            lo = lower(df) if lower is not None else None
            up = upper(df) if upper is not None else None
            return vv.clip(lower=lo, upper=up)

        return _clip

    if isinstance(expr, Exp):
        inner = _as_expr(expr.value)
        return lambda df: np.exp(cast(Any, inner(df)))

    if isinstance(expr, Log):
        inner = _as_expr(expr.value)
        return lambda df: np.log(cast(Any, inner(df)))

    if isinstance(expr, Sqrt):
        inner = _as_expr(expr.value)
        return lambda df: np.sqrt(cast(Any, inner(df)))

    if isinstance(expr, IsFinite):
        inner = _as_expr(expr.value)
        return lambda df: np.isfinite(cast(Any, inner(df)))

    if isinstance(expr, StrLower):
        inner = _as_expr(expr.value)
        return lambda df: cast(pd.Series, inner(df)).astype("string").str.lower()

    if isinstance(expr, StrStartsWith):
        inner = _as_expr(expr.value)
        prefix = expr.prefix
        return lambda df: (
            cast(pd.Series, inner(df)).astype("string").str.startswith(prefix, na=False)
        )

    if isinstance(expr, StrEndsWith):
        inner = _as_expr(expr.value)
        suffix = expr.suffix
        return lambda df: cast(pd.Series, inner(df)).astype("string").str.endswith(suffix, na=False)

    if isinstance(expr, StrContains):
        inner = _as_expr(expr.value)
        pattern = expr.pattern
        literal = bool(expr.literal)
        return lambda df: (
            cast(pd.Series, inner(df))
            .astype("string")
            .str.contains(pattern, regex=not literal, na=False)
        )

    if isinstance(expr, StrLen):
        inner = _as_expr(expr.value)
        return lambda df: cast(pd.Series, inner(df)).astype("string").str.len()

    if isinstance(expr, StrReplace):
        inner = _as_expr(expr.value)
        pattern = expr.pattern
        repl = expr.replacement
        literal = bool(expr.literal)
        return lambda df: (
            cast(pd.Series, inner(df))
            .astype("string")
            .str.replace(pattern, repl, regex=not literal)
        )

    if isinstance(expr, StrStrip):
        inner = _as_expr(expr.value)
        return lambda df: cast(pd.Series, inner(df)).astype("string").str.strip()

    if isinstance(expr, StrSplit):
        inner = _as_expr(expr.value)
        by = expr.by
        return lambda df: cast(pd.Series, inner(df)).astype("string").str.split(by)

    if isinstance(expr, DtYear):
        inner = _as_expr(expr.value)
        return lambda df: pd.to_datetime(cast(pd.Series, inner(df))).dt.year

    if isinstance(expr, DtMonth):
        inner = _as_expr(expr.value)
        return lambda df: pd.to_datetime(cast(pd.Series, inner(df))).dt.month

    if isinstance(expr, DtDay):
        inner = _as_expr(expr.value)
        return lambda df: pd.to_datetime(cast(pd.Series, inner(df))).dt.day

    if isinstance(expr, Over):
        # Pandas backend does not currently model window contexts; treat as passthrough.
        inner_fn = _as_expr(expr.value)
        return lambda df: inner_fn(df)

    raise PlanFrameExpressionError(
        f"Unsupported expression node type={type(expr).__name__!r} for pandas backend"
    )
