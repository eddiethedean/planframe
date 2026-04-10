from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from typing import Any, Literal, cast

import polars as pl

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
    IsIn,
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
    StrUpper,
    Sub,
    TrueDiv,
    Xor,
)

_dtype_hook: ContextVar[Callable[[str], object | None] | None] = ContextVar(
    "_dtype_hook", default=None
)


def compile_expr(
    expr: Expr[Any], *, dtype_for: Callable[[str], object | None] | None = None
) -> pl.Expr:
    tok = _dtype_hook.set(dtype_for)
    try:
        return _compile_expr(expr)
    finally:
        _dtype_hook.reset(tok)


def _compile_expr(expr: Expr[Any]) -> pl.Expr:
    if isinstance(expr, Alias):
        return _compile_expr(expr.expr)
    if isinstance(expr, Col):
        cb = _dtype_hook.get()
        if cb is not None:
            _ = cb(expr.name)
        return pl.col(expr.name)
    if isinstance(expr, Lit):
        return pl.lit(expr.value)
    if isinstance(expr, Add):
        return _compile_expr(expr.left) + _compile_expr(expr.right)
    if isinstance(expr, Sub):
        return _compile_expr(expr.left) - _compile_expr(expr.right)
    if isinstance(expr, Mul):
        return _compile_expr(expr.left) * _compile_expr(expr.right)
    if isinstance(expr, TrueDiv):
        return _compile_expr(expr.left) / _compile_expr(expr.right)
    if isinstance(expr, Eq):
        return _compile_expr(expr.left) == _compile_expr(expr.right)
    if isinstance(expr, Ne):
        return _compile_expr(expr.left) != _compile_expr(expr.right)
    if isinstance(expr, Lt):
        return _compile_expr(expr.left) < _compile_expr(expr.right)
    if isinstance(expr, Le):
        return _compile_expr(expr.left) <= _compile_expr(expr.right)
    if isinstance(expr, Gt):
        return _compile_expr(expr.left) > _compile_expr(expr.right)
    if isinstance(expr, Ge):
        return _compile_expr(expr.left) >= _compile_expr(expr.right)
    if isinstance(expr, IsNull):
        return _compile_expr(expr.value).is_null()
    if isinstance(expr, IsNotNull):
        return _compile_expr(expr.value).is_not_null()
    if isinstance(expr, IsIn):
        return _compile_expr(expr.value).is_in(list(expr.options))
    if isinstance(expr, And):
        return _compile_expr(expr.left) & _compile_expr(expr.right)
    if isinstance(expr, Or):
        return _compile_expr(expr.left) | _compile_expr(expr.right)
    if isinstance(expr, Not):
        return ~_compile_expr(expr.value)
    if isinstance(expr, Xor):
        return _compile_expr(expr.left) ^ _compile_expr(expr.right)
    if isinstance(expr, Abs):
        return _compile_expr(expr.value).abs()
    if isinstance(expr, Round):
        e = _compile_expr(expr.value)
        return e.round(expr.ndigits) if expr.ndigits is not None else e.round()
    if isinstance(expr, Floor):
        return _compile_expr(expr.value).floor()
    if isinstance(expr, Ceil):
        return _compile_expr(expr.value).ceil()
    if isinstance(expr, Coalesce):
        return pl.coalesce([_compile_expr(v) for v in expr.values])
    if isinstance(expr, IfElse):
        return (
            pl.when(_compile_expr(expr.cond))
            .then(_compile_expr(expr.then_value))
            .otherwise(_compile_expr(expr.else_value))
        )
    if isinstance(expr, Over):
        e = _compile_expr(expr.value)
        return e.over(
            partition_by=list(expr.partition_by),
            order_by=(list(expr.order_by) if expr.order_by is not None else None),
        )
    if isinstance(expr, Between):
        allowed_closed = {"left", "right", "both", "none"}
        if expr.closed not in allowed_closed:
            raise ValueError(f"Unsupported closed interval: {expr.closed!r}")
        closed_lit = cast(Literal["left", "right", "both", "none"], expr.closed)
        return _compile_expr(expr.value).is_between(
            _compile_expr(expr.low),
            _compile_expr(expr.high),
            closed=closed_lit,
        )
    if isinstance(expr, Clip):
        e = _compile_expr(expr.value)
        lower = _compile_expr(expr.lower) if expr.lower is not None else None
        upper = _compile_expr(expr.upper) if expr.upper is not None else None
        return e.clip(lower_bound=lower, upper_bound=upper)
    if isinstance(expr, Pow):
        return _compile_expr(expr.base) ** _compile_expr(expr.exponent)
    if isinstance(expr, Exp):
        return _compile_expr(expr.value).exp()
    if isinstance(expr, Log):
        return _compile_expr(expr.value).log()
    if isinstance(expr, StrContains):
        e = _compile_expr(expr.value).cast(pl.Utf8)
        return e.str.contains(expr.pattern, literal=expr.literal)
    if isinstance(expr, StrStartsWith):
        e = _compile_expr(expr.value).cast(pl.Utf8)
        return e.str.starts_with(expr.prefix)
    if isinstance(expr, StrEndsWith):
        e = _compile_expr(expr.value).cast(pl.Utf8)
        return e.str.ends_with(expr.suffix)
    if isinstance(expr, StrLower):
        e = _compile_expr(expr.value).cast(pl.Utf8)
        return e.str.to_lowercase()
    if isinstance(expr, StrUpper):
        e = _compile_expr(expr.value).cast(pl.Utf8)
        return e.str.to_uppercase()
    if isinstance(expr, StrLen):
        e = _compile_expr(expr.value).cast(pl.Utf8)
        return e.str.len_chars()
    if isinstance(expr, StrReplace):
        e = _compile_expr(expr.value).cast(pl.Utf8)
        return e.str.replace_all(expr.pattern, expr.replacement, literal=expr.literal)
    if isinstance(expr, StrStrip):
        e = _compile_expr(expr.value).cast(pl.Utf8)
        return e.str.strip_chars()
    if isinstance(expr, StrSplit):
        e = _compile_expr(expr.value).cast(pl.Utf8)
        return e.str.split(expr.by)
    if isinstance(expr, DtYear):
        return _compile_expr(expr.value).dt.year()
    if isinstance(expr, DtMonth):
        return _compile_expr(expr.value).dt.month()
    if isinstance(expr, DtDay):
        return _compile_expr(expr.value).dt.day()
    if isinstance(expr, Sqrt):
        return _compile_expr(expr.value).sqrt()
    if isinstance(expr, IsFinite):
        return _compile_expr(expr.value).is_finite()
    if isinstance(expr, AggExpr):
        inner = _compile_expr(expr.inner)
        if expr.op == "count":
            return inner.count()
        if expr.op == "sum":
            return inner.sum()
        if expr.op == "mean":
            return inner.mean()
        if expr.op == "min":
            return inner.min()
        if expr.op == "max":
            return inner.max()
        if expr.op == "n_unique":
            return inner.n_unique()
        raise PlanFrameExpressionError(f"Unsupported aggregation op: {expr.op!r}")

    raise PlanFrameExpressionError(f"Unsupported expr node: {type(expr)!r}")
