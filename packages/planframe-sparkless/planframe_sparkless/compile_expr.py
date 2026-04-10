from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from typing import Any, cast

from sparkless.sql import functions as F

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
) -> Any:
    tok = _dtype_hook.set(dtype_for)
    try:
        return _sl_compile(expr)
    finally:
        _dtype_hook.reset(tok)


def _sl_compile(expr: Expr[Any]) -> Any:
    if isinstance(expr, Alias):
        return _sl_compile(expr.expr)
    if isinstance(expr, Col):
        cb = _dtype_hook.get()
        if cb is not None:
            _ = cb(expr.name)
        return F.col(expr.name)
    if isinstance(expr, Lit):
        return F.lit(expr.value)
    if isinstance(expr, Add):
        return _sl_compile(expr.left) + _sl_compile(expr.right)
    if isinstance(expr, Sub):
        return _sl_compile(expr.left) - _sl_compile(expr.right)
    if isinstance(expr, Mul):
        return _sl_compile(expr.left) * _sl_compile(expr.right)
    if isinstance(expr, TrueDiv):
        return _sl_compile(expr.left) / _sl_compile(expr.right)
    if isinstance(expr, Eq):
        return _sl_compile(expr.left) == _sl_compile(expr.right)
    if isinstance(expr, Ne):
        return _sl_compile(expr.left) != _sl_compile(expr.right)
    if isinstance(expr, Lt):
        return _sl_compile(expr.left) < _sl_compile(expr.right)
    if isinstance(expr, Le):
        return _sl_compile(expr.left) <= _sl_compile(expr.right)
    if isinstance(expr, Gt):
        return _sl_compile(expr.left) > _sl_compile(expr.right)
    if isinstance(expr, Ge):
        return _sl_compile(expr.left) >= _sl_compile(expr.right)
    if isinstance(expr, IsNull):
        return _sl_compile(expr.value).isNull()
    if isinstance(expr, IsNotNull):
        return _sl_compile(expr.value).isNotNull()
    if isinstance(expr, IsIn):
        return _sl_compile(expr.value).isin(list(expr.options))
    if isinstance(expr, And):
        return _sl_compile(expr.left) & _sl_compile(expr.right)
    if isinstance(expr, Or):
        return _sl_compile(expr.left) | _sl_compile(expr.right)
    if isinstance(expr, Not):
        return ~_sl_compile(expr.value)
    if isinstance(expr, Xor):
        return _sl_compile(expr.left) ^ _sl_compile(expr.right)
    if isinstance(expr, Abs):
        return F.abs(_sl_compile(expr.value))
    if isinstance(expr, Round):
        e = _sl_compile(expr.value)
        return F.round(e, expr.ndigits) if expr.ndigits is not None else F.round(e)
    if isinstance(expr, Floor):
        return F.floor(_sl_compile(expr.value))
    if isinstance(expr, Ceil):
        return F.ceil(_sl_compile(expr.value))
    if isinstance(expr, Coalesce):
        return F.coalesce(*[_sl_compile(v) for v in expr.values])
    if isinstance(expr, IfElse):
        return F.when(_sl_compile(expr.cond), _sl_compile(expr.then_value)).otherwise(
            _sl_compile(expr.else_value)
        )
    if isinstance(expr, Over):
        # PlanFrame Over only carries partition/order column names, so we can map to Window.
        from sparkless.sql.window import Window

        w = Window.partitionBy(*expr.partition_by)
        if expr.order_by is not None:
            w = w.orderBy(*expr.order_by)
        return _sl_compile(expr.value).over(w)
    if isinstance(expr, Between):
        return _sl_compile(expr.value).between(_sl_compile(expr.low), _sl_compile(expr.high))
    if isinstance(expr, Clip):
        e = _sl_compile(expr.value)
        if expr.lower is not None:
            e = F.greatest(e, _sl_compile(expr.lower))
        if expr.upper is not None:
            e = F.least(e, _sl_compile(expr.upper))
        return e
    if isinstance(expr, Pow):
        return F.pow(_sl_compile(expr.base), _sl_compile(expr.exponent))
    if isinstance(expr, Exp):
        return F.exp(_sl_compile(expr.value))
    if isinstance(expr, Log):
        return F.log(_sl_compile(expr.value))
    if isinstance(expr, StrContains):
        e = _sl_compile(expr.value)
        if expr.literal:
            return e.contains(expr.pattern)
        return e.rlike(expr.pattern)
    if isinstance(expr, StrStartsWith):
        return _sl_compile(expr.value).startswith(expr.prefix)
    if isinstance(expr, StrEndsWith):
        return _sl_compile(expr.value).endswith(expr.suffix)
    if isinstance(expr, StrLower):
        return F.lower(_sl_compile(expr.value))
    if isinstance(expr, StrUpper):
        return F.upper(_sl_compile(expr.value))
    if isinstance(expr, StrLen):
        return F.length(_sl_compile(expr.value))
    if isinstance(expr, StrReplace):
        return F.regexp_replace(_sl_compile(expr.value), expr.pattern, expr.replacement)
    if isinstance(expr, StrStrip):
        return F.trim(_sl_compile(expr.value))
    if isinstance(expr, StrSplit):
        return F.split(_sl_compile(expr.value), expr.by)
    if isinstance(expr, DtYear):
        return F.year(_sl_compile(expr.value))
    if isinstance(expr, DtMonth):
        return F.month(_sl_compile(expr.value))
    if isinstance(expr, DtDay):
        return F.dayofmonth(_sl_compile(expr.value))
    if isinstance(expr, Sqrt):
        return F.sqrt(_sl_compile(expr.value))
    if isinstance(expr, IsFinite):
        # Spark doesn't have a direct isFinite; approximate via isnan/isnull checks.
        e = _sl_compile(expr.value)
        return (~cast(Any, F.isnan(e))) & e.isNotNull()
    if isinstance(expr, AggExpr):
        inner = _sl_compile(expr.inner)
        if expr.op == "count":
            return F.count(inner)
        if expr.op == "sum":
            return F.sum(inner)
        if expr.op == "mean":
            return F.avg(inner)
        if expr.op == "min":
            return F.min(inner)
        if expr.op == "max":
            return F.max(inner)
        if expr.op == "n_unique":
            return F.countDistinct(inner)
        raise PlanFrameExpressionError(f"Unsupported aggregation op: {expr.op!r}")

    raise PlanFrameExpressionError(f"Unsupported expr node for sparkless: {type(expr)!r}")
