from __future__ import annotations

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


def compile_expr(expr: Expr[Any]) -> Any:
    if isinstance(expr, Alias):
        return compile_expr(expr.expr)
    if isinstance(expr, Col):
        return F.col(expr.name)
    if isinstance(expr, Lit):
        return F.lit(expr.value)
    if isinstance(expr, Add):
        return compile_expr(expr.left) + compile_expr(expr.right)
    if isinstance(expr, Sub):
        return compile_expr(expr.left) - compile_expr(expr.right)
    if isinstance(expr, Mul):
        return compile_expr(expr.left) * compile_expr(expr.right)
    if isinstance(expr, TrueDiv):
        return compile_expr(expr.left) / compile_expr(expr.right)
    if isinstance(expr, Eq):
        return compile_expr(expr.left) == compile_expr(expr.right)
    if isinstance(expr, Ne):
        return compile_expr(expr.left) != compile_expr(expr.right)
    if isinstance(expr, Lt):
        return compile_expr(expr.left) < compile_expr(expr.right)
    if isinstance(expr, Le):
        return compile_expr(expr.left) <= compile_expr(expr.right)
    if isinstance(expr, Gt):
        return compile_expr(expr.left) > compile_expr(expr.right)
    if isinstance(expr, Ge):
        return compile_expr(expr.left) >= compile_expr(expr.right)
    if isinstance(expr, IsNull):
        return compile_expr(expr.value).isNull()
    if isinstance(expr, IsNotNull):
        return compile_expr(expr.value).isNotNull()
    if isinstance(expr, IsIn):
        return compile_expr(expr.value).isin(list(expr.options))
    if isinstance(expr, And):
        return compile_expr(expr.left) & compile_expr(expr.right)
    if isinstance(expr, Or):
        return compile_expr(expr.left) | compile_expr(expr.right)
    if isinstance(expr, Not):
        return ~compile_expr(expr.value)
    if isinstance(expr, Xor):
        return compile_expr(expr.left) ^ compile_expr(expr.right)
    if isinstance(expr, Abs):
        return F.abs(compile_expr(expr.value))
    if isinstance(expr, Round):
        e = compile_expr(expr.value)
        return F.round(e, expr.ndigits) if expr.ndigits is not None else F.round(e)
    if isinstance(expr, Floor):
        return F.floor(compile_expr(expr.value))
    if isinstance(expr, Ceil):
        return F.ceil(compile_expr(expr.value))
    if isinstance(expr, Coalesce):
        return F.coalesce(*[compile_expr(v) for v in expr.values])
    if isinstance(expr, IfElse):
        return F.when(compile_expr(expr.cond), compile_expr(expr.then_value)).otherwise(
            compile_expr(expr.else_value)
        )
    if isinstance(expr, Over):
        # PlanFrame Over only carries partition/order column names, so we can map to Window.
        from sparkless.sql.window import Window

        w = Window.partitionBy(*expr.partition_by)
        if expr.order_by is not None:
            w = w.orderBy(*expr.order_by)
        return compile_expr(expr.value).over(w)
    if isinstance(expr, Between):
        return compile_expr(expr.value).between(compile_expr(expr.low), compile_expr(expr.high))
    if isinstance(expr, Clip):
        e = compile_expr(expr.value)
        if expr.lower is not None:
            e = F.greatest(e, compile_expr(expr.lower))
        if expr.upper is not None:
            e = F.least(e, compile_expr(expr.upper))
        return e
    if isinstance(expr, Pow):
        return F.pow(compile_expr(expr.base), compile_expr(expr.exponent))
    if isinstance(expr, Exp):
        return F.exp(compile_expr(expr.value))
    if isinstance(expr, Log):
        return F.log(compile_expr(expr.value))
    if isinstance(expr, StrContains):
        e = compile_expr(expr.value)
        if expr.literal:
            return e.contains(expr.pattern)
        return e.rlike(expr.pattern)
    if isinstance(expr, StrStartsWith):
        return compile_expr(expr.value).startswith(expr.prefix)
    if isinstance(expr, StrEndsWith):
        return compile_expr(expr.value).endswith(expr.suffix)
    if isinstance(expr, StrLower):
        return F.lower(compile_expr(expr.value))
    if isinstance(expr, StrUpper):
        return F.upper(compile_expr(expr.value))
    if isinstance(expr, StrLen):
        return F.length(compile_expr(expr.value))
    if isinstance(expr, StrReplace):
        return F.regexp_replace(compile_expr(expr.value), expr.pattern, expr.replacement)
    if isinstance(expr, StrStrip):
        return F.trim(compile_expr(expr.value))
    if isinstance(expr, StrSplit):
        return F.split(compile_expr(expr.value), expr.by)
    if isinstance(expr, DtYear):
        return F.year(compile_expr(expr.value))
    if isinstance(expr, DtMonth):
        return F.month(compile_expr(expr.value))
    if isinstance(expr, DtDay):
        return F.dayofmonth(compile_expr(expr.value))
    if isinstance(expr, Sqrt):
        return F.sqrt(compile_expr(expr.value))
    if isinstance(expr, IsFinite):
        # Spark doesn't have a direct isFinite; approximate via isnan/isnull checks.
        e = compile_expr(expr.value)
        return (~cast(Any, F.isnan(e))) & e.isNotNull()
    if isinstance(expr, AggExpr):
        inner = compile_expr(expr.inner)
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
