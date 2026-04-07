from __future__ import annotations

from typing import Any

import polars as pl

from planframe.backend.errors import PlanFrameExpressionError
from planframe.expr.api import (
    Add,
    And,
    Abs,
    Ceil,
    Col,
    Coalesce,
    Eq,
    Expr,
    Floor,
    Ge,
    Gt,
    IfElse,
    IsIn,
    IsNotNull,
    IsNull,
    Le,
    Lit,
    Lt,
    Mul,
    Ne,
    Not,
    Or,
    Round,
    Sub,
    TrueDiv,
    Xor,
)


def compile_expr(expr: Expr[Any]) -> pl.Expr:
    if isinstance(expr, Col):
        return pl.col(expr.name)
    if isinstance(expr, Lit):
        return pl.lit(expr.value)
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
        return compile_expr(expr.value).is_null()
    if isinstance(expr, IsNotNull):
        return compile_expr(expr.value).is_not_null()
    if isinstance(expr, IsIn):
        return compile_expr(expr.value).is_in(list(expr.options))
    if isinstance(expr, And):
        return compile_expr(expr.left) & compile_expr(expr.right)
    if isinstance(expr, Or):
        return compile_expr(expr.left) | compile_expr(expr.right)
    if isinstance(expr, Not):
        return ~compile_expr(expr.value)
    if isinstance(expr, Xor):
        return compile_expr(expr.left) ^ compile_expr(expr.right)
    if isinstance(expr, Abs):
        return compile_expr(expr.value).abs()
    if isinstance(expr, Round):
        e = compile_expr(expr.value)
        return e.round(expr.ndigits) if expr.ndigits is not None else e.round()
    if isinstance(expr, Floor):
        return compile_expr(expr.value).floor()
    if isinstance(expr, Ceil):
        return compile_expr(expr.value).ceil()
    if isinstance(expr, Coalesce):
        return pl.coalesce([compile_expr(v) for v in expr.values])
    if isinstance(expr, IfElse):
        return pl.when(compile_expr(expr.cond)).then(compile_expr(expr.then_value)).otherwise(
            compile_expr(expr.else_value)
        )

    raise PlanFrameExpressionError(f"Unsupported expr node: {type(expr)!r}")

