from __future__ import annotations

from planframe.expr import Expr, col, lit


def f() -> None:
    c = col("age")
    _: Expr[bool] = c > 0
    _: Expr[bool] = c >= 0
    _: Expr[bool] = c < 0
    _: Expr[bool] = c <= 0

    _: Expr[bool] = c == 1
    _: Expr[bool] = c != 1.5
    _: Expr[bool] = c == "x"
    _: Expr[bool] = c == True
    _: Expr[bool] = c == None
    _: Expr[bool] = c == lit(1)
    _: Expr[bool] = c == col("other")

    p: Expr[bool] = c > 0
    _: Expr[bool] = p & (c < 99)
    _: Expr[bool] = p | (c < 1)
    _: Expr[bool] = ~p
    _: Expr[bool] = True & p
    _: Expr[bool] = False | p
