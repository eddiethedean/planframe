"""Contract: core Expr IR + helpers type-check without Polars (stub/runtime alignment).

Downstream adapters and `.pyi` surfaces rely on these patterns staying valid under Pyright
strict mode. Extend when public `planframe.expr` API gains user-facing constructors.

Use distinct locals (not repeated ``_``) so Pyright does not widen ``Expr[object]`` across lines.
"""

from __future__ import annotations

from planframe.expr import (
    Expr,
    add,
    agg_count,
    agg_sum,
    between,
    coalesce,
    col,
    eq,
    ge,
    gt,
    if_else,
    is_not_null,
    is_null,
    isin,
    lit,
    mul,
    not_,
    or_,
    sub,
)


def f() -> None:
    """Patterns mirroring docs/quickstarts — only `planframe.expr` (no Frame)."""

    a = col("a")
    b = col("b")

    arith0: Expr[object] = add(a, lit(1))
    arith1: Expr[object] = sub(a, lit(1))
    arith2: Expr[object] = mul(a, lit(2))
    p0: Expr[bool] = gt(a, lit(0))
    p1: Expr[bool] = ge(a, lit(0))
    p2: Expr[bool] = eq(a, lit(0))
    p3: Expr[bool] = eq(a, b)
    p4: Expr[bool] = is_null(a)
    p5: Expr[bool] = is_not_null(a)
    p6: Expr[bool] = between(a, lit(0), lit(10))
    p7: Expr[bool] = isin(a, (lit(1), lit(2)))
    co0: Expr[object] = coalesce(a, b, lit(0))
    if0: Expr[object] = if_else(eq(a, lit(1)), lit("one"), lit("other"))
    p8: Expr[bool] = or_(gt(a, lit(0)), gt(b, lit(0)))
    p9: Expr[bool] = not_(eq(a, lit(0)))

    g0: Expr[object] = agg_sum(a)
    g1: Expr[object] = agg_count(a)

    _ = (
        arith0,
        arith1,
        arith2,
        p0,
        p1,
        p2,
        p3,
        p4,
        p5,
        p6,
        p7,
        co0,
        if0,
        p8,
        p9,
        g0,
        g1,
    )
