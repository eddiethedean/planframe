from __future__ import annotations

import pytest

from planframe.backend.errors import PlanFrameExpressionError
from planframe.expr import (
    abs_,
    add,
    agg_count,
    agg_max,
    agg_mean,
    agg_min,
    agg_n_unique,
    agg_sum,
    and_,
    between,
    ceil,
    clip,
    coalesce,
    col,
    contains,
    day,
    ends_with,
    eq,
    exp,
    floor,
    ge,
    gt,
    if_else,
    is_finite,
    is_not_null,
    is_null,
    isin,
    le,
    length,
    lit,
    log,
    lower,
    lt,
    month,
    mul,
    ne,
    not_,
    or_,
    over,
    pow_,
    replace,
    round_,
    split,
    sqrt,
    starts_with,
    strip,
    sub,
    truediv,
    upper,
    xor,
    year,
)
from planframe.expr.api import _assert_bool, infer_dtype, is_bool_expr


def test_expr_api_wrappers_construct_nodes() -> None:
    a = col("a")
    b = col("b")

    _ = add(a, b)
    _ = sub(a, b)
    _ = mul(a, b)
    _ = truediv(a, b)

    _ = eq(a, b)
    _ = ne(a, b)
    _ = lt(a, b)
    _ = le(a, b)
    _ = gt(a, b)
    _ = ge(a, b)

    _ = is_null(a)
    _ = is_not_null(a)
    _ = isin(a, 1, 2, 3)

    _ = and_(eq(a, b), ne(a, b))
    _ = or_(eq(a, b), ne(a, b))
    _ = not_(eq(a, b))
    _ = xor(eq(a, b), ne(a, b))

    _ = abs_(a)
    _ = round_(a, 2)
    _ = floor(a)
    _ = ceil(a)
    _ = pow_(a, lit(2))
    _ = exp(a)
    _ = log(a)
    _ = sqrt(a)

    _ = coalesce(a, b, lit(1))
    _ = if_else(eq(a, b), a, b)
    _ = over(a, partition_by=("a",))
    _ = between(a, lit(0), lit(1), closed="both")
    _ = clip(a, lower=lit(0))

    _ = contains(a, "x", literal=True)
    _ = starts_with(a, "x")
    _ = ends_with(a, "y")
    _ = lower(a)
    _ = upper(a)
    _ = length(a)
    _ = replace(a, "x", "y", literal=True)
    _ = strip(a)
    _ = split(a, ",")

    _ = year(a)
    _ = month(a)
    _ = day(a)
    _ = is_finite(a)

    _ = agg_count(a)
    _ = agg_sum(a)
    _ = agg_mean(a)
    _ = agg_min(a)
    _ = agg_max(a)
    _ = agg_n_unique(a)


def test_expr_api_validation_branches_raise() -> None:
    a = col("a")

    with pytest.raises(PlanFrameExpressionError, match="coalesce requires at least one"):
        coalesce()

    with pytest.raises(PlanFrameExpressionError, match="over requires non-empty partition_by"):
        over(a, partition_by=())

    with pytest.raises(PlanFrameExpressionError, match="order_by must be non-empty"):
        over(a, partition_by=("a",), order_by=())

    with pytest.raises(PlanFrameExpressionError, match="between closed must be one of"):
        between(a, lit(0), lit(1), closed="nope")

    with pytest.raises(PlanFrameExpressionError, match="clip requires at least one"):
        clip(a)


def test_expr_bool_narrowing_helpers() -> None:
    e = eq(col("a"), lit(1))
    assert is_bool_expr(e)
    assert _assert_bool(e) is e

    with pytest.raises(PlanFrameExpressionError, match="Expected boolean Expr"):
        _assert_bool(add(col("a"), lit(1)))


def test_infer_dtype_covers_common_cases() -> None:
    assert infer_dtype(lit(1)) is int
    assert infer_dtype(eq(col("a"), lit(1))) is bool
    assert infer_dtype(add(col("a"), lit(1))) is object
    assert infer_dtype(coalesce(col("a"), lit(1))) is object
    assert infer_dtype(agg_sum(col("a"))) is object
    assert infer_dtype(agg_count(col("a"))) is int
    assert infer_dtype(agg_n_unique(col("a"))) is int
    assert infer_dtype(agg_mean(col("a"))) is float
    assert infer_dtype(length(col("a"))) is object
    assert infer_dtype(year(col("a"))) is object
