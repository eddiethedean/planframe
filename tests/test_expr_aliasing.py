from __future__ import annotations

import pytest

from planframe.expr import add, col, lit
from planframe.frame import Frame

from test_core_lazy_and_schema import SpyAdapter, UserDC


def test_expr_alias_requires_non_empty_name() -> None:
    with pytest.raises(ValueError, match="alias name must be non-empty"):
        _ = add(col("age"), lit(1)).alias("")


def test_with_columns_accepts_positional_aliased_exprs() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    out = pf.with_columns(add(col("age"), lit(1)).alias("age_plus_one"))
    assert out.schema().names() == ("id", "age", "age_plus_one")


def test_with_columns_rejects_positional_non_aliased_exprs() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    with pytest.raises(TypeError, match="positional arguments must be aliased expressions"):
        _ = pf.with_columns(add(col("age"), lit(1)))  # type: ignore[arg-type]


def test_select_accepts_col_expr_and_aliased_expr() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    out = pf.select(col("id"), add(col("age"), lit(1)).alias("age_plus_one"))
    assert out.schema().names() == ("id", "age_plus_one")

