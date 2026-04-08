from __future__ import annotations

import pytest
from test_core_lazy_and_schema import SpyAdapter, UserDC

from planframe.backend.errors import PlanFrameSchemaError
from planframe.expr import agg_sum, col
from planframe.frame import Frame


def test_groupby_agg_requires_at_least_one_named_agg() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)
    gb = pf.group_by("id")
    with pytest.raises(PlanFrameSchemaError, match="agg requires at least one"):
        gb.agg()


def test_groupby_agg_expr_missing_columns_errors() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    # Expression references unknown column `nope`.
    gb = pf.group_by("id")
    with pytest.raises(PlanFrameSchemaError, match="references unknown columns"):
        gb.agg(x=agg_sum(col("nope")))
