from __future__ import annotations

import pytest


def test_sparkless_basic_select_filter_withcolumn_to_dicts() -> None:
    pytest.importorskip("sparkless")
    pytest.importorskip("planframe_sparkless")

    from planframe.expr import add, col, lit
    from planframe_sparkless.frame import SparklessFrame

    class User(SparklessFrame):
        id: int
        x: int

    pf = User([{"id": 1, "x": 2}, {"id": 2, "x": 3}])

    out = (
        pf.select("id", "x")
        .withColumn("x2", add(col("x"), lit(1)))
        .where(pf["x"] > lit(2))
        .select("id", "x2")
        .orderBy("id")
    )

    rows = out.to_dicts()
    assert rows == [{"id": 2, "x2": 4}]
