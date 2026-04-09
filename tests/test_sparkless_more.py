from __future__ import annotations

import pytest


def _skip_if_missing() -> None:
    pytest.importorskip("sparkless")
    pytest.importorskip("planframe_sparkless")


def test_sparkless_join_inner_on_id() -> None:
    _skip_if_missing()

    from planframe_sparkless.frame import SparklessFrame

    class Left(SparklessFrame):
        id: int
        x: int

    class Right(SparklessFrame):
        id: int
        y: int

    left = Left([{"id": 1, "x": 10}, {"id": 2, "x": 20}])
    right = Right([{"id": 2, "y": 99}, {"id": 3, "y": 100}])

    out = left.join(right, on=("id",), how="inner").orderBy("id")
    assert out.to_dicts() == [{"id": 2, "x": 20, "y": 99}]


def test_sparkless_groupby_agg_count_sum() -> None:
    _skip_if_missing()

    from planframe.expr import agg_count, agg_sum, col
    from planframe_sparkless.frame import SparklessFrame

    class S(SparklessFrame):
        g: int
        x: int

    pf = S([{"g": 1, "x": 10}, {"g": 1, "x": 20}, {"g": 2, "x": 7}])
    out = pf.groupBy("g").agg(n=agg_count(col("x")), sx=agg_sum(col("x"))).sort("g")
    assert out.to_dicts() == [{"g": 1, "n": 2, "sx": 30}, {"g": 2, "n": 1, "sx": 7}]


def test_sparkless_drop_and_rename_strict() -> None:
    _skip_if_missing()

    from planframe.backend.errors import PlanFrameSchemaError
    from planframe_sparkless.frame import SparklessFrame

    class S(SparklessFrame):
        a: int
        b: int

    pf = S([{"a": 1, "b": 2}])

    out = pf.drop("b").rename(a="x")
    assert out.to_dicts() == [{"x": 1}]

    with pytest.raises(PlanFrameSchemaError):
        _ = pf.drop("missing", strict=True)

    # strict=False should ignore missing columns
    out2 = pf.drop("missing", strict=False)
    assert out2.to_dicts() == [{"a": 1, "b": 2}]


def test_sparkless_with_row_index_and_stream_dicts() -> None:
    _skip_if_missing()

    from planframe_sparkless.frame import SparklessFrame

    class S(SparklessFrame):
        id: int

    pf = S([{"id": 1}, {"id": 2}, {"id": 3}])
    out = pf.with_row_index(name="row_nr", offset=10).orderBy("id")

    rows = list(out.stream_dicts())
    assert [r["row_nr"] for r in rows] == [10, 11, 12]
