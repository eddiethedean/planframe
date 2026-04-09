from __future__ import annotations

import pytest

pytest.importorskip("polars")

from planframe.expr.api import lit
from planframe.plan.nodes import Filter, Hint, WithColumn
from planframe.spark import SparkFrame
from planframe.spark import functions as F
from planframe_polars import PolarsFrame


class _UsersXY(PolarsFrame, SparkFrame):
    x: int
    y: int


class _UsersYX(PolarsFrame, SparkFrame):
    y: int
    x: int


def test_sparkframe_with_column_matches_core_plan() -> None:
    s = _UsersXY({"x": [1], "y": [2]})
    sparked = s.withColumn("z", F.lit(3))
    core = s.with_column("z", lit(3))
    assert isinstance(sparked.plan(), WithColumn)
    assert isinstance(core.plan(), WithColumn)

    assert sparked.to_dicts() == core.to_dicts()
    assert sparked.to_dicts() == [{"x": 1, "y": 2, "z": 3}]


def test_sparkframe_where_filter() -> None:
    s = _UsersXY({"x": [1, 2], "y": [10, 20]})
    out = s.where(F.col("x") > F.lit(1))
    assert isinstance(out.plan(), Filter)
    assert out.to_dicts() == [{"x": 2, "y": 20}]


def test_sparkframe_select_column_wrapper() -> None:
    s = _UsersXY({"x": [1], "y": [2]})
    out = s.select(F.col("x").alias("xx"), "y")
    rows = out.to_dicts()
    assert rows == [{"xx": 1, "y": 2}]


def test_sparkframe_union_by_name_reorders_other() -> None:
    a = _UsersXY({"x": [1], "y": [2]})
    b = _UsersYX({"x": [3], "y": [4]})
    u = a.unionByName(b)
    assert u.to_dicts() == [{"x": 1, "y": 2}, {"x": 3, "y": 4}]


def test_sparkframe_count() -> None:
    s = _UsersXY({"x": [1, 2, 3], "y": [0, 0, 0]})
    assert s.count() == 3


def test_sparkframe_na_helpers() -> None:
    s = _UsersXY({"x": [1], "y": [2]})
    filled = s.na.fill(0, subset=["x"])
    assert filled.to_dicts() == [{"x": 1, "y": 2}]


def test_sparkframe_column_sugar() -> None:
    s = _UsersXY({"x": [1], "y": [2]})
    out = s.where(s["x"] > F.lit(0)).select(s.y.alias("yy"), "x")
    assert out.to_dicts() == [{"yy": 2, "x": 1}]


def test_sparkframe_with_columns_plural() -> None:
    s = _UsersXY({"x": [1], "y": [2]})
    out = s.withColumns({"z": s["x"] + 1, "w": F.lit(0)})
    assert out.to_dicts() == [{"x": 1, "y": 2, "z": 2, "w": 0}]


def test_sparkframe_groupby_agg_wrapper() -> None:
    s = _UsersXY({"x": [1, 1, 2], "y": [10, 20, 30]})
    out = s.groupBy("x").agg(y_sum=F.sum("y"))
    assert sorted(out.to_dicts(), key=lambda r: r["x"]) == [
        {"x": 1, "y_sum": 30},
        {"x": 2, "y_sum": 30},
    ]


def test_sparkframe_hint_node_noop_by_default() -> None:
    s = _UsersXY({"x": [1], "y": [2]})
    out = s.hint("broadcast", table="users")
    assert isinstance(out.plan(), Hint)
    assert out.to_dicts() == [{"x": 1, "y": 2}]
