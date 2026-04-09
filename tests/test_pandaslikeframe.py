from __future__ import annotations

import pytest

pytest.importorskip("polars")

from planframe.pandas import PandasLikeFrame
from planframe.plan.nodes import Join, Sort, WithColumn
from planframe_polars import PolarsFrame


class _A(PolarsFrame, PandasLikeFrame):
    x: int
    y: int


class _B(PolarsFrame, PandasLikeFrame):
    x: int
    z: int


def test_pandaslike_assign() -> None:
    df = _A({"x": [1], "y": [2]})
    out = df.assign(z=df["x"] + 1)
    assert isinstance(out.plan(), WithColumn)
    assert out.to_dicts() == [{"x": 1, "y": 2, "z": 2}]


def test_pandaslike_sort_values() -> None:
    df = _A({"x": [2, 1], "y": [0, 0]})
    out = df.sort_values("x")
    assert isinstance(out.plan(), Sort)
    assert out.to_dicts() == [{"x": 1, "y": 0}, {"x": 2, "y": 0}]


def test_pandaslike_merge_on() -> None:
    left = _A({"x": [1, 2], "y": [10, 20]})
    right = _B({"x": [2], "z": [99]})
    out = left.merge(right, on="x", how="inner")
    assert isinstance(out.plan(), Join)
    assert out.to_dicts() == [{"x": 2, "y": 20, "z": 99}]


def test_pandaslike_fillna_subset() -> None:
    df = _A({"x": [1], "y": [2]})
    # no nulls, but still should be a no-op on data
    out = df.fillna(0, subset=["x"])
    assert out.to_dicts() == [{"x": 1, "y": 2}]


def test_pandaslike_getitem_select() -> None:
    df = _A({"x": [1], "y": [2]})
    out = df[["y", "x"]]
    assert out.to_dicts() == [{"y": 2, "x": 1}]


def test_pandaslike_query_typed() -> None:
    df = _A({"x": [1, 2], "y": [10, 20]})
    out = df.query(df["x"] > 1)
    assert out.to_dicts() == [{"x": 2, "y": 20}]


def test_pandaslike_bool_indexing_sugar() -> None:
    df = _A({"x": [1, 2], "y": [10, 20]})
    out = df[df["x"] > 1]
    assert out.to_dicts() == [{"x": 2, "y": 20}]


def test_pandaslike_filter_items_and_regex() -> None:
    df = _A({"x": [1], "y": [2]})
    assert df.filter(items=["y"]).to_dicts() == [{"y": 2}]
    assert df.filter(regex="^x$").to_dicts() == [{"x": 1}]


def test_pandaslike_astype_eval_drop_duplicates() -> None:
    df = _A({"x": [1, 1], "y": [2, 2]})
    out = df.eval(z=df["x"] + 1).astype({"x": int}, errors="raise").drop_duplicates()
    assert out.to_dicts() == [{"x": 1, "y": 2, "z": 2}]
