from __future__ import annotations

from typing import Any, TypedDict

import pandas as pd
import pytest

from planframe.backend.errors import PlanFrameExecutionError
from planframe.expr import add, agg_sum, col, lit, ne
from planframe_pandas import PandasFrame


class _Meta(TypedDict):
    x: int
    y: str


class User(PandasFrame):
    id: int
    name: str
    age: int


def test_pandas_construction_and_collect_returns_dataframe() -> None:
    pf = User({"id": [1], "name": ["a"], "age": [10]})
    df = pf.collect()
    assert isinstance(df, pd.DataFrame)
    assert df.to_dict(orient="list") == {"id": [1], "name": ["a"], "age": [10]}


def test_pandas_select_with_column_filter_sort() -> None:
    pf = User({"id": [2, 1, 3], "name": ["b", "a", "c"], "age": [20, 10, 30]})
    out = (
        pf.select("id", "age")
        .with_column("age2", add(col("age"), lit(1)))
        .filter(ne(col("id"), lit(2)))
        .sort("id")
    )
    df = out.collect()
    assert df["id"].to_list() == [1, 3]
    assert df["age2"].to_list() == [11, 31]


def test_pandas_join_inner() -> None:
    left = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})

    class Right(PandasFrame):
        id: int
        x: int

    right = Right({"id": [2, 3], "x": [200, 300]})
    out = left.join(right, on=("id",), how="inner")
    df = out.collect()
    assert df.to_dict(orient="list") == {"id": [2], "name": ["b"], "age": [20], "x": [200]}


def test_pandas_group_by_agg_tuple_and_aggexpr() -> None:
    class S(PandasFrame):
        g: int
        x: int

    pf = S({"g": [1, 1, 2], "x": [10, 20, 7]})
    out = pf.group_by("g").agg(n=("count", "x"), sx=agg_sum(col("x"))).sort("g")
    df = out.collect()
    assert df.columns.tolist() == ["g", "n", "sx"]
    assert df["g"].to_list() == [1, 2]
    assert df["n"].to_list() == [2, 1]
    assert df["sx"].to_list() == [30, 7]


def test_pandas_melt_pivot_explode_unnest(tmp_path: Any) -> None:
    class S(PandasFrame):
        id: int
        a: int
        b: int
        parts: list[int]
        meta: _Meta

    pf = S(
        [
            {"id": 1, "a": 10, "b": 20, "parts": [1, 2], "meta": {"x": 1, "y": "a"}},
            {"id": 2, "a": 11, "b": 21, "parts": [3], "meta": {"x": 2, "y": "b"}},
        ]
    )

    melted = pf.melt(id_vars=("id",), value_vars=("a", "b"), variable_name="k", value_name="v")
    piv = melted.pivot(index=("id",), columns="k", values="v", on_columns=("a", "b"), agg="first")
    df = piv.sort("id").collect()
    assert df.columns.tolist() == ["id", "a", "b"]
    assert df.to_dict(orient="list") == {"id": [1, 2], "a": [10, 11], "b": [20, 21]}

    # unpivot alias
    unp = pf.unpivot(index=("id",), on=("a", "b"), variable_name="k", value_name="v")
    df_unp = unp.sort("id").collect()
    assert set(df_unp.columns.tolist()) == {"id", "k", "v"}

    exploded = pf.explode("parts").select("id", "parts").sort("id")
    df2 = exploded.collect()
    assert df2.to_dict(orient="list") == {"id": [1, 1, 2], "parts": [1, 2, 3]}

    unnested = pf.unnest("meta").select("id", "x", "y").sort("id")
    df3 = unnested.collect()
    assert df3.to_dict(orient="list") == {"id": [1, 2], "x": [1, 2], "y": ["a", "b"]}

    # IO: csv should work
    out_path = tmp_path / "out.csv"
    pf.select("id", "a").write_csv(str(out_path))
    assert out_path.exists()


def test_pandas_clip_subset_and_all_numeric() -> None:
    pf = User({"id": [1, 2], "age": [-1, 10]})

    df_subset = pf.clip(lower=0, upper=6, subset=("age",)).collect()
    assert df_subset.to_dict(orient="list") == {"id": [1, 2], "age": [0, 6]}

    df_all = pf.clip(lower=0).collect()
    assert df_all.to_dict(orient="list") == {"id": [1, 2], "age": [0, 10]}


def test_pandas_write_parquet_raises_clear_error_without_pyarrow(tmp_path: Any) -> None:
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        pass
    else:
        pytest.skip("This path only applies when pyarrow is not installed (e.g. minimal env).")

    pf = User({"id": [1], "name": ["a"], "age": [10]})
    out_path = tmp_path / "out.parquet"
    with pytest.raises(PlanFrameExecutionError):
        pf.write_parquet(str(out_path))
