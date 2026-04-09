from __future__ import annotations

from typing import Any, TypedDict

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from planframe.backend.errors import PlanFrameExecutionError
from planframe.expr import add, agg_sum, col, lit, ne
from planframe_pandas import PandasFrame
from planframe_polars import PolarsFrame


class _Meta(TypedDict):
    x: int
    y: str


class User(PandasFrame):
    id: int
    name: str
    age: int


def test_pandas_construction_and_collect_backend_returns_dataframe() -> None:
    pf = User({"id": [1], "name": ["a"], "age": [10]})
    df = pf.collect_backend()
    assert isinstance(df, pd.DataFrame)
    assert df.to_dict(orient="list") == {"id": [1], "name": ["a"], "age": [10]}


def test_pandas_select_with_column_filter_sort() -> None:
    pf = User({"id": [2, 1, 3], "name": ["b", "a", "c"], "age": [20, 10, 30]})
    out = (
        pf[["id", "age"]]
        .assign(age2=add(col("age"), lit(1)))[ne(col("id"), lit(2))]
        .sort_values("id")
    )
    df = out.collect_backend()
    assert df["id"].to_list() == [1, 3]
    assert df["age2"].to_list() == [11, 31]


def test_pandas_join_inner() -> None:
    left = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})

    class Right(PandasFrame):
        id: int
        x: int

    right = Right({"id": [2, 3], "x": [200, 300]})
    out = left.merge(right, on="id", how="inner")
    df = out.collect_backend()
    assert df.to_dict(orient="list") == {"id": [2], "name": ["b"], "age": [20], "x": [200]}


def test_pandas_group_by_agg_tuple_and_aggexpr() -> None:
    class S(PandasFrame):
        g: int
        x: int

    pf = S({"g": [1, 1, 2], "x": [10, 20, 7]})
    out = pf.groupby("g").agg(n=("count", "x"), sx=agg_sum(col("x"))).sort_values("g")
    df = out.collect_backend()
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

    melted = pf.melt(id_vars=("id",), value_vars=("a", "b"), var_name="k", value_name="v")
    piv = melted.pivot(index=("id",), columns="k", values="v", on_columns=("a", "b"), agg="first")
    df = piv.sort_values("id").collect_backend()
    assert df.columns.tolist() == ["id", "a", "b"]
    assert df.to_dict(orient="list") == {"id": [1, 2], "a": [10, 11], "b": [20, 21]}

    unp = pf.melt(id_vars=("id",), value_vars=("a", "b"), var_name="k", value_name="v")
    df_unp = unp.sort_values("id").collect_backend()
    assert set(df_unp.columns.tolist()) == {"id", "k", "v"}

    exploded = pf.explode("parts")[["id", "parts"]].sort_values("id")
    df2 = exploded.collect_backend()
    assert df2.to_dict(orient="list") == {"id": [1, 1, 2], "parts": [1, 2, 3]}

    unnested = pf.unnest("meta")[["id", "x", "y"]].sort_values("id")
    df3 = unnested.collect_backend()
    assert df3.to_dict(orient="list") == {"id": [1, 2], "x": [1, 2], "y": ["a", "b"]}

    # IO: csv should work
    out_path = tmp_path / "out.csv"
    pf[["id", "a"]].to_csv(str(out_path))
    assert out_path.exists()


def test_pandas_clip_subset_and_all_numeric() -> None:
    pf = User({"id": [1, 2], "age": [-1, 10]})

    df_subset = pf.clip(lower=0, upper=6, subset=("age",)).collect_backend()
    assert df_subset.to_dict(orient="list") == {"id": [1, 2], "age": [0, 6]}

    df_all = pf.clip(lower=0).collect_backend()
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
        pf.sink_parquet(str(out_path))


def test_pandas_drop_nulls_threshold_matches_polars() -> None:
    class RowP(PandasFrame):
        a: int | None
        b: int | None

    class RowL(PolarsFrame):
        a: int | None
        b: int | None

    data = {"a": [1, None, None], "b": [None, 2, None]}
    p = RowP(data).dropna(subset=("a", "b"), thresh=1).collect_backend()
    polars_out = RowL(data).drop_nulls(subset=("a", "b"), threshold=1).collect_backend()
    assert_frame_equal(
        p.reset_index(drop=True),
        polars_out.to_pandas().reset_index(drop=True),
        check_dtype=False,
    )

    p_all = RowP(data).dropna(subset=("a", "b"), how="all", thresh=1).collect_backend()
    polars_all = RowL(data).drop_nulls(subset=("a", "b"), how="all", threshold=1).collect_backend()
    assert_frame_equal(
        p_all.reset_index(drop=True),
        polars_all.to_pandas().reset_index(drop=True),
        check_dtype=False,
    )


def test_pandas_fill_null_strategy_respects_subset_matches_polars() -> None:
    """Strategy fill must not forward/backward-fill columns outside subset (pandas/polars parity)."""

    class RowP(PandasFrame):
        a: float | None
        b: float | None

    class RowL(PolarsFrame):
        a: float | None
        b: float | None

    data = {"a": [None, None, 1.0], "b": [100.0, None, None]}
    p_out = RowP(data).fill_null(None, "a", strategy="forward").collect_backend()
    l_out = RowL(data).fill_null(None, "a", strategy="forward").collect_backend()

    def _floats(series: Any) -> list[float | None]:
        return [
            None if (x is None or (isinstance(x, float) and pd.isna(x))) else float(x)
            for x in series
        ]  # type: ignore[arg-type]

    assert _floats(p_out["a"]) == _floats(l_out["a"])
    assert _floats(p_out["b"]) == _floats(l_out["b"])
    assert _floats(p_out["b"]) == [100.0, None, None]
