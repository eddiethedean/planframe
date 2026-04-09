from __future__ import annotations

import pandas as pd
import pytest

from planframe.expr import add, agg_sum, col, eq, lit
from planframe_pandas import PandasFrame

pytestmark = pytest.mark.conformance


class User(PandasFrame):
    id: int
    name: str
    age: int


def test_pandasframe_blocks_core_verbs() -> None:
    pf = User({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(NotImplementedError, match="select"):
        pf.select("id")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="with_columns"):
        pf.with_columns(x=lit(1))  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="sort"):
        pf.sort("id")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="join"):
        pf.join(pf, on=("id",))  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="unpivot"):
        pf.unpivot(index=("id",), on=("age",), variable_name="k", value_name="v")  # type: ignore[arg-type]


def test_pandas_ui_read_transform_collect_backend() -> None:
    pf = User({"id": [2, 1], "name": ["b", "a"], "age": [20, 10]})
    out = pf[["id", "age"]].assign(age_plus_one=add(col("age"), lit(1)))[eq(col("id"), lit(1))]
    df = out.collect_backend()
    assert isinstance(df, pd.DataFrame)
    assert df.to_dict(orient="list") == {"id": [1], "age": [10], "age_plus_one": [11]}


def test_pandas_ui_merge_groupby_melt() -> None:
    left = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})

    class Right(PandasFrame):
        id: int
        x: int

    right = Right({"id": [2], "x": [200]})
    merged = left.merge(right, on="id", how="inner")
    assert merged.collect_backend().to_dict(orient="list") == {
        "id": [2],
        "name": ["b"],
        "age": [20],
        "x": [200],
    }

    class S(PandasFrame):
        g: int
        x: int

    pf = S({"g": [1, 1, 2], "x": [10, 20, 7]})
    gb = pf.groupby("g").agg(n=("count", "x"), sx=agg_sum(col("x"))).sort_values("g")
    assert gb.collect_backend().to_dict(orient="list") == {"g": [1, 2], "n": [2, 1], "sx": [30, 7]}

    class M(PandasFrame):
        id: int
        a: int
        b: int

    m = M({"id": [1], "a": [10], "b": [20]}).melt(id_vars=("id",), value_vars=("a", "b"))
    assert set(m.collect_backend().columns.tolist()) == {"id", "variable", "value"}
