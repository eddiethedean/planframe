from __future__ import annotations

from typing import Any

from planframe.pandas import PandasLikeFrame
from planframe.spark import SparkFrame
from planframe.spark import functions as F
from planframe_polars import PolarsFrame


class S_spark(PolarsFrame, SparkFrame[Any, Any, Any]):
    x: int
    y: int


df = S_spark({"x": [1], "y": [2]})

# Spark sugar
_ = df["x"] + F.lit(1)
_ = df.x + F.lit(1)
_ = df.withColumns({"z": df["x"] + 1})
_ = df.groupBy("x").agg(y_sum=F.sum("y"))
_ = df.hint("broadcast", table="S")


class S_pandas(PolarsFrame, PandasLikeFrame[Any, Any, Any]):
    x: int
    y: int


df2 = S_pandas({"x": [1], "y": [2]})

# Pandas sugar (typed; no string query/eval)
_ = df2.assign(z=df2["x"] + 1).eval(w=df2["y"] + 2)
_ = df2[df2["x"] > 0]
_ = df2[["y", "x"]]
_ = df2.filter(items=["x"])
_ = df2.astype({"x": int})
_ = df2.drop_duplicates()
