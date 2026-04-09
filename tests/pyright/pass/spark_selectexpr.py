from __future__ import annotations

from typing import Any

from planframe.spark import SparkFrame
from planframe_polars import PolarsFrame


class _Users(PolarsFrame, SparkFrame[Any, Any, Any]):
    x: int
    y: int


df = _Users({"x": [1], "y": [2]})

_ = df.selectExpr("x")
_ = df.selectExpr("x AS xx", "y")
