from __future__ import annotations

from typing import Any

from planframe.pandas import PandasLikeFrame
from planframe_polars import PolarsFrame


class _Users(PolarsFrame, PandasLikeFrame[Any, Any, Any]):
    x: int
    y: int


df = _Users({"x": [1, 2], "y": [10, 20]})

_ = df.query("x > 1")
_ = df.query("y == 10")
