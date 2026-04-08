from __future__ import annotations

from planframe.expr import add, col, lit
from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    a: int | None
    b: int


pf = S({"id": [1], "a": [None], "b": [2]})

_ = pf.fill_null(0, "a")
_ = pf.fill_null(add(col("b"), lit(1)), "a")
_ = pf.fill_null(None, "a", strategy="forward")

