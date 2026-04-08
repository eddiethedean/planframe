from __future__ import annotations

from planframe.expr import add, col, lit
from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    age: int


pf = User({"id": [1], "age": [2]})

_ = pf.sort(add(col("id"), col("age")))
_ = pf.sort("id", add(col("age"), lit(1)), descending=[True, False])
