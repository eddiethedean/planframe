from __future__ import annotations

from planframe.expr import col, lit, mul
from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    age: int


pf = User({"id": [1], "age": [2]})

# Mixed str columns and (output_name, Expr) tuples in one select / Project step.
_ = pf.select("id", ("doubled", mul(col("age"), lit(2))))
