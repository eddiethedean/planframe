from __future__ import annotations

from typing_extensions import reveal_type

from planframe.expr import add, col, eq, lit
from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    age: int


pf = User({"id": [1], "age": [2]})

out = (
    pf.select("id", "age")
    .with_columns(age_plus_one=add(col("age"), lit(1)))
    .filter(eq(col("id"), lit(1)))
)
df = out.collect()

reveal_type(df)
