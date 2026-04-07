from __future__ import annotations

from planframe.expr import add, col, eq, lit

from planframe_polars import PolarsFrame
from typing_extensions import reveal_type


class User(PolarsFrame):
    id: int
    age: int


pf = User({"id": [1], "age": [2]})

out = (
    pf.select("id", "age")
    .with_column("age_plus_one", add(col("age"), lit(1)))
    .filter(eq(col("id"), lit(1)))
)
df = out.collect()

reveal_type(df)
