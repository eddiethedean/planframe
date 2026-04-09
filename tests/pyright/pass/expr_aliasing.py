from __future__ import annotations

from typing_extensions import reveal_type

from planframe.expr import add, col, lit
from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    age: int


pf = S({"id": [1], "age": [2]})

out = pf.with_columns(add(col("age"), lit(1)).alias("age_plus_one")).select(
    col("id"), col("age"), col("age_plus_one")
)
df = out.collect()

reveal_type(out)
reveal_type(df)
