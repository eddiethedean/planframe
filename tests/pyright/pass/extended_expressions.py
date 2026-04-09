from __future__ import annotations

from typing_extensions import reveal_type

from planframe.expr import and_, col, gt, is_not_null, isin, lit, mul
from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    age: int | None


pf = User({"id": [1], "age": [2]})

out = (
    pf.select("id", "age")
    .filter(and_(is_not_null(col("age")), isin(col("id"), 1, 2, 3)))
    .with_columns(age2=mul(col("age"), lit(2)))
    .filter(gt(col("age2"), lit(1)))
)

df = out.collect()
reveal_type(df)
