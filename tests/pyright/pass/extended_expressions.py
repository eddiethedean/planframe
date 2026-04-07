from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe.expr import and_, col, gt, is_not_null, isin, lit, mul
from planframe_polars import from_polars


@dataclass(frozen=True)
class UserSchema:
    id: int
    age: int | None


lf = pl.DataFrame({"id": [1], "age": [2]}).lazy()
pf = from_polars(lf, schema=UserSchema)

out = (
    pf.select("id", "age")
    .filter(and_(is_not_null(col("age")), isin(col("id"), 1, 2, 3)))
    .with_column("age2", mul(col("age"), lit(2)))
    .filter(gt(col("age2"), lit(1)))
)

df = out.collect()
reveal_type(df)

