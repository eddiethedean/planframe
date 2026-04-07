from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe.expr import add, col, lit
from planframe_polars import from_polars


@dataclass(frozen=True)
class UserSchema:
    id: int
    age: int


lf = pl.DataFrame({"id": [1], "age": [2]}).lazy()
pf = from_polars(lf, schema=UserSchema)

out = pf.select("id", "age").with_column("age_plus_one", add(col("age"), lit(1)))

OutDC = out.materialize_model("OutDC", kind="dataclass")
OutPD = out.materialize_model("OutPD", kind="pydantic")

reveal_type(OutDC)
reveal_type(OutPD)

