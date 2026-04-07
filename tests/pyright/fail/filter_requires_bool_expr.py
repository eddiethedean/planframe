from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe.expr import col
from planframe_polars import from_polars


@dataclass(frozen=True)
class UserSchema:
    id: int
    age: int


lf = pl.DataFrame({"id": [1], "age": [2]}).lazy()
pf = from_polars(lf, schema=UserSchema)

# This should fail: filter expects Expr[bool], but col(...) is not boolean.
pf2 = pf.filter(col("age"))

