from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class UserSchema:
    id: int
    age: int


lf = pl.DataFrame({"id": [1, 1], "age": [10, 20]}).lazy()
pf = from_polars(lf, schema=UserSchema)

out = pf.group_by("id").agg(total=("sum", "age"), n=("count", "age"))
df = out.collect()
reveal_type(df)

