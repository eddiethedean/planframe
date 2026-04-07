from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class UserSchema:
    id: int
    name: str
    age: int


lf = pl.DataFrame({"id": [1], "name": ["a"], "age": [2]}).lazy()
pf = from_polars(lf, schema=UserSchema)

out = pf.select_exclude("name").select_first("age").rename_prefix("x_", "id").move("x_id", after="age")
df = out.collect()
reveal_type(df)

