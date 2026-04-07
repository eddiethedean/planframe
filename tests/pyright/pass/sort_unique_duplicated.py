from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class UserSchema:
    id: int
    name: str


lf = pl.DataFrame({"id": [2, 1, 1], "name": ["b", "a", "a"]}).lazy()
pf = from_polars(lf, schema=UserSchema)

out = pf.sort("id").unique("id", keep="first")
mask = pf.duplicated("id")

df = out.collect()
df2 = mask.collect()
reveal_type(df)
reveal_type(df2)

