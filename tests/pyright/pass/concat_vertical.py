from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class S:
    id: int
    age: int


pf1 = from_polars(pl.DataFrame({"id": [1], "age": [10]}).lazy(), schema=S)
pf2 = from_polars(pl.DataFrame({"id": [2], "age": [20]}).lazy(), schema=S)

out = pf1.concat_vertical(pf2)
df = out.collect()

reveal_type(out)
reveal_type(df)

