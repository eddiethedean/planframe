from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class S:
    id: int
    age: int


pf = from_polars(pl.DataFrame({"id": [1, 2, 3], "age": [10, 20, 30]}).lazy(), schema=S)
out = pf.head(2).slice(0, 1).tail(1).limit(1)
df = out.collect()

reveal_type(out)
reveal_type(df)

