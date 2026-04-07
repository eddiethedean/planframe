from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class S:
    id: int
    x_a: int
    x_b: int
    y: int


pf = from_polars(pl.DataFrame({"id": [1], "x_a": [10], "x_b": [20], "y": [30]}).lazy(), schema=S)
out = pf.select_prefix("x_").drop_regex("^x_b$")
df = out.collect()

reveal_type(out)
reveal_type(df)

