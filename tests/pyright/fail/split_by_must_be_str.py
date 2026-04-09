from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe.expr import col, split
from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    s: str


pf = PolarsFrame.from_polars(pl.DataFrame({"s": ["a,b"]}).lazy(), schema=S)

# should fail: by must be str
_out = pf.with_columns(x=split(col("s"), 1))
