from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe.expr import col, contains
from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    s: str


pf = PolarsFrame.from_polars(pl.DataFrame({"s": ["x"]}).lazy(), schema=S)

# should fail: literal must be bool
_out = pf.with_columns(bad=contains(col("s"), "x", literal="yes"))
