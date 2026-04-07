from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe.expr import col, replace
from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    s: str


pf = PolarsFrame.from_polars(pl.DataFrame({"s": ["x"]}).lazy(), schema=S)

# should fail: literal must be bool
_out = pf.with_column("bad", replace(col("s"), "x", "y", literal="no"))
