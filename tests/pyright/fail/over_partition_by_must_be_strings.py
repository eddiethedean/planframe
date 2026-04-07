from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe.expr import col, over
from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int
    x: float


pf = PolarsFrame.from_polars(pl.DataFrame({"id": [1], "x": [1.0]}).lazy(), schema=S)

# should fail: partition_by must be tuple[str, ...]
_out = pf.with_column("bad", over(col("x"), partition_by=(1,)))
