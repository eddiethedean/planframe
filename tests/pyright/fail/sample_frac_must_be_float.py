from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int


pf = PolarsFrame.from_polars(pl.DataFrame({"id": [1]}).lazy(), schema=S)

# should fail: frac must be float | None
_out = pf.sample(frac="0.5")

