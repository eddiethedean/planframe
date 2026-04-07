from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int


pf = PolarsFrame.from_polars(pl.DataFrame({"id": [1]}).lazy(), schema=S)

# should fail: maintain_order must be bool
_out = pf.unique("id", maintain_order=1)
