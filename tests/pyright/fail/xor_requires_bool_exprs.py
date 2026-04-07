from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe.expr import col, xor
from planframe_polars import from_polars


@dataclass(frozen=True)
class S:
    id: int
    x: float


pf = from_polars(pl.DataFrame({"id": [1], "x": [1.2]}).lazy(), schema=S)

# should fail: xor requires Expr[bool]
_out = pf.with_column("bad", xor(col("id"), col("x")))

