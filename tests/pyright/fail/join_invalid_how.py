from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class Left:
    id: int


@dataclass(frozen=True)
class Right:
    id: int


left = from_polars(pl.DataFrame({"id": [1]}).lazy(), schema=Left)
right = from_polars(pl.DataFrame({"id": [1]}).lazy(), schema=Right)

# should fail: invalid how literal
_out = left.join(right, on=("id",), how="not_a_join")

