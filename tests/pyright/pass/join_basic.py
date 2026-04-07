from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class Left:
    id: int
    name: str


@dataclass(frozen=True)
class Right:
    id: int
    city: str


left_lf = pl.DataFrame({"id": [1], "name": ["a"]}).lazy()
right_lf = pl.DataFrame({"id": [1], "city": ["NY"]}).lazy()

left = from_polars(left_lf, schema=Left)
right = from_polars(right_lf, schema=Right)

out = left.join(right, on=("id",), how="inner", suffix="_right")
df = out.collect()

reveal_type(out)
reveal_type(df)

