from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class UserSchema:
    id: int
    age: int


lf = pl.DataFrame({"id": [1], "age": [2]}).lazy()
pf = from_polars(lf, schema=UserSchema)

col_name: str = "id"

bad_col_name: int = 1

# This should fail: column names must be strings (ideally literals).
pf2 = pf.select(bad_col_name)

