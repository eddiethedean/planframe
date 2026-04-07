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

# This should fail: with_column expects an Expr[T], not an int.
pf2 = pf.with_column("x", 1)

