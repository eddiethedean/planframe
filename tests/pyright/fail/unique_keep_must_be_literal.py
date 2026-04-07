from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class UserSchema:
    id: int


lf = pl.DataFrame({"id": [1]}).lazy()
pf = from_polars(lf, schema=UserSchema)

def get_keep() -> str:
    return "first"


bad_keep = get_keep()

# This should fail: keep is Literal["first", "last"].
pf2 = pf.unique("id", keep=bad_keep)

