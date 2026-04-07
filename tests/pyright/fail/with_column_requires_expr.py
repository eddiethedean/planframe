from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from typing import Any, cast

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class UserSchema:
    id: int
    age: int


lf = pl.DataFrame({"id": [1], "age": [2]}).lazy()
pf = cast(Any, PolarsFrame[UserSchema])(lf)

# This should fail: with_column expects an Expr[T], not an int.
pf2 = pf.with_column("x", 1)
