from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import polars as pl

from planframe.expr import col
from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class UserSchema:
    id: int
    age: int


lf = pl.DataFrame({"id": [1], "age": [2]}).lazy()
pf = cast(Any, PolarsFrame[UserSchema])(lf)

# This should fail: filter expects Expr[bool], but col(...) is not boolean.
pf2 = pf.filter(col("age"))
