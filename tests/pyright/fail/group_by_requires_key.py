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

# This should fail: keys must be strings (ideally literals).
pf2 = pf.group_by(1)

