from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import polars as pl

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class UserSchema:
    id: int
    age: int


lf = pl.DataFrame({"id": [1], "age": [2]}).lazy()
pf = cast(Any, PolarsFrame[UserSchema])(lf)

# This should fail: kind is a Literal["dataclass", "pydantic"].
Out = pf.materialize_model("Out", kind="nope")
