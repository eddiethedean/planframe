from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from typing import Any, cast

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class UserSchema:
    id: int


lf = pl.DataFrame({"id": [1]}).lazy()
pf = cast(Any, PolarsFrame[UserSchema])(lf)


def get_keep() -> str:
    return "first"


bad_keep = get_keep()

# This should fail: keep is Literal["first", "last"].
pf2 = pf.unique("id", keep=bad_keep)
