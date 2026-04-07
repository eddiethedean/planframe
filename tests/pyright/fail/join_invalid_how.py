from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from typing import Any, cast

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class Left:
    id: int


@dataclass(frozen=True)
class Right:
    id: int


left = cast(Any, PolarsFrame[Left])(pl.DataFrame({"id": [1]}).lazy())
right = cast(Any, PolarsFrame[Right])(pl.DataFrame({"id": [1]}).lazy())

# should fail: invalid how literal
_out = left.join(right, on=("id",), how="not_a_join")

