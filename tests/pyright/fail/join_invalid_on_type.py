from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import polars as pl

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class Left:
    id: int


@dataclass(frozen=True)
class Right:
    id: int


left = cast(Any, PolarsFrame[Left])(pl.DataFrame({"id": [1]}).lazy())
right = cast(Any, PolarsFrame[Right])(pl.DataFrame({"id": [1]}).lazy())

# should fail: on key must be a (literal) string
_out = left.join(right, on=(1,))
