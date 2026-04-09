from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import polars as pl

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int
    a: int
    b: int


lf = pl.DataFrame({"id": [1], "a": [2], "b": [3]}).lazy()
pf = cast(Any, PolarsFrame[S])(lf)

# This should fail: unpivot expects `on=` to be a sequence of strings (not a mixed/invalid shape here).
pf2 = pf.unpivot(index=("id",), on=123)
