from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe.expr import col, xor
from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int
    x: float


from typing import Any, cast

pf = cast(Any, PolarsFrame[S])(pl.DataFrame({"id": [1], "x": [1.2]}).lazy())

# should fail: xor requires Expr[bool]
_out = pf.with_column("bad", xor(col("id"), col("x")))

