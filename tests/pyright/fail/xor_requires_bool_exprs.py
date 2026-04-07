from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from planframe.expr import col, xor
from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int
    x: float


pf = cast(Any, PolarsFrame[S])({"id": [1], "x": [1.2]})

# should fail: xor requires Expr[bool]
_out = pf.with_column("bad", xor(col("id"), col("x")))
