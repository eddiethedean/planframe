from __future__ import annotations

from dataclasses import dataclass

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int


# should fail: data must be a Polars frame or mapping/rows
pf = PolarsFrame[S](123)

