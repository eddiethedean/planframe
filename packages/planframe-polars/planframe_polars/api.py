from __future__ import annotations

from typing import Any

import polars as pl

from planframe.frame import Frame
from planframe_polars.adapter import PolarsAdapter, PolarsFrame


def from_polars(df: PolarsFrame, *, schema: type[Any]) -> Frame[Any, PolarsFrame, pl.Expr]:
    return Frame.source(df, adapter=PolarsAdapter(), schema=schema)

