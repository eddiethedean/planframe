from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from typing import Any, cast

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int


pf = cast(Any, PolarsFrame[S])(pl.DataFrame({"id": [1]}).lazy())

# should fail: path must be str
pf.write_parquet(123)
