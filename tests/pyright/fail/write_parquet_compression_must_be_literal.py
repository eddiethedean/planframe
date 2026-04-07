from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from typing import Any, cast

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int


pf = cast(Any, PolarsFrame[S])(pl.DataFrame({"id": [1]}).lazy())

def get_comp() -> str:
    return "zstd"

# should fail: compression must be a known Literal
pf.write_parquet("out.parquet", compression=get_comp())

