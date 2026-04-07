from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from typing import Any, cast

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int


pf = cast(Any, PolarsFrame[S])(pl.DataFrame({"id": [1]}).lazy())

def get_mode() -> str:
    return "append"

# should fail: if_table_exists must be a known Literal
pf.write_database("t", connection=object(), if_table_exists=get_mode())

