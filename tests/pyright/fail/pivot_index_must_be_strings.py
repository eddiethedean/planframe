from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from typing import Any, cast

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int
    k: str
    v: int


pf = cast(Any, PolarsFrame[S])(pl.DataFrame({"id": [1, 1], "k": ["a", "b"], "v": [10, 20]}).lazy())

# should fail: index must be tuple[str]
_out = pf.pivot(index=(1,), on="k", values="v", on_columns=("a", "b"))
