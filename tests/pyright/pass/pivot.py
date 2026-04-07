from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class S:
    id: int
    k: str
    v: int


pf = from_polars(pl.DataFrame({"id": [1, 1], "k": ["a", "b"], "v": [10, 20]}).lazy(), schema=S)
out = pf.pivot(index=("id",), on="k", values="v", on_columns=("a", "b"))
df = out.collect()

reveal_type(out)
reveal_type(df)

