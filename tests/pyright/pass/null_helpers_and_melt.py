from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class S:
    id: int
    a: int | None
    b: int


lf = pl.DataFrame({"id": [1], "a": [None], "b": [2]}).lazy()
pf = from_polars(lf, schema=S)

out = pf.fill_null(0, "a").drop_nulls("a").melt(id_vars=("id",), value_vars=("a", "b"))
df = out.collect()
reveal_type(df)

