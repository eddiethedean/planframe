from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from planframe_polars import from_polars


@dataclass(frozen=True)
class S:
    id: int


lf = pl.DataFrame({"id": [1, 1]}).lazy()
pf = from_polars(lf, schema=S)

# Typing allows bool keep; runtime backend may reject keep=False depending on implementation.
mask = pf.duplicated("id", keep=False)
df = mask.collect()
reveal_type(df)

