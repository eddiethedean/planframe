from __future__ import annotations

from typing_extensions import reveal_type

from planframe.expr import col, is_finite, split, sqrt, strip
from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    s: str
    x: float


pf = S({"id": [1, 2, 3], "s": [" a,b ", "c,d", "e,f"], "x": [1.0, 4.0, 9.0]})

out = (
    pf.sample(2, seed=1, shuffle=True)
    .with_columns(s2=strip(col("s")))
    .with_columns(parts=split(strip(col("s")), ","))
    .with_columns(r=sqrt(col("x")))
    .with_columns(ok=is_finite(col("x")))
)

df = out.collect()
reveal_type(df)
