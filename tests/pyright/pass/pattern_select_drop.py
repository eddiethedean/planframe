from __future__ import annotations

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    x_a: int
    x_b: int
    y: int


pf = S({"id": [1], "x_a": [10], "x_b": [20], "y": [30]})
out = pf.select_prefix("x_").drop_regex("^x_b$")
df = out.collect()

reveal_type(out)
reveal_type(df)

