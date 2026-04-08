from __future__ import annotations

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    ts: int
    x: int


pf = S({"ts": [1], "x": [10]})
out = pf.rolling_agg(on="ts", column="x", window_size=2, op="mean", out_name="x_roll")
df = out.collect()
