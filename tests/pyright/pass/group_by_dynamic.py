from __future__ import annotations

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    ts: int
    g: str
    x: int


pf = S({"ts": [1], "g": ["a"], "x": [10]})
out = pf.group_by_dynamic("ts", every="1h", by=("g",)).agg(n=("count", "x"))
df = out.collect()
