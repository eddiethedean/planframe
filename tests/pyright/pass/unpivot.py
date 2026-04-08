from __future__ import annotations

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    a: int
    b: int


pf = S({"id": [1], "a": [10], "b": [20]})
out = pf.unpivot(index=("id",), on=("a", "b"))
df = out.collect()
