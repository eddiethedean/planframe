from __future__ import annotations

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    a: int | None


pf = S({"id": [1], "a": [None]})

# should error: neither value nor strategy
pf.fill_null()
