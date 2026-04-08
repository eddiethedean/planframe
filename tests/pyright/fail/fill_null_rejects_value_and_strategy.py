from __future__ import annotations

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    a: int | None


pf = S({"id": [1], "a": [None]})

# should error: value and strategy together
pf.fill_null(0, "a", strategy="forward")

