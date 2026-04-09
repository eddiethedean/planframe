from __future__ import annotations

from typing_extensions import reveal_type

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    a: int | None
    b: int


pf = S({"id": [1], "a": [None], "b": [2]})

out = pf.fill_null(0, "a").drop_nulls(subset=("a",)).unpivot(index=("id",), on=("a", "b"))
df = out.collect()
reveal_type(df)
