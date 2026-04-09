from __future__ import annotations

from typing_extensions import reveal_type

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    age: int


pf1 = S({"id": [1], "age": [10]})
pf2 = S({"id": [2], "age": [20]})

out = pf1.vstack(pf2)
df = out.collect()

reveal_type(out)
reveal_type(df)
