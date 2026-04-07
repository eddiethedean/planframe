from __future__ import annotations

from planframe_polars import PolarsFrame
from typing_extensions import reveal_type


class S(PolarsFrame):
    id: int
    age: int


pf1 = S({"id": [1], "age": [10]})
pf2 = S({"id": [2], "age": [20]})

out = pf1.concat_vertical(pf2)
df = out.collect()

reveal_type(out)
reveal_type(df)
