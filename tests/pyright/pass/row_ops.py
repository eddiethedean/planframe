from __future__ import annotations

from typing_extensions import reveal_type

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    age: int


pf = S({"id": [1, 2, 3], "age": [10, 20, 30]})
out = pf.head(2).slice(0, 1).tail(1).limit(1)
df = out.collect()

reveal_type(out)
reveal_type(df)
