from __future__ import annotations

from typing_extensions import reveal_type

from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    name: str


pf = User({"id": [2, 1, 1], "name": ["b", "a", "a"]})

out = pf.sort("id").unique("id", keep="first")
mask = pf.duplicated("id")

df = out.collect()
df2 = mask.collect()
reveal_type(df)
reveal_type(df2)
