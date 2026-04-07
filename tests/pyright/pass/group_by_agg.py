from __future__ import annotations

from typing_extensions import reveal_type

from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    age: int


pf = User({"id": [1, 1], "age": [10, 20]})

out = pf.group_by("id").agg(total=("sum", "age"), n=("count", "age"))
df = out.collect()
reveal_type(df)
