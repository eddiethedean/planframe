from __future__ import annotations

from typing_extensions import reveal_type

from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    name: str
    age: int


pf = User({"id": [1], "name": ["a"], "age": [2]})

out = (
    pf.select_exclude("name")
    .select_first("age")
    .rename_prefix("x_", "id")
    .move("x_id", after="age")
)
df = out.collect()
reveal_type(df)
