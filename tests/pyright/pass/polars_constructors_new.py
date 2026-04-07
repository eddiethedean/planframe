from __future__ import annotations

from dataclasses import dataclass
from planframe_polars import PolarsFrame
from typing_extensions import reveal_type


@dataclass(frozen=True)
class UserSchema:
    id: int
    name: str
    age: int


class User(PolarsFrame):
    id: int
    name: str
    age: int


pf1 = User({"id": [1], "name": ["a"], "age": [10]})
df1 = pf1.select("id").collect()
reveal_type(df1)

pf2 = User([{"id": 1, "name": "a", "age": 10}])
df2 = pf2.select("age").collect()
reveal_type(df2)
