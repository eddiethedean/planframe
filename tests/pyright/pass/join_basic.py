from __future__ import annotations

from planframe_polars import PolarsFrame


class Left(PolarsFrame):
    id: int
    name: str


class Right(PolarsFrame):
    id: int
    city: str


left = Left({"id": [1], "name": ["a"]})
right = Right({"id": [1], "city": ["NY"]})

out = left.join(right, on=("id",), how="inner", suffix="_right")
df = out.collect()

reveal_type(out)
reveal_type(df)

