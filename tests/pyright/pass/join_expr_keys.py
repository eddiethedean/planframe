from __future__ import annotations

from planframe.expr import col, lower
from planframe_polars import PolarsFrame


class L(PolarsFrame):
    id: int
    email: str


class R(PolarsFrame):
    rid: int
    email_norm: str


left = L({"id": [1], "email": ["a"]})
right = R({"rid": [1], "email_norm": ["a"]})

_ = left.join(
    right,
    left_on=(lower(col("email")),),
    right_on=(lower(col("email_norm")),),
    how="inner",
)
