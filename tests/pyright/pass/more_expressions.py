from __future__ import annotations

from typing_extensions import reveal_type

from planframe.expr import abs_, ceil, coalesce, col, eq, floor, if_else, lit, round_, xor
from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    x: float
    a: int | None
    b: int | None


pf = S({"id": [1], "x": [-1.2], "a": [None], "b": [10]})
out = pf.select("id", "x", "a", "b").with_columns(
    ax=abs_(col("x")),
    rx=round_(col("x"), 0),
    fx=floor(col("x")),
    cx=ceil(col("x")),
    c=coalesce(col("a"), col("b")),
    flag=xor(eq(col("id"), lit(1)), eq(col("id"), lit(2))),
    picked=if_else(eq(col("id"), lit(1)), lit("one"), lit("other")),
)

df = out.collect()
reveal_type(out)
reveal_type(df)
