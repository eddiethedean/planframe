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
out = (
    pf.select("id", "x", "a", "b")
    .with_column("ax", abs_(col("x")))
    .with_column("rx", round_(col("x"), 0))
    .with_column("fx", floor(col("x")))
    .with_column("cx", ceil(col("x")))
    .with_column("c", coalesce(col("a"), col("b")))
    .with_column("flag", xor(eq(col("id"), lit(1)), eq(col("id"), lit(2))))
    .with_column("picked", if_else(eq(col("id"), lit(1)), lit("one"), lit("other")))
)

df = out.collect()
reveal_type(out)
reveal_type(df)
