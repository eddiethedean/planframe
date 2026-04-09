from __future__ import annotations

from typing_extensions import reveal_type

from planframe.expr import col, contains, length, lower, replace
from planframe_polars import PolarsFrame


class S(PolarsFrame):
    s: str | None


pf = S({"s": ["a.a", None]})

out = pf.with_columns(
    has_dot=contains(col("s"), ".", literal=True),
    has_dot2=contains(lower(col("s")), ".", literal=False),
    ln=length(col("s")),
    r=replace(col("s"), ".", "_", literal=True),
)

df = out.collect()
reveal_type(df)
