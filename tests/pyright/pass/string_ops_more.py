from __future__ import annotations

from planframe.expr import col, contains, length, lower, replace

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    s: str | None


pf = S({"s": ["a.a", None]})

out = (
    pf.with_column("has_dot", contains(col("s"), ".", literal=True))
    .with_column("has_dot2", contains(lower(col("s")), ".", literal=False))
    .with_column("ln", length(col("s")))
    .with_column("r", replace(col("s"), ".", "_", literal=True))
)

df = out.collect()
reveal_type(df)

