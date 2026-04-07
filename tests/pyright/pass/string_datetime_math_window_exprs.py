from __future__ import annotations

from planframe.expr import between, clip, col, contains, day, ends_with, exp, length, lit, log, lower, month, over, pow_, replace, starts_with, upper, year

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    s: str
    x: float
    dt: object


pf = S({"id": [1], "s": ["Hello"], "x": [2.0], "dt": ["2026-01-02"]})

out = (
    pf.with_column("has_hello", contains(lower(col("s")), "hello"))
    .with_column("sw", starts_with(col("s"), "H"))
    .with_column("ew", ends_with(col("s"), "o"))
    .with_column("s_len", length(col("s")))
    .with_column("s2", replace(col("s"), "l", "L", literal=True))
    .with_column("y", year(col("dt")))
    .with_column("m", month(col("dt")))
    .with_column("d", day(col("dt")))
    .with_column("btw", between(col("x"), lit(1.5), lit(3.0)))
    .with_column("clp", clip(col("x"), lower=lit(1.5)))
    .with_column("p", pow_(col("x"), lit(2)))
    .with_column("lg", log(exp(lit(1.0))))
    .with_column("x_over", over(col("x"), partition_by=("id",)))
    .with_column("u", upper(col("s")))
)

df = out.collect()
reveal_type(df)

