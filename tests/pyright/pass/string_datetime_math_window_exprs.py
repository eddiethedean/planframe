from __future__ import annotations

from typing_extensions import reveal_type

from planframe.expr import (
    between,
    clip,
    col,
    contains,
    day,
    ends_with,
    exp,
    length,
    lit,
    log,
    lower,
    month,
    over,
    pow_,
    replace,
    starts_with,
    upper,
    year,
)
from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    s: str
    x: float
    dt: object


pf = S({"id": [1], "s": ["Hello"], "x": [2.0], "dt": ["2026-01-02"]})

out = pf.with_columns(
    has_hello=contains(lower(col("s")), "hello"),
    sw=starts_with(col("s"), "H"),
    ew=ends_with(col("s"), "o"),
    s_len=length(col("s")),
    s2=replace(col("s"), "l", "L", literal=True),
    y=year(col("dt")),
    m=month(col("dt")),
    d=day(col("dt")),
    btw=between(col("x"), lit(1.5), lit(3.0)),
    clp=clip(col("x"), lower=lit(1.5)),
    p=pow_(col("x"), lit(2)),
    lg=log(exp(lit(1.0))),
    x_over=over(col("x"), partition_by=("id",)),
    u=upper(col("s")),
)

df = out.collect()
reveal_type(df)
