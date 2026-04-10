from __future__ import annotations

from planframe.expr import col


def f() -> None:
    _ = col("age") > 0
    _ = col("age") >= 0
    _ = col("age") < 0
    _ = col("age") <= 0
