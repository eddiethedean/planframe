from __future__ import annotations

from dataclasses import dataclass

from planframe import Frame
from planframe.expr import col


@dataclass(frozen=True)
class S:
    a: int
    b: int


def f(pf: Frame[S, object, object]) -> None:
    _ = pf.fill_null_subset(0, "a", "b")
    _ = pf.fill_null_subset(None, "a", strategy="forward")
    _ = pf.fill_null_many({"a": 0, "b": col("a")})
