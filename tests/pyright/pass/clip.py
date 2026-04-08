from __future__ import annotations

from dataclasses import dataclass

from planframe import Frame
from planframe.expr import col


@dataclass(frozen=True)
class S:
    x: int
    y: float


def f(pf: Frame[S, object, object]) -> None:
    _ = pf.clip(lower=0)
    _ = pf.clip(upper=1.0)
    _ = pf.clip(lower=col("x"), upper=10, subset=("x",))
