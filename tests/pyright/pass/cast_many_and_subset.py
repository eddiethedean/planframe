from __future__ import annotations

from dataclasses import dataclass

from planframe import Frame


@dataclass(frozen=True)
class S:
    a: int
    b: int


def f(pf: Frame[S, object, object]) -> None:
    _ = pf.cast_many({"a": float, "b": str})
    _ = pf.cast_subset("a", "b", dtype=float)
