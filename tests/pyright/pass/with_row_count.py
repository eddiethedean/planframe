from __future__ import annotations

from dataclasses import dataclass

from planframe import Frame


@dataclass(frozen=True)
class S:
    id: int


def f(pf: Frame[S, object, object]) -> None:
    out = pf.with_row_count()
    out2 = pf.with_row_count(name="rn", offset=5)
    _ = (out, out2)
