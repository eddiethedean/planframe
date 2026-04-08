from __future__ import annotations

from dataclasses import dataclass

from planframe import Frame


@dataclass(frozen=True)
class S:
    id: int
    a: int
    b: int


def f(pf: Frame[S, object, object]) -> None:
    _ = pf.pivot_longer(id_vars=("id",), value_vars=("a", "b"))
    _ = pf.pivot_wider(
        index=("id",), names_from="variable", values_from=("value",), on_columns=("a", "b")
    )
