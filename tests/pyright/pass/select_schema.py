from __future__ import annotations

from dataclasses import dataclass

from planframe import Frame
from planframe.selector import by_name, dtype, prefix, regex, suffix


@dataclass(frozen=True)
class S:
    a: int
    b: int
    name: str


def f(pf: Frame[S, object, object]) -> None:
    _ = pf.select_schema(by_name("a", "b"))
    _ = pf.select_schema(prefix("na"))
    _ = pf.select_schema(suffix("me"))
    _ = pf.select_schema(regex("^a$"))
    _ = pf.select_schema(dtype(is_subclass=int))
