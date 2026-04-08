from __future__ import annotations

from dataclasses import dataclass

from planframe import Frame


@dataclass(frozen=True)
class S:
    foo: int
    bar: int


def f(pf: Frame[S, object, object]) -> None:
    _ = pf.rename_upper()
    _ = pf.rename_lower("foo")
    _ = pf.rename_title("foo", "bar")
    _ = pf.rename_strip(chars="_")
