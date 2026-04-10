from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import Self

from planframe import Frame
from planframe.typing import HasInnerFrame


@dataclass(frozen=True)
class S:
    id: int
    name: str


@dataclass(frozen=True)
class Host(HasInnerFrame[S, object, object]):
    inner: Frame[S, object, object]

    def select_id(self) -> Self:
        # Re-wrap after delegating to Frame.
        return type(self)(self.inner.select("id"))


def f(x: Host) -> None:
    # Ensure `.inner` preserves Frame typing.
    _ = x.inner.select("id", "name")
    # Ensure chaining through wrapper stays well-typed.
    _ = x.select_id().inner.to_dicts()
    # Ensure protocol is usable in helper types.
    y: HasInnerFrame[S, object, object] = x
    _ = y.inner.collect_backend()
