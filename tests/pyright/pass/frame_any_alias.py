from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from planframe.typing import FrameAny
from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class User(PolarsFrame):
    id: int
    age: int


def f(pf: User) -> None:
    widened: FrameAny = pf
    _ = widened.select("id")
    _ = cast(Any, widened).to_dicts()
