from __future__ import annotations

from collections.abc import Sequence


def _coerce_sort_flags(name: str, n: int, value: bool | Sequence[bool]) -> tuple[bool, ...]:
    if isinstance(value, bool):
        return (value,) * n
    seq = tuple(value)
    if len(seq) != n:
        raise ValueError(
            f"sort {name} must be a bool or a sequence of length {n} "
            f"(number of sort keys), got length {len(seq)}"
        )
    for i, x in enumerate(seq):
        if not isinstance(x, bool):
            raise TypeError(
                f"sort {name} must contain only bool values, got {type(x).__name__!r} at index {i}"
            )
    return seq
