from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class JoinOptions:
    """Optional join hints for backends that support them (for example Polars).

    Fields that are ``None`` are omitted when calling the backend so its defaults apply.
    """

    coalesce: bool | None = None
    validate: str | None = None
    join_nulls: bool | None = None
    maintain_order: str | bool | None = None
    streaming: bool | None = None
