from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class JoinOptions:
    """Optional join hints for backends that support them (for example Polars).

    Fields that are ``None`` are omitted when calling the backend so its defaults apply.

    ``streaming`` / ``engine_streaming`` mirror :class:`planframe.execution_options.ExecutionOptions`
    (user-level streaming vs engine-level streaming); backends may support none, one, or both.
    """

    coalesce: bool | None = None
    validate: str | None = None
    join_nulls: bool | None = None
    maintain_order: str | bool | None = None
    streaming: bool | None = None
    engine_streaming: bool | None = None
    allow_parallel: bool | None = None
    force_parallel: bool | None = None
