from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExecutionOptions:
    """Backend-agnostic execution-time hints.

    These options are only consulted at execution/materialization boundaries
    (e.g. `collect`, `to_dicts`, `to_dict`) and must not affect schema evolution.
    """

    streaming: bool | None = None
    engine_streaming: bool | None = None
