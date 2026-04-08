from __future__ import annotations

from typing import Any, Protocol


class FrameLike(Protocol):
    """Internal protocol for plan nodes that carry other frames.

    This avoids importing `Frame` into IR modules while still letting type checkers
    understand the attributes used by the evaluator.
    """

    # Note: these are intentionally minimal and backend-agnostic.
    _adapter: Any
    _plan: Any

    def _eval(self, node: object) -> Any: ...
