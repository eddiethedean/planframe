from __future__ import annotations

from typing import Protocol


class FrameLike(Protocol):
    """Internal protocol for plan nodes that carry other frames.

    This avoids importing `Frame` into IR modules while still letting type checkers
    understand the attributes used by the evaluator.
    """

    _adapter: object
    _plan: object

    def _eval(self, node: object) -> object: ...
