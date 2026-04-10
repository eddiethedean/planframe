from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    from planframe.frame import Frame


# Public helper aliases for adapter authors and host types.
#
# These are intentionally defined in a small module to avoid importing `Frame` at runtime
# (which can create cycles) while still providing stable import paths for type checkers.

FrameAny: TypeAlias = "Frame[Any, Any, Any]"
