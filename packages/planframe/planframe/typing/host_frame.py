from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypeVar

if TYPE_CHECKING:
    from planframe.frame import Frame

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


class HasInnerFrame(Protocol[SchemaT, BackendFrameT, BackendExprT]):
    """Protocol for host types that embed a `Frame` by composition.

    This is intended for third-party libraries that wrap PlanFrame in a façade type
    (e.g. to carry validation state, I/O handles, or application-specific methods)
    while still exposing a Pyright-friendly path back to the underlying `Frame`.
    """

    inner: Frame[SchemaT, BackendFrameT, BackendExprT]
