from __future__ import annotations

from typing import Any, TypeAlias

from planframe.frame import Frame

from ._schema_types import JoinedSchema as JoinedSchema
from .host_frame import HasInnerFrame as HasInnerFrame
from .scalars import Scalar as Scalar
from .storage import StorageOptions as StorageOptions

FrameAny: TypeAlias = Frame[Any, Any, Any]

__all__ = ["FrameAny", "HasInnerFrame", "JoinedSchema", "Scalar", "StorageOptions"]
