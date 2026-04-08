from __future__ import annotations

from typing import Generic, TypeVar

LeftSchemaT = TypeVar("LeftSchemaT")
RightSchemaT = TypeVar("RightSchemaT")

class JoinedSchema(Generic[LeftSchemaT, RightSchemaT]): ...
