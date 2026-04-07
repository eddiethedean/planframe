from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar

from typing_extensions import LiteralString

from planframe.frame import Frame

BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")
SchemaT = TypeVar("SchemaT")

AggOp = Literal["count", "sum", "mean", "min", "max", "n_unique"]


class GroupedFrame(Generic[SchemaT, BackendFrameT, BackendExprT]):
    def agg(self, **named_aggs: tuple[AggOp, LiteralString]) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...

