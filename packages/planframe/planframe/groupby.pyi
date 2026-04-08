from __future__ import annotations

from typing import Generic, Literal, TypeVar

from typing_extensions import LiteralString

from planframe.backend.adapter import BackendAdapter
from planframe.expr.api import AggExpr
from planframe.frame import Frame
from planframe.plan.nodes import JoinKeyColumn, JoinKeyExpr, PlanNode
from planframe.schema.ir import Schema

BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")
SchemaT = TypeVar("SchemaT")

AggOp = Literal["count", "sum", "mean", "min", "max", "n_unique"]

class GroupedFrame(Generic[SchemaT, BackendFrameT, BackendExprT]):
    def __init__(
        self,
        *,
        _data: BackendFrameT,
        _adapter: BackendAdapter[BackendFrameT, BackendExprT],
        _plan: PlanNode,
        _schema: Schema,
        _key_items: tuple[JoinKeyColumn | JoinKeyExpr, ...],
    ) -> None: ...
    def agg(
        self, **named_aggs: tuple[AggOp, LiteralString] | AggExpr
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...
