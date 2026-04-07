from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar

from planframe.backend.errors import PlanFrameSchemaError
from planframe.plan.nodes import Agg, GroupBy, PlanNode
from planframe.schema.ir import Field, Schema

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")

AggOp = Literal["count", "sum", "mean", "min", "max", "n_unique"]


class GroupedFrame(Generic[SchemaT, BackendFrameT, BackendExprT]):
    __slots__ = ("_data", "_adapter", "_plan", "_schema", "_keys")

    _data: BackendFrameT
    _adapter: Any
    _plan: PlanNode
    _schema: Schema
    _keys: tuple[str, ...]

    def __init__(
        self,
        *,
        _data: BackendFrameT,
        _adapter: Any,
        _plan: PlanNode,
        _schema: Schema,
        _keys: tuple[str, ...],
    ) -> None:
        self._data = _data
        self._adapter = _adapter
        self._plan = _plan
        self._schema = _schema
        self._keys = _keys

    def agg(self, **named_aggs: tuple[AggOp, str]) -> Any:
        if not named_aggs:
            raise PlanFrameSchemaError("agg requires at least one named aggregation")
        # Schema: keys + named aggs (types are conservative)
        out_fields = [self._schema.get(k) for k in self._keys]
        for out_name, (op, col) in named_aggs.items():
            self._schema.get(col)  # validate
            dtype: Any = object
            if op in {"count", "n_unique"}:
                dtype = int
            out_fields.append(Field(name=out_name, dtype=dtype))
        schema2 = Schema(fields=tuple(out_fields))
        plan2 = Agg(GroupBy(self._plan, keys=self._keys), named_aggs=dict(named_aggs))
        from planframe.frame import Frame  # avoid cycle

        return Frame(_data=self._data, _adapter=self._adapter, _plan=plan2, _schema=schema2)
