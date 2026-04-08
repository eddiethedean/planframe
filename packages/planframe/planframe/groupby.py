from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar

from planframe.backend.errors import PlanFrameSchemaError
from planframe.expr.api import AggExpr, Expr, infer_dtype
from planframe.plan.nodes import Agg, GroupBy, JoinKeyColumn, JoinKeyExpr, PlanNode
from planframe.schema.ir import Field, Schema, collect_col_names_in_expr

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")

AggOp = Literal["count", "sum", "mean", "min", "max", "n_unique"]


class GroupedFrame(Generic[SchemaT, BackendFrameT, BackendExprT]):
    __slots__ = ("_data", "_adapter", "_plan", "_schema", "_key_items")

    _data: BackendFrameT
    _adapter: Any
    _plan: PlanNode
    _schema: Schema
    _key_items: tuple[JoinKeyColumn | JoinKeyExpr, ...]

    def __init__(
        self,
        *,
        _data: BackendFrameT,
        _adapter: Any,
        _plan: PlanNode,
        _schema: Schema,
        _key_items: tuple[JoinKeyColumn | JoinKeyExpr, ...],
    ) -> None:
        self._data = _data
        self._adapter = _adapter
        self._plan = _plan
        self._schema = _schema
        self._key_items = _key_items

    def agg(self, **named_aggs: tuple[AggOp, str] | Expr[Any]) -> Any:
        if not named_aggs:
            raise PlanFrameSchemaError("agg requires at least one named aggregation")
        # Schema: keys + named aggs (types are conservative)
        out_fields: list[Field] = []
        for i, k in enumerate(self._key_items):
            if isinstance(k, JoinKeyColumn):
                out_fields.append(self._schema.get(k.name))
            else:
                out_fields.append(
                    Field(name=f"__pf_g{i}", dtype=infer_dtype(k.expr))
                )
        fm = self._schema.field_map()
        for out_name, spec in named_aggs.items():
            if isinstance(spec, tuple):
                op, col = spec
                self._schema.get(col)  # validate
                dtype: Any = object
                if op in {"count", "n_unique"}:
                    dtype = int
                out_fields.append(Field(name=out_name, dtype=dtype))
            elif isinstance(spec, AggExpr):
                missing = collect_col_names_in_expr(spec.inner).difference(fm.keys())
                if missing:
                    raise PlanFrameSchemaError(
                        "aggregation expression references unknown columns: "
                        f"{sorted(missing)}"
                    )
                out_fields.append(Field(name=out_name, dtype=infer_dtype(spec)))
            else:
                raise PlanFrameSchemaError(
                    "agg expects (op, column_name) tuples or agg_sum/agg_mean/...(...) "
                    f"over an expression, got {type(spec).__name__!r}"
                )
        schema2 = Schema(fields=tuple(out_fields))
        plan2 = Agg(
            GroupBy(self._plan, keys=self._key_items), named_aggs=dict(named_aggs)
        )
        from planframe.frame import Frame  # avoid cycle

        return Frame(_data=self._data, _adapter=self._adapter, _plan=plan2, _schema=schema2)
