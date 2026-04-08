from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from planframe.backend.errors import PlanFrameSchemaError
from planframe.expr.api import AggExpr, Expr, infer_dtype
from planframe.plan.nodes import DynamicGroupByAgg, PlanNode
from planframe.schema.ir import Field, Schema, collect_col_names_in_expr

if TYPE_CHECKING:
    from planframe.backend.adapter import BackendAdapter
    from planframe.frame import Frame

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")

AggOp = Literal["count", "sum", "mean", "min", "max", "n_unique"]


class DynamicGroupedFrame(Generic[SchemaT, BackendFrameT, BackendExprT]):
    __slots__ = (
        "_data",
        "_adapter",
        "_plan",
        "_schema",
        "_index_column",
        "_every",
        "_period",
        "_by",
    )

    _data: BackendFrameT
    _adapter: BackendAdapter[BackendFrameT, BackendExprT]
    _plan: PlanNode
    _schema: Schema
    _index_column: str
    _every: str
    _period: str | None
    _by: tuple[str, ...] | None

    def __init__(
        self,
        *,
        _data: BackendFrameT,
        _adapter: BackendAdapter[BackendFrameT, BackendExprT],
        _plan: PlanNode,
        _schema: Schema,
        _index_column: str,
        _every: str,
        _period: str | None,
        _by: tuple[str, ...] | None,
    ) -> None:
        self._data = _data
        self._adapter = _adapter
        self._plan = _plan
        self._schema = _schema
        self._index_column = _index_column
        self._every = _every
        self._period = _period
        self._by = _by

    def agg(
        self, **named_aggs: tuple[AggOp, str] | Expr[Any]
    ) -> Frame[Any, BackendFrameT, BackendExprT]:
        if not named_aggs:
            raise PlanFrameSchemaError("agg requires at least one named aggregation")

        # Output schema: index + by + agg fields.
        out_fields: list[Field] = []
        out_fields.append(self._schema.get(self._index_column))
        if self._by is not None:
            for c in self._by:
                out_fields.append(self._schema.get(c))

        fm = self._schema.field_map()
        for out_name, spec in named_aggs.items():
            if (
                isinstance(spec, tuple)
                and len(spec) == 2
                and isinstance(spec[0], str)
                and isinstance(spec[1], str)
            ):
                op = spec[0]
                col = spec[1]
                self._schema.get(col)
                dtype: object = object
                if op in {"count", "n_unique"}:
                    dtype = int
                out_fields.append(Field(name=out_name, dtype=dtype))
            elif isinstance(spec, AggExpr):
                missing = collect_col_names_in_expr(spec.inner).difference(fm.keys())
                if missing:
                    raise PlanFrameSchemaError(
                        f"aggregation expression references unknown columns: {sorted(missing)}"
                    )
                out_fields.append(Field(name=out_name, dtype=infer_dtype(spec)))
            else:
                raise PlanFrameSchemaError(
                    "agg expects (op, column_name) tuples or agg_sum/agg_mean/...(...) "
                    f"over an expression, got {type(spec).__name__!r}"
                )

        schema2 = Schema(fields=tuple(out_fields))
        plan2 = DynamicGroupByAgg(
            prev=self._plan,
            index_column=self._index_column,
            every=self._every,
            period=self._period,
            by=self._by,
            named_aggs=dict(named_aggs),
        )

        from planframe.frame import Frame  # avoid cycle

        return Frame(_data=self._data, _adapter=self._adapter, _plan=plan2, _schema=schema2)
