from __future__ import annotations

from typing import Any, Generic, TypeVar, cast

from planframe.expr.api import AggExpr, Expr
from planframe.groupby import GroupedFrame

from .column import Column, unwrap_expr

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


class GroupedData(Generic[SchemaT, BackendFrameT, BackendExprT]):
    """Spark-ish wrapper over core `GroupedFrame` with a typed `.agg` surface."""

    __slots__ = ("_g",)

    def __init__(self, g: GroupedFrame[SchemaT, BackendFrameT, BackendExprT]) -> None:
        self._g = g

    def agg(self, **named_aggs: Column[Any] | Expr[Any]) -> Any:
        # Accept only aggregation expressions (AggExpr) wrapped in Column or as Expr.
        out: dict[str, AggExpr] = {}
        for name, value in named_aggs.items():
            e = unwrap_expr(value) if isinstance(value, Column) else value
            if not isinstance(e, AggExpr):
                raise TypeError(
                    "GroupedData.agg expects Spark functions (sum/mean/min/max/count/n_unique) "
                    "which produce aggregation expressions."
                )
            out[name] = e
        return self._g.agg(**cast(Any, out))


__all__ = ["GroupedData"]
