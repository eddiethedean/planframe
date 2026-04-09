from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Generic, Literal, NoReturn, TypeVar, overload

from typing_extensions import LiteralString, Self

from planframe.backend.adapter import BackendAdapter
from planframe.dynamic_groupby import DynamicGroupedFrame
from planframe.execution_options import ExecutionOptions
from planframe.expr.api import Expr
from planframe.groupby import GroupedFrame
from planframe.plan.join_options import JoinOptions
from planframe.plan.nodes import PlanNode
from planframe.schema.ir import Schema
from planframe.selector import ColumnSelector
from planframe.typing._schema_types import JoinedSchema
from planframe.typing.scalars import Scalar
from planframe.typing.storage import StorageOptions

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")
OtherSchemaT = TypeVar("OtherSchemaT")
T = TypeVar("T")

class Frame(Generic[SchemaT, BackendFrameT, BackendExprT]):
    _data: BackendFrameT
    _adapter: BackendAdapter[BackendFrameT, BackendExprT]
    _plan: PlanNode
    _schema: Schema

    def __init__(
        self,
        _data: BackendFrameT,
        _adapter: BackendAdapter[BackendFrameT, BackendExprT],
        _plan: PlanNode,
        _schema: Schema,
    ) -> None: ...
    @classmethod
    def source(
        cls,
        data: BackendFrameT,
        *,
        adapter: BackendAdapter[BackendFrameT, BackendExprT],
        schema: type[SchemaT],
    ) -> Self: ...
    def schema(self) -> Schema: ...
    def plan(self) -> PlanNode: ...
    def optimize(self, *, level: Literal[0, 1, 2] = ...) -> Self: ...
    def explain(self, *_: object, **__: object) -> NoReturn: ...
    def profile(self, *_: object, **__: object) -> NoReturn: ...
    def show_graph(self, *_: object, **__: object) -> NoReturn: ...

    # NOTE: Pyright's behavior around LiteralString vs str can be permissive.
    # Overloads here are intended to encourage literal call sites and improve IDE help.
    @overload
    def select(self, __c1: LiteralString) -> Self: ...
    @overload
    def select(self, __c1: LiteralString, __c2: LiteralString) -> Self: ...
    @overload
    def select(self, __c1: LiteralString, __c2: LiteralString, __c3: LiteralString) -> Self: ...
    @overload
    def select(
        self, __c1: LiteralString, __c2: LiteralString, __c3: LiteralString, __c4: LiteralString
    ) -> Self: ...
    @overload
    def select(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
    ) -> Self: ...
    @overload
    def select(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
    ) -> Self: ...
    @overload
    def select(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
    ) -> Self: ...
    @overload
    def select(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
    ) -> Self: ...
    @overload
    def select(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        __c9: LiteralString,
    ) -> Self: ...
    @overload
    def select(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        __c9: LiteralString,
        __c10: LiteralString,
    ) -> Self: ...
    @overload
    def select(self, *columns: LiteralString) -> Self: ...
    @overload
    def select(
        self, *columns: LiteralString | tuple[str, Expr[Any]] | Expr[Any], **named_exprs: Expr[Any]
    ) -> Self: ...
    def select(self, *columns: Any) -> Self: ...
    def select_prefix(self, prefix: str) -> Self: ...
    def select_suffix(self, suffix: str) -> Self: ...
    def select_regex(self, pattern: str) -> Self: ...
    def select_schema(self, selector: ColumnSelector, *, strict: bool = ...) -> Self: ...
    @overload
    def select_exclude(self, __c1: LiteralString) -> Self: ...
    @overload
    def select_exclude(self, __c1: LiteralString, __c2: LiteralString) -> Self: ...
    @overload
    def select_exclude(
        self, __c1: LiteralString, __c2: LiteralString, __c3: LiteralString
    ) -> Self: ...
    @overload
    def select_exclude(
        self, __c1: LiteralString, __c2: LiteralString, __c3: LiteralString, __c4: LiteralString
    ) -> Self: ...
    @overload
    def select_exclude(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
    ) -> Self: ...
    @overload
    def select_exclude(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
    ) -> Self: ...
    @overload
    def select_exclude(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
    ) -> Self: ...
    @overload
    def select_exclude(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
    ) -> Self: ...
    @overload
    def select_exclude(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        __c9: LiteralString,
    ) -> Self: ...
    @overload
    def select_exclude(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        __c9: LiteralString,
        __c10: LiteralString,
    ) -> Self: ...
    @overload
    def select_exclude(self, *columns: LiteralString) -> Self: ...
    @overload
    def select_exclude(self, *columns: str) -> Self: ...
    def select_exclude(self, *columns: Any) -> Self: ...
    @overload
    def drop(self, __c1: LiteralString, *, strict: bool = True) -> Self: ...
    @overload
    def drop(self, __c1: LiteralString, __c2: LiteralString, *, strict: bool = True) -> Self: ...
    @overload
    def drop(
        self, __c1: LiteralString, __c2: LiteralString, __c3: LiteralString, *, strict: bool = True
    ) -> Self: ...
    @overload
    def drop(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        *,
        strict: bool = True,
    ) -> Self: ...
    @overload
    def drop(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        *,
        strict: bool = True,
    ) -> Self: ...
    @overload
    def drop(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        *,
        strict: bool = True,
    ) -> Self: ...
    @overload
    def drop(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        *,
        strict: bool = True,
    ) -> Self: ...
    @overload
    def drop(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        *,
        strict: bool = True,
    ) -> Self: ...
    @overload
    def drop(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        __c9: LiteralString,
        *,
        strict: bool = True,
    ) -> Self: ...
    @overload
    def drop(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        __c9: LiteralString,
        __c10: LiteralString,
        *,
        strict: bool = True,
    ) -> Self: ...
    @overload
    def drop(self, *columns: LiteralString, strict: bool = True) -> Self: ...
    @overload
    def drop(self, *columns: str, strict: bool = True) -> Self: ...
    def drop(self, *columns: Any, strict: bool = True) -> Self: ...
    def drop_prefix(self, prefix: str) -> Self: ...
    def drop_suffix(self, suffix: str) -> Self: ...
    def drop_regex(self, pattern: str) -> Self: ...
    @overload
    def reorder_columns(self, __c1: LiteralString) -> Self: ...
    @overload
    def reorder_columns(self, __c1: LiteralString, __c2: LiteralString) -> Self: ...
    @overload
    def reorder_columns(
        self, __c1: LiteralString, __c2: LiteralString, __c3: LiteralString
    ) -> Self: ...
    @overload
    def reorder_columns(
        self, __c1: LiteralString, __c2: LiteralString, __c3: LiteralString, __c4: LiteralString
    ) -> Self: ...
    @overload
    def reorder_columns(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
    ) -> Self: ...
    @overload
    def reorder_columns(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
    ) -> Self: ...
    @overload
    def reorder_columns(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
    ) -> Self: ...
    @overload
    def reorder_columns(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
    ) -> Self: ...
    @overload
    def reorder_columns(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        __c9: LiteralString,
    ) -> Self: ...
    @overload
    def reorder_columns(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        __c9: LiteralString,
        __c10: LiteralString,
    ) -> Self: ...
    @overload
    def reorder_columns(self, *columns: LiteralString) -> Self: ...
    @overload
    def reorder_columns(self, *columns: str) -> Self: ...
    def reorder_columns(self, *columns: Any) -> Self: ...
    @overload
    def select_first(self, __c1: LiteralString) -> Self: ...
    @overload
    def select_first(self, __c1: LiteralString, __c2: LiteralString) -> Self: ...
    @overload
    def select_first(
        self, __c1: LiteralString, __c2: LiteralString, __c3: LiteralString
    ) -> Self: ...
    @overload
    def select_first(
        self, __c1: LiteralString, __c2: LiteralString, __c3: LiteralString, __c4: LiteralString
    ) -> Self: ...
    @overload
    def select_first(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
    ) -> Self: ...
    @overload
    def select_first(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
    ) -> Self: ...
    @overload
    def select_first(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
    ) -> Self: ...
    @overload
    def select_first(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
    ) -> Self: ...
    @overload
    def select_first(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        __c9: LiteralString,
    ) -> Self: ...
    @overload
    def select_first(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        __c9: LiteralString,
        __c10: LiteralString,
    ) -> Self: ...
    @overload
    def select_first(self, *columns: LiteralString) -> Self: ...
    @overload
    def select_first(self, *columns: str) -> Self: ...
    def select_first(self, *columns: Any) -> Self: ...
    @overload
    def select_last(self, __c1: LiteralString) -> Self: ...
    @overload
    def select_last(self, __c1: LiteralString, __c2: LiteralString) -> Self: ...
    @overload
    def select_last(
        self, __c1: LiteralString, __c2: LiteralString, __c3: LiteralString
    ) -> Self: ...
    @overload
    def select_last(
        self, __c1: LiteralString, __c2: LiteralString, __c3: LiteralString, __c4: LiteralString
    ) -> Self: ...
    @overload
    def select_last(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
    ) -> Self: ...
    @overload
    def select_last(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
    ) -> Self: ...
    @overload
    def select_last(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
    ) -> Self: ...
    @overload
    def select_last(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
    ) -> Self: ...
    @overload
    def select_last(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        __c9: LiteralString,
    ) -> Self: ...
    @overload
    def select_last(
        self,
        __c1: LiteralString,
        __c2: LiteralString,
        __c3: LiteralString,
        __c4: LiteralString,
        __c5: LiteralString,
        __c6: LiteralString,
        __c7: LiteralString,
        __c8: LiteralString,
        __c9: LiteralString,
        __c10: LiteralString,
    ) -> Self: ...
    @overload
    def select_last(self, *columns: LiteralString) -> Self: ...
    @overload
    def select_last(self, *columns: str) -> Self: ...
    def select_last(self, *columns: Any) -> Self: ...
    @overload
    def move(
        self,
        column: LiteralString,
        *,
        before: LiteralString | None = ...,
        after: LiteralString | None = ...,
    ) -> Self: ...
    @overload
    def move(
        self,
        column: str,
        *,
        before: str | None = ...,
        after: str | None = ...,
    ) -> Self: ...
    def move(
        self,
        column: Any,
        *,
        before: Any | None = ...,
        after: Any | None = ...,
    ) -> Self: ...
    def rename(
        self, mapping: Mapping[str, str] | None = ..., *, strict: bool = ..., **named: str
    ) -> Self: ...
    @overload
    def rename_prefix(self, prefix: str, *subset: LiteralString) -> Self: ...
    @overload
    def rename_suffix(self, suffix: str, *subset: LiteralString) -> Self: ...
    @overload
    def rename_replace(self, old: str, new: str, *subset: LiteralString) -> Self: ...
    @overload
    def rename_prefix(self, prefix: str, *subset: str) -> Self: ...
    @overload
    def rename_suffix(self, suffix: str, *subset: str) -> Self: ...
    @overload
    def rename_replace(self, old: str, new: str, *subset: str) -> Self: ...
    def rename_prefix(self, prefix: str, *subset: Any) -> Self: ...
    def rename_suffix(self, suffix: str, *subset: Any) -> Self: ...
    def rename_replace(self, old: str, new: str, *subset: Any) -> Self: ...
    def rename_upper(self, *subset: LiteralString, strict: bool = ...) -> Self: ...
    def rename_lower(self, *subset: LiteralString, strict: bool = ...) -> Self: ...
    def rename_title(self, *subset: LiteralString, strict: bool = ...) -> Self: ...
    def rename_strip(
        self,
        *subset: LiteralString,
        chars: str | None = ...,
        strict: bool = ...,
    ) -> Self: ...
    def with_columns(
        self,
        *expressions: Expr[Any],
        exprs: Mapping[LiteralString, Expr[Any]] | None = ...,
        **named_exprs: Expr[Any],
    ) -> Self: ...
    def with_row_index(self, *, name: str = ..., offset: int = ...) -> Self: ...
    def clip(
        self,
        *,
        lower: Expr[object] | Scalar | None = ...,
        upper: Expr[object] | Scalar | None = ...,
        subset: Sequence[LiteralString] | None = ...,
    ) -> Self: ...
    def cast_many(self, mapping: Mapping[LiteralString, object], *, strict: bool = ...) -> Self: ...
    def cast_subset(self, *columns: LiteralString, dtype: object, strict: bool = ...) -> Self: ...
    @overload
    def cast(self, name: LiteralString, dtype: type[T]) -> Self: ...
    @overload
    def cast(self, name: LiteralString, dtype: object) -> Self: ...
    def filter(self, *predicates: Expr[bool]) -> Self: ...
    def cache(self) -> NoReturn: ...
    def sql(self, query: str, *, table_name: str = ...) -> NoReturn: ...
    def map_batches(self, *_: object, **__: object) -> NoReturn: ...
    def pipe(self, *_: object, **__: object) -> NoReturn: ...
    @overload
    def sort(
        self,
        __k1: LiteralString,
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    @overload
    def sort(
        self,
        __k1: LiteralString,
        __k2: LiteralString,
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    @overload
    def sort(
        self,
        __k1: LiteralString,
        __k2: LiteralString,
        __k3: LiteralString,
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    @overload
    def sort(
        self,
        __k1: LiteralString,
        __k2: LiteralString,
        __k3: LiteralString,
        __k4: LiteralString,
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    @overload
    def sort(
        self,
        __k1: LiteralString,
        __k2: LiteralString,
        __k3: LiteralString,
        __k4: LiteralString,
        __k5: LiteralString,
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    @overload
    def sort(
        self,
        __k1: LiteralString,
        __k2: LiteralString,
        __k3: LiteralString,
        __k4: LiteralString,
        __k5: LiteralString,
        __k6: LiteralString,
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    @overload
    def sort(
        self,
        __k1: LiteralString,
        __k2: LiteralString,
        __k3: LiteralString,
        __k4: LiteralString,
        __k5: LiteralString,
        __k6: LiteralString,
        __k7: LiteralString,
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    @overload
    def sort(
        self,
        __k1: LiteralString,
        __k2: LiteralString,
        __k3: LiteralString,
        __k4: LiteralString,
        __k5: LiteralString,
        __k6: LiteralString,
        __k7: LiteralString,
        __k8: LiteralString,
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    @overload
    def sort(
        self,
        __k1: LiteralString,
        __k2: LiteralString,
        __k3: LiteralString,
        __k4: LiteralString,
        __k5: LiteralString,
        __k6: LiteralString,
        __k7: LiteralString,
        __k8: LiteralString,
        __k9: LiteralString,
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    @overload
    def sort(
        self,
        __k1: LiteralString,
        __k2: LiteralString,
        __k3: LiteralString,
        __k4: LiteralString,
        __k5: LiteralString,
        __k6: LiteralString,
        __k7: LiteralString,
        __k8: LiteralString,
        __k9: LiteralString,
        __k10: LiteralString,
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    @overload
    def sort(
        self,
        *keys: LiteralString,
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    @overload
    def sort(
        self,
        *keys: LiteralString | Expr[Any],
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    def sort(
        self,
        *keys: Any,
        descending: bool | Sequence[bool] = ...,
        nulls_last: bool | Sequence[bool] = ...,
    ) -> Self: ...
    @overload
    def unique(
        self,
        *subset: LiteralString,
        keep: Literal["first", "last"] = ...,
        maintain_order: bool = ...,
    ) -> Self: ...
    @overload
    def unique(
        self,
        *subset: str,
        keep: Literal["first", "last"] = ...,
        maintain_order: bool = ...,
    ) -> Self: ...
    def unique(self, *subset: Any, keep: Any = ..., maintain_order: bool = ...) -> Self: ...
    @overload
    def drop_duplicates(
        self,
        *subset: LiteralString,
        keep: Literal["first", "last"] = ...,
        maintain_order: bool = ...,
    ) -> Self: ...
    @overload
    def drop_duplicates(
        self,
        *subset: str,
        keep: Literal["first", "last"] = ...,
        maintain_order: bool = ...,
    ) -> Self: ...
    def drop_duplicates(
        self, *subset: Any, keep: Literal["first", "last"] = ..., maintain_order: bool = ...
    ) -> Self: ...
    @overload
    def duplicated(
        self,
        *subset: LiteralString,
        keep: Literal["first", "last"] | bool = ...,
        out_name: str = ...,
    ) -> Self: ...
    @overload
    def duplicated(
        self,
        *subset: str,
        keep: Literal["first", "last"] | bool = ...,
        out_name: str = ...,
    ) -> Self: ...
    def duplicated(
        self, *subset: Any, keep: Literal["first", "last"] | bool = ..., out_name: str = ...
    ) -> Self: ...
    @overload
    def group_by(
        self,
        __gk1: LiteralString,
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    @overload
    def group_by(
        self,
        __gk1: LiteralString,
        __gk2: LiteralString,
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    @overload
    def group_by(
        self,
        __gk1: LiteralString,
        __gk2: LiteralString,
        __gk3: LiteralString,
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    @overload
    def group_by(
        self,
        __gk1: LiteralString,
        __gk2: LiteralString,
        __gk3: LiteralString,
        __gk4: LiteralString,
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    @overload
    def group_by(
        self,
        __gk1: LiteralString,
        __gk2: LiteralString,
        __gk3: LiteralString,
        __gk4: LiteralString,
        __gk5: LiteralString,
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    @overload
    def group_by(
        self,
        __gk1: LiteralString,
        __gk2: LiteralString,
        __gk3: LiteralString,
        __gk4: LiteralString,
        __gk5: LiteralString,
        __gk6: LiteralString,
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    @overload
    def group_by(
        self,
        __gk1: LiteralString,
        __gk2: LiteralString,
        __gk3: LiteralString,
        __gk4: LiteralString,
        __gk5: LiteralString,
        __gk6: LiteralString,
        __gk7: LiteralString,
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    @overload
    def group_by(
        self,
        __gk1: LiteralString,
        __gk2: LiteralString,
        __gk3: LiteralString,
        __gk4: LiteralString,
        __gk5: LiteralString,
        __gk6: LiteralString,
        __gk7: LiteralString,
        __gk8: LiteralString,
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    @overload
    def group_by(
        self,
        __gk1: LiteralString,
        __gk2: LiteralString,
        __gk3: LiteralString,
        __gk4: LiteralString,
        __gk5: LiteralString,
        __gk6: LiteralString,
        __gk7: LiteralString,
        __gk8: LiteralString,
        __gk9: LiteralString,
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    @overload
    def group_by(
        self,
        __gk1: LiteralString,
        __gk2: LiteralString,
        __gk3: LiteralString,
        __gk4: LiteralString,
        __gk5: LiteralString,
        __gk6: LiteralString,
        __gk7: LiteralString,
        __gk8: LiteralString,
        __gk9: LiteralString,
        __gk10: LiteralString,
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    @overload
    def group_by(
        self, *keys: LiteralString
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    @overload
    def group_by(
        self, *keys: LiteralString | Expr[Any]
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    def group_by(self, *keys: Any) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    def group_by_dynamic(
        self,
        index_column: LiteralString,
        *,
        every: str,
        period: str | None = ...,
        by: tuple[LiteralString, ...] | None = ...,
    ) -> DynamicGroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...
    def drop_nulls(
        self,
        subset: tuple[LiteralString, ...] | LiteralString | None = ...,
        *,
        how: Literal["any", "all"] = ...,
        threshold: int | None = ...,
    ) -> Self: ...
    @overload
    def fill_null(self, value: Scalar, *subset: LiteralString) -> Self: ...
    @overload
    def fill_null(self, value: Expr[Any], *subset: LiteralString) -> Self: ...
    @overload
    def fill_null(
        self,
        value: None = ...,
        *subset: LiteralString,
        strategy: str,
    ) -> Self: ...
    @overload
    def fill_null(self, value: Scalar, *subset: str) -> Self: ...
    @overload
    def fill_null(self, value: Expr[Any], *subset: str) -> Self: ...
    @overload
    def fill_null(
        self,
        value: None = ...,
        *subset: str,
        strategy: str,
    ) -> Self: ...
    def fill_null_subset(
        self,
        value: Scalar | Expr[Any] | None = ...,
        *columns: LiteralString,
        strategy: str | None = ...,
    ) -> Self: ...
    def fill_null_many(
        self,
        mapping: Mapping[LiteralString, Scalar | Expr[Any]],
        *,
        strict: bool = ...,
    ) -> Self: ...
    def unpivot(
        self,
        *,
        index: tuple[LiteralString, ...] | None = ...,
        on: tuple[LiteralString, ...] | None = ...,
        variable_name: str = ...,
        value_name: str = ...,
    ) -> Self: ...
    def pivot_longer(
        self,
        *,
        id_vars: tuple[LiteralString, ...] | None = ...,
        value_vars: tuple[LiteralString, ...] | None = ...,
        names_to: str = ...,
        values_to: str = ...,
    ) -> Self: ...
    def pivot_wider(
        self,
        *,
        index: tuple[LiteralString, ...],
        names_from: LiteralString,
        values_from: LiteralString | tuple[LiteralString, ...],
        aggregate_function: Literal[
            "first", "last", "sum", "mean", "min", "max", "count", "len", "median"
        ] = ...,
        on_columns: tuple[LiteralString, ...] | None = ...,
        sort_columns: bool = ...,
        separator: str = ...,
    ) -> Self: ...
    def slice(self, offset: int, length: int | None = ...) -> Self: ...
    def limit(self, n: int) -> Self: ...
    def head(self, n: int) -> Self: ...
    def tail(self, n: int) -> Self: ...
    def vstack(self, other: Frame[SchemaT, BackendFrameT, BackendExprT]) -> Self: ...
    def hstack(self, other: Frame[SchemaT, BackendFrameT, BackendExprT]) -> Self: ...
    def union_distinct(self, other: Frame[SchemaT, BackendFrameT, BackendExprT]) -> Self: ...
    @overload
    def explode(
        self,
        __c1: LiteralString,
        *columns: LiteralString,
        outer: bool = ...,
    ) -> Self: ...
    @overload
    def explode(
        self,
        __c1: str,
        *columns: str,
        outer: bool = ...,
    ) -> Self: ...
    def explode(self, *columns: Any, outer: bool = ...) -> Self: ...
    @overload
    def unnest(self, __c1: LiteralString, *columns: LiteralString) -> Self: ...
    @overload
    def unnest(self, __c1: str, *columns: str) -> Self: ...
    def unnest(self, *columns: Any) -> Self: ...
    def posexplode(
        self,
        column: LiteralString,
        *,
        pos: str = ...,
        value: str | None = ...,
        outer: bool = ...,
    ) -> Self: ...
    @overload
    def drop_nulls_all(self, *subset: LiteralString) -> Self: ...
    @overload
    def drop_nulls_all(self, *subset: str) -> Self: ...
    def drop_nulls_all(self, *subset: Any) -> Self: ...
    def pivot(
        self,
        *,
        index: tuple[LiteralString, ...],
        columns: LiteralString | None = ...,
        on: LiteralString | None = ...,
        values: LiteralString | tuple[LiteralString, ...],
        agg: Literal["first", "last", "sum", "mean", "min", "max", "count", "len", "median"] = ...,
        on_columns: tuple[str, ...] | None = ...,
        separator: str = ...,
        sort_columns: bool = ...,
    ) -> Self: ...
    def sample(
        self,
        n: int | None = ...,
        *,
        frac: float | None = ...,
        with_replacement: bool = ...,
        shuffle: bool = ...,
        seed: int | None = ...,
    ) -> Self: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        how: Literal["cross"],
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        on: tuple[LiteralString],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        on: tuple[LiteralString, LiteralString],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        on: tuple[LiteralString, LiteralString, LiteralString],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        on: tuple[LiteralString, LiteralString, LiteralString, LiteralString],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        on: tuple[LiteralString, LiteralString, LiteralString, LiteralString, LiteralString],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        on: tuple[
            LiteralString, LiteralString, LiteralString, LiteralString, LiteralString, LiteralString
        ],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        on: tuple[
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
        ],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        on: tuple[
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
        ],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        on: tuple[
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
        ],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        on: tuple[
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
        ],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        left_on: tuple[LiteralString],
        right_on: tuple[LiteralString],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        left_on: tuple[LiteralString, LiteralString],
        right_on: tuple[LiteralString, LiteralString],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        left_on: tuple[LiteralString, LiteralString, LiteralString],
        right_on: tuple[LiteralString, LiteralString, LiteralString],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        left_on: tuple[LiteralString, LiteralString, LiteralString, LiteralString],
        right_on: tuple[LiteralString, LiteralString, LiteralString, LiteralString],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        left_on: tuple[LiteralString, LiteralString, LiteralString, LiteralString, LiteralString],
        right_on: tuple[LiteralString, LiteralString, LiteralString, LiteralString, LiteralString],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        left_on: tuple[
            LiteralString, LiteralString, LiteralString, LiteralString, LiteralString, LiteralString
        ],
        right_on: tuple[
            LiteralString, LiteralString, LiteralString, LiteralString, LiteralString, LiteralString
        ],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        left_on: tuple[
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
        ],
        right_on: tuple[
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
        ],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        left_on: tuple[
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
        ],
        right_on: tuple[
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
        ],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        left_on: tuple[
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
        ],
        right_on: tuple[
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
        ],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        left_on: tuple[
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
        ],
        right_on: tuple[
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
            LiteralString,
        ],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        on: tuple[LiteralString | Expr[Any], ...],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    @overload
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        left_on: tuple[LiteralString | Expr[Any], ...],
        right_on: tuple[LiteralString | Expr[Any], ...],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...
    def join(
        self,
        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],
        *,
        on: tuple[Any, ...] | None = ...,
        left_on: tuple[Any, ...] | None = ...,
        right_on: tuple[Any, ...] | None = ...,
        how: Any = ...,
        suffix: str = ...,
        options: JoinOptions | None = ...,
    ) -> Frame[Any, BackendFrameT, BackendExprT]: ...
    @overload
    def collect(self, *, options: ExecutionOptions | None = ...) -> BackendFrameT: ...
    @overload
    def collect(
        self,
        *,
        kind: Literal["dataclass", "pydantic"],
        name: str = ...,
        options: ExecutionOptions | None = ...,
    ) -> list[Any]: ...
    @overload
    async def acollect(self, *, options: ExecutionOptions | None = ...) -> BackendFrameT: ...
    @overload
    async def acollect(
        self,
        *,
        kind: Literal["dataclass", "pydantic"],
        name: str = ...,
        options: ExecutionOptions | None = ...,
    ) -> list[Any]: ...
    def to_dicts(self, *, options: ExecutionOptions | None = ...) -> list[dict[str, object]]: ...
    def to_dict(self, *, options: ExecutionOptions | None = ...) -> dict[str, list[object]]: ...
    async def ato_dicts(
        self, *, options: ExecutionOptions | None = ...
    ) -> list[dict[str, object]]: ...
    async def ato_dict(
        self, *, options: ExecutionOptions | None = ...
    ) -> dict[str, list[object]]: ...
    def write_parquet(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4"] = ...,
        row_group_size: int | None = ...,
        partition_by: tuple[LiteralString, ...] | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> None: ...
    def write_csv(
        self,
        path: str,
        *,
        separator: str = ...,
        include_header: bool = ...,
        storage_options: StorageOptions | None = ...,
    ) -> None: ...
    def write_ndjson(self, path: str, *, storage_options: StorageOptions | None = ...) -> None: ...
    def write_ipc(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "lz4", "zstd"] = ...,
        storage_options: StorageOptions | None = ...,
    ) -> None: ...
    def write_database(
        self,
        table_name: str,
        *,
        connection: object,
        if_table_exists: Literal["fail", "replace", "append"] = ...,
        engine: str | None = ...,
    ) -> None: ...
    def write_excel(self, path: str, *, worksheet: str = ...) -> None: ...
    def write_delta(
        self,
        target: str,
        *,
        mode: Literal["error", "append", "overwrite", "ignore", "merge"] = ...,
        storage_options: StorageOptions | None = ...,
    ) -> None: ...
    def write_avro(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "deflate"] = ...,
        name: str = ...,
    ) -> None: ...
    def sink_parquet(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4"] = ...,
        row_group_size: int | None = ...,
        partition_by: tuple[LiteralString, ...] | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> None: ...
    def sink_csv(
        self,
        path: str,
        *,
        separator: str = ...,
        include_header: bool = ...,
        storage_options: StorageOptions | None = ...,
    ) -> None: ...
    def sink_ndjson(self, path: str, *, storage_options: StorageOptions | None = ...) -> None: ...
    def sink_ipc(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "lz4", "zstd"] = ...,
        storage_options: StorageOptions | None = ...,
    ) -> None: ...
    def sink_database(
        self,
        table_name: str,
        *,
        connection: object,
        if_table_exists: Literal["fail", "replace", "append"] = ...,
        engine: str | None = ...,
    ) -> None: ...
    def sink_excel(self, path: str, *, worksheet: str = ...) -> None: ...
    def sink_delta(
        self,
        target: str,
        *,
        mode: Literal["error", "append", "overwrite", "ignore", "merge"] = ...,
        storage_options: StorageOptions | None = ...,
    ) -> None: ...
    def sink_avro(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "deflate"] = ...,
        name: str = ...,
    ) -> None: ...
    def materialize_model(
        self,
        name: str,
        *,
        kind: Literal["dataclass", "pydantic"] = ...,
    ) -> type[Any]: ...
    def rolling_agg(
        self,
        *,
        on: LiteralString,
        column: LiteralString,
        window_size: int | str,
        op: str,
        out_name: str,
        by: tuple[LiteralString, ...] | None = ...,
        min_periods: int = ...,
    ) -> Self: ...
