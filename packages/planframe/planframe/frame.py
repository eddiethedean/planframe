from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Generic, Literal, TypeVar, cast

from planframe.backend.adapter import BackendAdapter
from planframe.backend.errors import PlanFrameBackendError, PlanFrameExecutionError, PlanFrameSchemaError
from planframe.expr.api import Expr, infer_dtype
from planframe.plan.nodes import (
    Agg,
    Cast,
    Drop,
    Duplicated,
    Filter,
    GroupBy,
    PlanNode,
    Rename,
    Select,
    Sort,
    Source,
    Unique,
    WithColumn,
    DropNulls,
    FillNull,
    Melt,
    Join,
    Slice,
    Head,
    Tail,
    ConcatVertical,
    Pivot,
)
from planframe.schema.ir import Field, Schema
from planframe.schema.materialize import materialize_model
from planframe.schema.source import schema_from_type
from planframe.groupby import GroupedFrame

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


@dataclass(frozen=True, slots=True)
class Frame(Generic[SchemaT, BackendFrameT, BackendExprT]):
    _data: BackendFrameT
    _adapter: BackendAdapter[BackendFrameT, BackendExprT]
    _plan: PlanNode
    _schema: Schema

    @classmethod
    def source(
        cls,
        data: BackendFrameT,
        *,
        adapter: BackendAdapter[BackendFrameT, BackendExprT],
        schema: type[SchemaT],
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        schema_ir = schema_from_type(schema)
        return cls(_data=data, _adapter=adapter, _plan=Source(schema_type=schema), _schema=schema_ir)

    def schema(self) -> Schema:
        return self._schema

    def plan(self) -> PlanNode:
        return self._plan

    def _compile(self, expr: Any) -> BackendExprT:
        try:
            return self._adapter.compile_expr(expr)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameBackendError(
                f"Failed to compile expression for backend {self._adapter.name}"
            ) from e

    def _eval(self, node: PlanNode) -> BackendFrameT:
        if isinstance(node, Source):
            return self._data
        if isinstance(node, Select):
            return self._adapter.select(self._eval(node.prev), node.columns)
        if isinstance(node, Drop):
            return self._adapter.drop(self._eval(node.prev), node.columns)
        if isinstance(node, Rename):
            return self._adapter.rename(self._eval(node.prev), node.mapping)
        if isinstance(node, WithColumn):
            prev = self._eval(node.prev)
            bexpr = self._compile(node.expr)
            return self._adapter.with_column(prev, node.name, bexpr)
        if isinstance(node, Cast):
            return self._adapter.cast(self._eval(node.prev), node.name, node.dtype)
        if isinstance(node, Filter):
            prev = self._eval(node.prev)
            bexpr = self._compile(node.predicate)
            return self._adapter.filter(prev, bexpr)
        if isinstance(node, Sort):
            return self._adapter.sort(self._eval(node.prev), node.columns, descending=node.descending)
        if isinstance(node, Unique):
            return self._adapter.unique(self._eval(node.prev), node.subset, keep=node.keep)
        if isinstance(node, Duplicated):
            return self._adapter.duplicated(
                self._eval(node.prev),
                node.subset,
                keep=node.keep,
                out_name=node.out_name,
            )
        if isinstance(node, GroupBy):
            # GroupBy is only meaningful when immediately followed by Agg.
            return self._eval(node.prev)
        if isinstance(node, Agg):
            # prev is a GroupBy node
            if not isinstance(node.prev, GroupBy):
                raise PlanFrameBackendError("Agg must follow GroupBy")
            return self._adapter.group_by_agg(
                self._eval(node.prev.prev),
                keys=node.prev.keys,
                named_aggs=node.named_aggs,
            )
        if isinstance(node, DropNulls):
            return self._adapter.drop_nulls(self._eval(node.prev), node.subset)
        if isinstance(node, FillNull):
            return self._adapter.fill_null(self._eval(node.prev), node.value, node.subset)
        if isinstance(node, Melt):
            return self._adapter.melt(
                self._eval(node.prev),
                id_vars=node.id_vars,
                value_vars=node.value_vars,
                variable_name=node.variable_name,
                value_name=node.value_name,
            )
        if isinstance(node, Join):
            left_df = self._eval(node.prev)
            right_frame = cast(Any, node.right)
            if getattr(right_frame, "_adapter", None) is None:
                raise PlanFrameBackendError("Join node right frame is invalid")
            if right_frame._adapter.name != self._adapter.name:
                raise PlanFrameBackendError("Cannot join frames from different backends")
            right_df = right_frame._eval(right_frame._plan)
            return self._adapter.join(left_df, right_df, on=node.on, how=node.how, suffix=node.suffix)
        if isinstance(node, Slice):
            return self._adapter.slice(self._eval(node.prev), offset=node.offset, length=node.length)
        if isinstance(node, Head):
            return self._adapter.head(self._eval(node.prev), node.n)
        if isinstance(node, Tail):
            return self._adapter.tail(self._eval(node.prev), node.n)
        if isinstance(node, ConcatVertical):
            left_df = self._eval(node.prev)
            other_frame = cast(Any, node.other)
            if getattr(other_frame, "_adapter", None) is None:
                raise PlanFrameBackendError("ConcatVertical node other frame is invalid")
            if other_frame._adapter.name != self._adapter.name:
                raise PlanFrameBackendError("Cannot concat frames from different backends")
            right_df = other_frame._eval(other_frame._plan)
            return self._adapter.concat_vertical(left_df, right_df)
        if isinstance(node, Pivot):
            return self._adapter.pivot(
                self._eval(node.prev),
                index=node.index,
                on=node.on,
                values=node.values,
                agg=node.agg,
                on_columns=node.on_columns,
                separator=node.separator,
            )

        raise PlanFrameBackendError(f"Unsupported plan node: {type(node)!r}")

    def select(self, *columns: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(columns)
        schema2 = self._schema.select(cols)
        return Frame(_data=self._data, _adapter=self._adapter, _plan=Select(self._plan, cols), _schema=schema2)

    def select_prefix(self, prefix: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(n for n in self._schema.names() if n.startswith(prefix))
        schema2 = self._schema.select(cols)
        return Frame(_data=self._data, _adapter=self._adapter, _plan=Select(self._plan, cols), _schema=schema2)

    def select_suffix(self, suffix: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(n for n in self._schema.names() if n.endswith(suffix))
        schema2 = self._schema.select(cols)
        return Frame(_data=self._data, _adapter=self._adapter, _plan=Select(self._plan, cols), _schema=schema2)

    def select_regex(self, pattern: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        rx = re.compile(pattern)
        cols = tuple(n for n in self._schema.names() if rx.search(n))
        schema2 = self._schema.select(cols)
        return Frame(_data=self._data, _adapter=self._adapter, _plan=Select(self._plan, cols), _schema=schema2)

    def select_exclude(self, *columns: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(columns)
        schema2 = self._schema.select_exclude(cols)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def reorder_columns(self, *columns: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        schema2 = self._schema.reorder_columns(columns)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def select_first(self, *columns: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        schema2 = self._schema.select_first(columns)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def select_last(self, *columns: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        schema2 = self._schema.select_last(columns)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def move(
        self, column: str, *, before: str | None = None, after: str | None = None
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        schema2 = self._schema.move(column, before=before, after=after)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def drop(self, *columns: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(columns)
        schema2 = self._schema.drop(cols)
        return Frame(_data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2)

    def drop_prefix(self, prefix: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(n for n in self._schema.names() if n.startswith(prefix))
        if not cols:
            return self
        schema2 = self._schema.drop(cols)
        return Frame(_data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2)

    def drop_suffix(self, suffix: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(n for n in self._schema.names() if n.endswith(suffix))
        if not cols:
            return self
        schema2 = self._schema.drop(cols)
        return Frame(_data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2)

    def drop_regex(self, pattern: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        rx = re.compile(pattern)
        cols = tuple(n for n in self._schema.names() if rx.search(n))
        if not cols:
            return self
        schema2 = self._schema.drop(cols)
        return Frame(_data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2)

    def rename(self, **mapping: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        if not mapping:
            return self
        schema2 = self._schema.rename(mapping)
        return Frame(_data=self._data, _adapter=self._adapter, _plan=Rename(self._plan, mapping), _schema=schema2)

    def rename_prefix(self, prefix: str, *subset: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        names = subset if subset else self._schema.names()
        mapping = {n: f"{prefix}{n}" for n in names}
        return self.rename(**mapping)

    def rename_suffix(self, suffix: str, *subset: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        names = subset if subset else self._schema.names()
        mapping = {n: f"{n}{suffix}" for n in names}
        return self.rename(**mapping)

    def rename_replace(self, old: str, new: str, *subset: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        names = subset if subset else self._schema.names()
        mapping = {n: n.replace(old, new) for n in names}
        return self.rename(**mapping)

    def with_column(self, name: str, expr: Expr[Any]) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        dtype = infer_dtype(expr)
        schema2 = self._schema.with_column(name, dtype=dtype)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=WithColumn(self._plan, name=name, expr=expr),
            _schema=schema2,
        )

    def cast(self, name: str, dtype: Any) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        schema2 = self._schema.cast(name, dtype=dtype)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Cast(self._plan, name=name, dtype=dtype),
            _schema=schema2,
        )

    def filter(self, predicate: Expr[bool]) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Filter(self._plan, predicate=predicate),
            _schema=self._schema,
        )

    def sort(self, *columns: str, descending: bool = False) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(columns)
        self._schema.select(cols)  # validate existence only
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Sort(self._plan, columns=cols, descending=descending),
            _schema=self._schema,
        )

    def unique(
        self, *subset: str, keep: Literal["first", "last"] = "first"
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        sub = tuple(subset) if subset else None
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.unique()
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Unique(self._plan, subset=sub, keep=keep),
            _schema=schema2,
        )

    def duplicated(
        self, *subset: str, keep: Literal["first", "last"] | bool = "first", out_name: str = "duplicated"
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        sub = tuple(subset) if subset else None
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.duplicated(out_name=out_name)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Duplicated(self._plan, subset=sub, keep=keep, out_name=out_name),
            _schema=schema2,
        )

    def group_by(self, *keys: str) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]:
        if not keys:
            raise PlanFrameBackendError("group_by requires at least one key")
        self._schema.select(keys)  # validate
        return GroupedFrame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=self._plan,
            _schema=self._schema,
            _keys=tuple(keys),
        )

    def drop_nulls(self, *subset: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        sub = tuple(subset) if subset else None
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.drop_nulls()
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=DropNulls(self._plan, subset=sub),
            _schema=schema2,
        )

    def fill_null(self, value: Any, *subset: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        sub = tuple(subset) if subset else None
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.fill_null()
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=FillNull(self._plan, value=value, subset=sub),
            _schema=schema2,
        )

    def melt(
        self,
        *,
        id_vars: tuple[str, ...],
        value_vars: tuple[str, ...],
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        schema2 = self._schema.melt(
            id_vars=id_vars, value_vars=value_vars, variable_name=variable_name, value_name=value_name
        )
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Melt(
                self._plan,
                id_vars=id_vars,
                value_vars=value_vars,
                variable_name=variable_name,
                value_name=value_name,
            ),
            _schema=schema2,
        )

    def join(
        self,
        other: "Frame[Any, BackendFrameT, BackendExprT]",
        *,
        on: tuple[str, ...],
        how: Literal["inner", "left", "right", "full", "semi", "anti", "cross"] = "inner",
        suffix: str = "_right",
    ) -> "Frame[Any, BackendFrameT, BackendExprT]":
        schema2 = self._schema.join_merge(other._schema, on=on, suffix=suffix)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Join(self._plan, right=other, on=on, how=how, suffix=suffix),
            _schema=schema2,
        )

    def slice(self, offset: int, length: int | None = None) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        if length is not None and length < 0:
            raise ValueError("length must be non-negative or None")
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Slice(self._plan, offset=offset, length=length),
            _schema=self._schema,
        )

    def limit(self, n: int) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        return self.head(n)

    def head(self, n: int) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        if n < 0:
            raise ValueError("n must be non-negative")
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Head(self._plan, n=n),
            _schema=self._schema,
        )

    def tail(self, n: int) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        if n < 0:
            raise ValueError("n must be non-negative")
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Tail(self._plan, n=n),
            _schema=self._schema,
        )

    def concat_vertical(
        self, other: "Frame[SchemaT, BackendFrameT, BackendExprT]"
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        if other._adapter.name != self._adapter.name:
            raise PlanFrameBackendError("Cannot concat frames from different backends")
        if self._schema.names() != other._schema.names():
            raise PlanFrameSchemaError("concat_vertical requires identical column names and ordering")
        for name in self._schema.names():
            if self._schema.get(name).dtype != other._schema.get(name).dtype:
                raise PlanFrameSchemaError(f"concat_vertical dtype mismatch for column: {name}")
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=ConcatVertical(self._plan, other=other),
            _schema=self._schema,
        )

    def pivot(
        self,
        *,
        index: tuple[str, ...],
        on: str,
        values: str,
        agg: Literal["first", "last", "sum", "mean", "min", "max", "count", "len", "median"] = "first",
        on_columns: tuple[str, ...] | None = None,
        separator: str = "_",
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        if not index:
            raise PlanFrameSchemaError("pivot requires non-empty index")
        self._schema.select(index)  # validate
        self._schema.get(on)
        self._schema.get(values)

        out_fields = [self._schema.get(c) for c in index]
        if on_columns is not None:
            for c in on_columns:
                out_fields.append(Field(name=str(c), dtype=object))
        schema2 = Schema(fields=tuple(out_fields))
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Pivot(
                self._plan,
                index=index,
                on=on,
                values=values,
                agg=agg,
                on_columns=on_columns,
                separator=separator,
            ),
            _schema=schema2,
        )

    def collect(self) -> BackendFrameT:
        try:
            planned = self._eval(self._plan)
            return self._adapter.collect(planned)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(f"Backend collect failed for {self._adapter.name}") from e

    def materialize_model(
        self,
        name: str,
        *,
        kind: Literal["dataclass", "pydantic"] = "dataclass",
    ) -> type[Any]:
        return materialize_model(name=name, schema=self._schema, kind=kind)
 
