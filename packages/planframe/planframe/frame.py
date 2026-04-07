from __future__ import annotations

import re
from typing import Any, Generic, Literal, TypeVar

from planframe.backend.adapter import BackendAdapter
from planframe.backend.errors import (
    PlanFrameBackendError,
    PlanFrameExecutionError,
    PlanFrameSchemaError,
)
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
    Explode,
    Unnest,
    ConcatHorizontal,
    DropNullsAll,
    Sample,
)
from planframe.schema.ir import Field, Schema
from planframe.schema.materialize import materialize_model
from planframe.schema.source import schema_from_type
from planframe.groupby import GroupedFrame

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


class Frame(Generic[SchemaT, BackendFrameT, BackendExprT]):
    __slots__ = ("_data", "_adapter", "_plan", "_schema")

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
    ) -> None:
        self._data = _data
        self._adapter = _adapter
        self._plan = _plan
        self._schema = _schema

    @classmethod
    def source(
        cls,
        data: BackendFrameT,
        *,
        adapter: BackendAdapter[BackendFrameT, BackendExprT],
        schema: type[SchemaT],
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        schema_ir = schema_from_type(schema)
        return cls(
            _data=data, _adapter=adapter, _plan=Source(schema_type=schema), _schema=schema_ir
        )

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
            return self._adapter.drop(
                self._eval(node.prev), node.columns, strict=node.strict
            )
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
            return self._adapter.sort(
                self._eval(node.prev),
                node.columns,
                descending=node.descending,
                nulls_last=node.nulls_last,
            )
        if isinstance(node, Unique):
            return self._adapter.unique(
                self._eval(node.prev),
                node.subset,
                keep=node.keep,
                maintain_order=node.maintain_order,
            )
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
            right_frame = node.right
            if getattr(right_frame, "_adapter", None) is None:
                raise PlanFrameBackendError("Join node right frame is invalid")
            if right_frame._adapter.name != self._adapter.name:
                raise PlanFrameBackendError("Cannot join frames from different backends")
            right_df = right_frame._eval(right_frame._plan)
            return self._adapter.join(
                left_df, right_df, on=node.on, how=node.how, suffix=node.suffix
            )
        if isinstance(node, Slice):
            return self._adapter.slice(
                self._eval(node.prev), offset=node.offset, length=node.length
            )
        if isinstance(node, Head):
            return self._adapter.head(self._eval(node.prev), node.n)
        if isinstance(node, Tail):
            return self._adapter.tail(self._eval(node.prev), node.n)
        if isinstance(node, ConcatVertical):
            left_df = self._eval(node.prev)
            other_frame = node.other
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
        if isinstance(node, Explode):
            return self._adapter.explode(self._eval(node.prev), node.column)
        if isinstance(node, Unnest):
            return self._adapter.unnest(self._eval(node.prev), node.column)
        if isinstance(node, ConcatHorizontal):
            left_df = self._eval(node.prev)
            other_frame = node.other
            if getattr(other_frame, "_adapter", None) is None:
                raise PlanFrameBackendError("ConcatHorizontal node other frame is invalid")
            if other_frame._adapter.name != self._adapter.name:
                raise PlanFrameBackendError("Cannot concat frames from different backends")
            right_df = other_frame._eval(other_frame._plan)
            return self._adapter.concat_horizontal(left_df, right_df)
        if isinstance(node, DropNullsAll):
            return self._adapter.drop_nulls_all(self._eval(node.prev), node.subset)
        if isinstance(node, Sample):
            return self._adapter.sample(
                self._eval(node.prev),
                n=node.n,
                frac=node.frac,
                with_replacement=node.with_replacement,
                shuffle=node.shuffle,
                seed=node.seed,
            )

        raise PlanFrameBackendError(f"Unsupported plan node: {type(node)!r}")

    def select(self, *columns: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(columns)
        schema2 = self._schema.select(cols)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, cols),
            _schema=schema2,
        )

    def select_prefix(self, prefix: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(n for n in self._schema.names() if n.startswith(prefix))
        schema2 = self._schema.select(cols)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, cols),
            _schema=schema2,
        )

    def select_suffix(self, suffix: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(n for n in self._schema.names() if n.endswith(suffix))
        schema2 = self._schema.select(cols)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, cols),
            _schema=schema2,
        )

    def select_regex(self, pattern: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        rx = re.compile(pattern)
        cols = tuple(n for n in self._schema.names() if rx.search(n))
        schema2 = self._schema.select(cols)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, cols),
            _schema=schema2,
        )

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

    def drop(
        self, *columns: str, strict: bool = True
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(columns)
        schema2 = self._schema.drop(cols, strict=strict)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Drop(self._plan, cols, strict=strict),
            _schema=schema2,
        )

    def drop_prefix(self, prefix: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(n for n in self._schema.names() if n.startswith(prefix))
        if not cols:
            return self
        schema2 = self._schema.drop(cols)
        return Frame(
            _data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2
        )

    def drop_suffix(self, suffix: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(n for n in self._schema.names() if n.endswith(suffix))
        if not cols:
            return self
        schema2 = self._schema.drop(cols)
        return Frame(
            _data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2
        )

    def drop_regex(self, pattern: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        rx = re.compile(pattern)
        cols = tuple(n for n in self._schema.names() if rx.search(n))
        if not cols:
            return self
        schema2 = self._schema.drop(cols)
        return Frame(
            _data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2
        )

    def rename(self, **mapping: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        if not mapping:
            return self
        schema2 = self._schema.rename(mapping)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping),
            _schema=schema2,
        )

    def rename_prefix(
        self, prefix: str, *subset: str
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        names = subset if subset else self._schema.names()
        mapping = {n: f"{prefix}{n}" for n in names}
        return self.rename(**mapping)

    def rename_suffix(
        self, suffix: str, *subset: str
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        names = subset if subset else self._schema.names()
        mapping = {n: f"{n}{suffix}" for n in names}
        return self.rename(**mapping)

    def rename_replace(
        self, old: str, new: str, *subset: str
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        names = subset if subset else self._schema.names()
        mapping = {n: n.replace(old, new) for n in names}
        return self.rename(**mapping)

    def with_column(
        self, name: str, expr: Expr[Any]
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
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

    def sort(
        self, *columns: str, descending: bool = False, nulls_last: bool = False
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        cols = tuple(columns)
        self._schema.select(cols)  # validate existence only
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Sort(self._plan, columns=cols, descending=descending, nulls_last=nulls_last),
            _schema=self._schema,
        )

    def unique(
        self,
        *subset: str,
        keep: Literal["first", "last"] = "first",
        maintain_order: bool = False,
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        sub = tuple(subset) if subset else None
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.unique()
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Unique(self._plan, subset=sub, keep=keep, maintain_order=maintain_order),
            _schema=schema2,
        )

    def drop_duplicates(
        self,
        *subset: str,
        keep: Literal["first", "last"] = "first",
        maintain_order: bool = False,
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        return self.unique(*subset, keep=keep, maintain_order=maintain_order)

    def duplicated(
        self,
        *subset: str,
        keep: Literal["first", "last"] | bool = "first",
        out_name: str = "duplicated",
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

    def sample(
        self,
        n: int | None = None,
        *,
        frac: float | None = None,
        with_replacement: bool = False,
        shuffle: bool = False,
        seed: int | None = None,
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        if (n is None) == (frac is None):
            raise ValueError("sample requires exactly one of n= or frac=")
        if n is not None and n < 0:
            raise ValueError("sample n must be non-negative")
        if frac is not None and frac < 0:
            raise ValueError("sample frac must be non-negative")
        if frac is not None and frac > 1.0 and not with_replacement:
            raise ValueError("sample frac > 1.0 requires with_replacement=True")
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Sample(
                self._plan,
                n=n,
                frac=frac,
                with_replacement=with_replacement,
                shuffle=shuffle,
                seed=seed,
            ),
            _schema=self._schema,
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

    def drop_nulls_all(self, *subset: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        sub = tuple(subset) if subset else None
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.drop_nulls_all()
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=DropNullsAll(self._plan, subset=sub),
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
            id_vars=id_vars,
            value_vars=value_vars,
            variable_name=variable_name,
            value_name=value_name,
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

    def slice(
        self, offset: int, length: int | None = None
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
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
            raise PlanFrameSchemaError(
                "concat_vertical requires identical column names and ordering"
            )
        for name in self._schema.names():
            if self._schema.get(name).dtype != other._schema.get(name).dtype:
                raise PlanFrameSchemaError(f"concat_vertical dtype mismatch for column: {name}")
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=ConcatVertical(self._plan, other=other),
            _schema=self._schema,
        )

    def concat_horizontal(
        self, other: "Frame[SchemaT, BackendFrameT, BackendExprT]"
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        if other._adapter.name != self._adapter.name:
            raise PlanFrameBackendError("Cannot concat frames from different backends")
        left_names = set(self._schema.names())
        right_names = set(other._schema.names())
        overlap = left_names.intersection(right_names)
        if overlap:
            raise PlanFrameSchemaError(
                f"concat_horizontal has overlapping columns: {sorted(overlap)}"
            )
        schema2 = Schema(fields=tuple([*self._schema.fields, *other._schema.fields]))
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=ConcatHorizontal(self._plan, other=other),
            _schema=schema2,
        )

    def union_distinct(
        self, other: "Frame[SchemaT, BackendFrameT, BackendExprT]"
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        return self.concat_vertical(other).unique()

    def explode(self, column: str) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        schema2 = self._schema.explode(column)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Explode(self._plan, column=column),
            _schema=schema2,
        )

    def unnest(
        self, column: str, *, fields: tuple[str, ...]
    ) -> "Frame[SchemaT, BackendFrameT, BackendExprT]":
        schema2 = self._schema.unnest(column, fields=fields)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Unnest(self._plan, column=column),
            _schema=schema2,
        )

    def pivot(
        self,
        *,
        index: tuple[str, ...],
        on: str,
        values: str,
        agg: Literal[
            "first", "last", "sum", "mean", "min", "max", "count", "len", "median"
        ] = "first",
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

    def collect(
        self,
        *,
        kind: Literal["dataclass", "pydantic"] | None = None,
        name: str = "Row",
    ) -> BackendFrameT | list[Any]:
        try:
            planned = self._eval(self._plan)
            out = self._adapter.collect(planned)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(f"Backend collect failed for {self._adapter.name}") from e

        if kind is None:
            return out

        # Build row models from the derived schema.
        Model = materialize_model(name=name, schema=self._schema, kind=kind)
        try:
            rows = self._adapter.to_dicts(out)
            return [Model(**r) for r in rows]
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend collect(kind={kind!r}) failed for {self._adapter.name}"
            ) from e

    def to_dicts(self) -> list[dict[str, object]]:
        try:
            planned = self._eval(self._plan)
            return self._adapter.to_dicts(planned)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend to_dicts failed for {self._adapter.name}"
            ) from e

    def to_dict(self) -> dict[str, list[object]]:
        try:
            planned = self._eval(self._plan)
            return self._adapter.to_dict(planned)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(f"Backend to_dict failed for {self._adapter.name}") from e

    def write_parquet(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4"] = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        try:
            planned = self._eval(self._plan)
            self._adapter.write_parquet(
                planned,
                path,
                compression=compression,
                row_group_size=row_group_size,
                partition_by=partition_by,
                storage_options=storage_options,
            )
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend write_parquet failed for {self._adapter.name}"
            ) from e

    def write_csv(
        self,
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        try:
            planned = self._eval(self._plan)
            self._adapter.write_csv(
                planned,
                path,
                separator=separator,
                include_header=include_header,
                storage_options=storage_options,
            )
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend write_csv failed for {self._adapter.name}"
            ) from e

    def write_ndjson(self, path: str, *, storage_options: dict[str, Any] | None = None) -> None:
        try:
            planned = self._eval(self._plan)
            self._adapter.write_ndjson(planned, path, storage_options=storage_options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend write_ndjson failed for {self._adapter.name}"
            ) from e

    def write_ipc(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "lz4", "zstd"] = "uncompressed",
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        try:
            planned = self._eval(self._plan)
            self._adapter.write_ipc(
                planned, path, compression=compression, storage_options=storage_options
            )
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend write_ipc failed for {self._adapter.name}"
            ) from e

    def write_database(
        self,
        table_name: str,
        *,
        connection: Any,
        if_table_exists: Literal["fail", "replace", "append"] = "fail",
        engine: str | None = None,
    ) -> None:
        try:
            planned = self._eval(self._plan)
            self._adapter.write_database(
                planned,
                table_name=table_name,
                connection=connection,
                if_table_exists=if_table_exists,
                engine=engine,
            )
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend write_database failed for {self._adapter.name}"
            ) from e

    def write_excel(self, path: str, *, worksheet: str = "Sheet1") -> None:
        try:
            planned = self._eval(self._plan)
            self._adapter.write_excel(planned, path, worksheet=worksheet)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend write_excel failed for {self._adapter.name}"
            ) from e

    def write_delta(
        self,
        target: str,
        *,
        mode: Literal["error", "append", "overwrite", "ignore", "merge"] = "error",
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        try:
            planned = self._eval(self._plan)
            self._adapter.write_delta(planned, target, mode=mode, storage_options=storage_options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend write_delta failed for {self._adapter.name}"
            ) from e

    def write_avro(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "deflate"] = "uncompressed",
        name: str = "",
    ) -> None:
        try:
            planned = self._eval(self._plan)
            self._adapter.write_avro(planned, path, compression=compression, name=name)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend write_avro failed for {self._adapter.name}"
            ) from e

    def materialize_model(
        self,
        name: str,
        *,
        kind: Literal["dataclass", "pydantic"] = "dataclass",
    ) -> type[Any]:
        return materialize_model(name=name, schema=self._schema, kind=kind)
