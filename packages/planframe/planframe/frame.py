from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any, Generic, Literal, TypeVar, cast

from planframe.backend.adapter import (
    BackendAdapter,
    CompiledJoinKey,
    CompiledProjectItem,
    CompiledSortKey,
)
from planframe.backend.errors import (
    PlanFrameBackendError,
    PlanFrameExecutionError,
    PlanFrameSchemaError,
)
from planframe.expr.api import Expr, infer_dtype
from planframe.groupby import GroupedFrame
from planframe.plan.join_options import JoinOptions
from planframe.plan.nodes import (
    Agg,
    Cast,
    ConcatHorizontal,
    ConcatVertical,
    Drop,
    DropNulls,
    DropNullsAll,
    Duplicated,
    Explode,
    FillNull,
    Filter,
    GroupBy,
    Head,
    Join,
    JoinKeyColumn,
    JoinKeyExpr,
    Melt,
    Pivot,
    PlanNode,
    Project,
    ProjectExpr,
    ProjectPick,
    Rename,
    Sample,
    Select,
    Slice,
    Sort,
    SortColumnKey,
    SortExprKey,
    Source,
    Tail,
    Unique,
    Unnest,
    WithColumn,
)
from planframe.schema.ir import Field, Schema, collect_col_names_in_expr
from planframe.schema.materialize import materialize_model
from planframe.schema.source import schema_from_type
from planframe.typing.scalars import Scalar
from planframe.typing.storage import StorageOptions

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


def _coerce_sort_flags(name: str, n: int, value: bool | Sequence[bool]) -> tuple[bool, ...]:
    if isinstance(value, bool):
        return (value,) * n
    seq = tuple(value)
    if len(seq) != n:
        raise ValueError(
            f"sort {name} must be a bool or a sequence of length {n} "
            f"(number of sort keys), got length {len(seq)}"
        )
    for i, x in enumerate(seq):
        if not isinstance(x, bool):
            raise TypeError(
                f"sort {name} must contain only bool values, got {type(x).__name__!r} at index {i}"
            )
    return seq


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
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        schema_ir = schema_from_type(schema)
        return cls(
            _data=data, _adapter=adapter, _plan=Source(schema_type=schema), _schema=schema_ir
        )

    def schema(self) -> Schema:
        return self._schema

    def plan(self) -> PlanNode:
        return self._plan

    def _compile(self, expr: object) -> BackendExprT:
        try:
            return self._adapter.compile_expr(expr, schema=self._schema)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameBackendError(
                f"Failed to compile expression for backend {self._adapter.name}"
            ) from e

    def _compile_join_keys_tuple(
        self, keys: tuple[JoinKeyColumn | JoinKeyExpr, ...]
    ) -> tuple[CompiledJoinKey[BackendExprT], ...]:
        out: list[CompiledJoinKey[BackendExprT]] = []
        for k in keys:
            if isinstance(k, JoinKeyColumn):
                out.append(CompiledJoinKey(column=k.name, expr=None))
            else:
                out.append(CompiledJoinKey(column=None, expr=self._compile(k.expr)))
        return tuple(out)

    def _compile_named_aggs(
        self, named_aggs: dict[str, tuple[str, str] | Expr[Any]]
    ) -> dict[str, tuple[str, str] | BackendExprT]:
        out: dict[str, tuple[str, str] | BackendExprT] = {}
        for name, spec in named_aggs.items():
            if (
                isinstance(spec, tuple)
                and len(spec) == 2
                and isinstance(spec[0], str)
                and isinstance(spec[1], str)
            ):
                out[name] = cast(tuple[str, str], spec)
            else:
                out[name] = self._compile(spec)
        return out

    def _normalize_join_keys(
        self, items: tuple[str | Expr[Any], ...]
    ) -> tuple[JoinKeyColumn | JoinKeyExpr, ...]:
        out: list[JoinKeyColumn | JoinKeyExpr] = []
        for x in items:
            if isinstance(x, str):
                out.append(JoinKeyColumn(name=x))
            elif isinstance(x, Expr):
                out.append(JoinKeyExpr(expr=x))
            else:
                raise TypeError(
                    f"join keys must be column names (str) or Expr, got {type(x).__name__!r}"
                )
        return tuple(out)

    def _eval(self, node: PlanNode) -> BackendFrameT:
        if isinstance(node, Source):
            return self._data
        if isinstance(node, Select):
            return self._adapter.select(self._eval(node.prev), node.columns)
        if isinstance(node, Project):
            prev = self._eval(node.prev)
            parts: list[CompiledProjectItem[BackendExprT]] = []
            for it in node.items:
                if isinstance(it, ProjectPick):
                    parts.append(
                        CompiledProjectItem(name=it.column, from_column=it.column, expr=None)
                    )
                else:
                    parts.append(
                        CompiledProjectItem(
                            name=it.name,
                            from_column=None,
                            expr=self._compile(it.expr),
                        )
                    )
            return self._adapter.project(prev, tuple(parts))
        if isinstance(node, Drop):
            return self._adapter.drop(self._eval(node.prev), node.columns, strict=node.strict)
        if isinstance(node, Rename):
            return self._adapter.rename(self._eval(node.prev), node.mapping, strict=node.strict)
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
            prev = self._eval(node.prev)
            compiled: list[CompiledSortKey[BackendExprT]] = []
            for k in node.keys:
                if isinstance(k, SortColumnKey):
                    compiled.append(CompiledSortKey(column=k.name, expr=None))
                else:
                    compiled.append(CompiledSortKey(column=None, expr=self._compile(k.expr)))
            return self._adapter.sort(
                prev,
                tuple(compiled),
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
            compiled_keys = self._compile_join_keys_tuple(node.prev.keys)
            compiled_aggs = self._compile_named_aggs(node.named_aggs)
            return self._adapter.group_by_agg(
                self._eval(node.prev.prev),
                keys=compiled_keys,
                named_aggs=compiled_aggs,
            )
        if isinstance(node, DropNulls):
            return self._adapter.drop_nulls(
                self._eval(node.prev),
                node.subset,
                how=node.how,
                threshold=node.threshold,
            )
        if isinstance(node, FillNull):
            prev = self._eval(node.prev)
            if node.value is not None and isinstance(node.value, Expr):
                compiled_value: BackendExprT = self._compile(node.value)
            else:
                compiled_value = node.value
            return self._adapter.fill_null(
                prev,
                compiled_value,
                node.subset,
                strategy=node.strategy,
            )
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
            if node.left_keys is node.right_keys:
                compiled = self._compile_join_keys_tuple(node.left_keys)
                lo = ro = compiled
            else:
                lo = self._compile_join_keys_tuple(node.left_keys)
                ro = self._compile_join_keys_tuple(node.right_keys)
            return self._adapter.join(
                left_df,
                right_df,
                left_on=lo,
                right_on=ro,
                how=node.how,
                suffix=node.suffix,
                options=node.options,
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
            return self._adapter.unnest(self._eval(node.prev), node.column, fields=node.fields)
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

    def select(
        self, *columns: str | tuple[str, Expr[Any]]
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        if not columns:
            cols: tuple[str, ...] = tuple()
            schema2 = self._schema.select(cols)
            return Frame(
                _data=self._data,
                _adapter=self._adapter,
                _plan=Select(self._plan, cols),
                _schema=schema2,
            )
        if all(isinstance(c, str) for c in columns):
            cols = cast(tuple[str, ...], tuple(columns))
            schema2 = self._schema.select(cols)
            return Frame(
                _data=self._data,
                _adapter=self._adapter,
                _plan=Select(self._plan, cols),
                _schema=schema2,
            )
        items: list[ProjectPick | ProjectExpr] = []
        for c in columns:
            if isinstance(c, str):
                items.append(ProjectPick(column=c))
            elif isinstance(c, tuple) and len(c) == 2 and isinstance(c[0], str):
                items.append(ProjectExpr(name=c[0], expr=c[1]))
            else:
                raise TypeError(
                    "select arguments must be column names (str) or (name, Expr) tuples"
                )
        tup = tuple(items)
        schema3 = self._schema.project(tup)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Project(self._plan, tup),
            _schema=schema3,
        )

    def select_prefix(self, prefix: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        cols = tuple(n for n in self._schema.names() if n.startswith(prefix))
        schema2 = self._schema.select(cols)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, cols),
            _schema=schema2,
        )

    def select_suffix(self, suffix: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        cols = tuple(n for n in self._schema.names() if n.endswith(suffix))
        schema2 = self._schema.select(cols)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, cols),
            _schema=schema2,
        )

    def select_regex(self, pattern: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        rx = re.compile(pattern)
        cols = tuple(n for n in self._schema.names() if rx.search(n))
        schema2 = self._schema.select(cols)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, cols),
            _schema=schema2,
        )

    def select_exclude(self, *columns: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        cols = tuple(columns)
        schema2 = self._schema.select_exclude(cols)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def reorder_columns(self, *columns: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        schema2 = self._schema.reorder_columns(columns)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def select_first(self, *columns: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        schema2 = self._schema.select_first(columns)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def select_last(self, *columns: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        schema2 = self._schema.select_last(columns)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def move(
        self, column: str, *, before: str | None = None, after: str | None = None
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        schema2 = self._schema.move(column, before=before, after=after)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def drop(
        self, *columns: str, strict: bool = True
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        cols = tuple(columns)
        schema2 = self._schema.drop(cols, strict=strict)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Drop(self._plan, cols, strict=strict),
            _schema=schema2,
        )

    def drop_prefix(self, prefix: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        cols = tuple(n for n in self._schema.names() if n.startswith(prefix))
        if not cols:
            return self
        schema2 = self._schema.drop(cols)
        return Frame(
            _data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2
        )

    def drop_suffix(self, suffix: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        cols = tuple(n for n in self._schema.names() if n.endswith(suffix))
        if not cols:
            return self
        schema2 = self._schema.drop(cols)
        return Frame(
            _data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2
        )

    def drop_regex(self, pattern: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        rx = re.compile(pattern)
        cols = tuple(n for n in self._schema.names() if rx.search(n))
        if not cols:
            return self
        schema2 = self._schema.drop(cols)
        return Frame(
            _data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2
        )

    def rename(
        self, *, strict: bool = True, **mapping: str
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        if not mapping:
            return self
        schema2 = self._schema.rename(mapping, strict=strict)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping, strict=strict),
            _schema=schema2,
        )

    def rename_prefix(
        self, prefix: str, *subset: str
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        names = subset if subset else self._schema.names()
        mapping = {n: f"{prefix}{n}" for n in names}
        schema2 = self._schema.rename(mapping, strict=True)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping, strict=True),
            _schema=schema2,
        )

    def rename_suffix(
        self, suffix: str, *subset: str
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        names = subset if subset else self._schema.names()
        mapping = {n: f"{n}{suffix}" for n in names}
        schema2 = self._schema.rename(mapping, strict=True)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping, strict=True),
            _schema=schema2,
        )

    def rename_replace(
        self, old: str, new: str, *subset: str
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        names = subset if subset else self._schema.names()
        mapping = {n: n.replace(old, new) for n in names}
        schema2 = self._schema.rename(mapping, strict=True)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping, strict=True),
            _schema=schema2,
        )

    def with_column(
        self, name: str, expr: Expr[Any]
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        dtype = infer_dtype(expr)
        schema2 = self._schema.with_column(name, dtype=dtype)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=WithColumn(self._plan, name=name, expr=expr),
            _schema=schema2,
        )

    def cast(self, name: str, dtype: object) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        schema2 = self._schema.cast(name, dtype=dtype)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Cast(self._plan, name=name, dtype=dtype),
            _schema=schema2,
        )

    def filter(self, predicate: Expr[bool]) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Filter(self._plan, predicate=predicate),
            _schema=self._schema,
        )

    def sort(
        self,
        *keys: str | Expr[Any],
        descending: bool | Sequence[bool] = False,
        nulls_last: bool | Sequence[bool] = False,
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        sort_keys: list[SortColumnKey | SortExprKey] = []
        for k in keys:
            if isinstance(k, str):
                self._schema.get(k)
                sort_keys.append(SortColumnKey(name=k))
            elif isinstance(k, Expr):
                sort_keys.append(SortExprKey(expr=k))
            else:
                raise TypeError(
                    f"sort keys must be column names (str) or Expr, got {type(k).__name__!r}"
                )
        key_tuple = tuple(sort_keys)
        des = _coerce_sort_flags("descending", len(key_tuple), descending)
        nls = _coerce_sort_flags("nulls_last", len(key_tuple), nulls_last)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Sort(self._plan, keys=key_tuple, descending=des, nulls_last=nls),
            _schema=self._schema,
        )

    def unique(
        self,
        *subset: str,
        keep: Literal["first", "last"] = "first",
        maintain_order: bool = False,
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
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
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        return self.unique(*subset, keep=keep, maintain_order=maintain_order)

    def duplicated(
        self,
        *subset: str,
        keep: Literal["first", "last"] | bool = "first",
        out_name: str = "duplicated",
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
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
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
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

    def group_by(
        self, *keys: str | Expr[Any]
    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]:
        if not keys:
            raise PlanFrameBackendError("group_by requires at least one key")
        items = self._normalize_join_keys(tuple(keys))
        fm = self._schema.field_map()
        for k in items:
            if isinstance(k, JoinKeyColumn):
                self._schema.get(k.name)
            else:
                missing = collect_col_names_in_expr(k.expr).difference(fm.keys())
                if missing:
                    raise PlanFrameSchemaError(
                        f"group_by expression references unknown columns: {sorted(missing)}"
                    )
        return GroupedFrame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=self._plan,
            _schema=self._schema,
            _key_items=items,
        )

    def drop_nulls(
        self,
        *subset: str,
        how: Literal["any", "all"] = "any",
        threshold: int | None = None,
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        sub = tuple(subset) if subset else None
        if how not in ("any", "all"):
            raise ValueError("drop_nulls how must be 'any' or 'all'")
        if threshold is not None and threshold < 0:
            raise ValueError("drop_nulls threshold must be non-negative")
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.drop_nulls()
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=DropNulls(self._plan, subset=sub, how=how, threshold=threshold),
            _schema=schema2,
        )

    def drop_nulls_all(self, *subset: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
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

    def fill_null(
        self,
        value: Scalar | Expr[Any] | None = None,
        *subset: str,
        strategy: str | None = None,
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        sub = tuple(subset) if subset else None
        if (value is None) == (strategy is None):
            raise ValueError("fill_null requires exactly one of value= or strategy=")
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.fill_null()
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=FillNull(self._plan, value=value, subset=sub, strategy=strategy),
            _schema=schema2,
        )

    def melt(
        self,
        *,
        id_vars: tuple[str, ...],
        value_vars: tuple[str, ...],
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
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
        other: Frame[Any, BackendFrameT, BackendExprT],
        *,
        on: tuple[str | Expr[Any], ...] | None = None,
        left_on: tuple[str | Expr[Any], ...] | None = None,
        right_on: tuple[str | Expr[Any], ...] | None = None,
        how: Literal["inner", "left", "right", "full", "semi", "anti", "cross"] = "inner",
        suffix: str = "_right",
        options: JoinOptions | None = None,
    ) -> Frame[Any, BackendFrameT, BackendExprT]:
        if how == "cross":
            if on is not None or left_on is not None or right_on is not None:
                raise ValueError("cross join must not specify join keys (on, left_on, or right_on)")
            schema2 = self._schema.join_merge_cross(other._schema, suffix=suffix)
            lk: tuple[JoinKeyColumn | JoinKeyExpr, ...] = ()
            rk: tuple[JoinKeyColumn | JoinKeyExpr, ...] = ()
        else:
            if on is not None:
                if left_on is not None or right_on is not None:
                    raise ValueError("join(): pass either on= or left_on=/right_on=, not both")
                lk = rk = self._normalize_join_keys(tuple(on))
            elif left_on is not None and right_on is not None:
                lk = self._normalize_join_keys(tuple(left_on))
                rk = self._normalize_join_keys(tuple(right_on))
            elif left_on is not None or right_on is not None:
                raise ValueError("join(): left_on and right_on must be provided together")
            else:
                raise ValueError("join(): requires on= or both left_on= and right_on=")
            if len(lk) != len(rk):
                raise ValueError("join(): left_on and right_on must have the same length")
            if not lk:
                raise ValueError("join keys must be non-empty for non-cross joins")
            schema2 = self._schema.join_merge(other._schema, left_on=lk, right_on=rk, suffix=suffix)

        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Join(
                self._plan,
                right=other,
                left_keys=lk,
                right_keys=rk,
                how=how,
                suffix=suffix,
                options=options,
            ),
            _schema=schema2,
        )

    def slice(
        self, offset: int, length: int | None = None
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        if length is not None and length < 0:
            raise ValueError("length must be non-negative or None")
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Slice(self._plan, offset=offset, length=length),
            _schema=self._schema,
        )

    def limit(self, n: int) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        return self.head(n)

    def head(self, n: int) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        if n < 0:
            raise ValueError("n must be non-negative")
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Head(self._plan, n=n),
            _schema=self._schema,
        )

    def tail(self, n: int) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        if n < 0:
            raise ValueError("n must be non-negative")
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Tail(self._plan, n=n),
            _schema=self._schema,
        )

    def concat_vertical(
        self, other: Frame[SchemaT, BackendFrameT, BackendExprT]
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
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
        self, other: Frame[SchemaT, BackendFrameT, BackendExprT]
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
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
        self, other: Frame[SchemaT, BackendFrameT, BackendExprT]
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        return self.concat_vertical(other).unique()

    def explode(self, column: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        schema2 = self._schema.explode(column)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Explode(self._plan, column=column),
            _schema=schema2,
        )

    def unnest(
        self, column: str, *, fields: tuple[str, ...]
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        schema2 = self._schema.unnest(column, fields=fields)
        return Frame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Unnest(self._plan, column=column, fields=fields),
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
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
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

    async def acollect(
        self,
        *,
        kind: Literal["dataclass", "pydantic"] | None = None,
        name: str = "Row",
    ) -> BackendFrameT | list[Any]:
        try:
            planned = self._eval(self._plan)
            out = await self._adapter.acollect(planned)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend acollect failed for {self._adapter.name}"
            ) from e

        if kind is None:
            return out

        Model = materialize_model(name=name, schema=self._schema, kind=kind)
        try:
            rows = self._adapter.to_dicts(out)
            return [Model(**r) for r in rows]
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend acollect(kind={kind!r}) failed for {self._adapter.name}"
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

    async def ato_dicts(self) -> list[dict[str, object]]:
        try:
            planned = self._eval(self._plan)
            return await self._adapter.ato_dicts(planned)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend ato_dicts failed for {self._adapter.name}"
            ) from e

    async def ato_dict(self) -> dict[str, list[object]]:
        try:
            planned = self._eval(self._plan)
            return await self._adapter.ato_dict(planned)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend ato_dict failed for {self._adapter.name}"
            ) from e

    def write_parquet(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4"] = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: StorageOptions | None = None,
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
        storage_options: StorageOptions | None = None,
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

    def write_ndjson(self, path: str, *, storage_options: StorageOptions | None = None) -> None:
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
        storage_options: StorageOptions | None = None,
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
        connection: object,
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
        storage_options: StorageOptions | None = None,
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
