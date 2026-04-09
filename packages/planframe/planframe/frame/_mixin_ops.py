"""Lazy transforms: selection through pivot."""

from __future__ import annotations

import numbers
import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast

if TYPE_CHECKING:
    from planframe.frame._class import Frame

from typing_extensions import Self

from planframe.backend.errors import PlanFrameBackendError, PlanFrameSchemaError
from planframe.dynamic_groupby import DynamicGroupedFrame
from planframe.expr.api import Expr, clip, col, infer_dtype, lit
from planframe.frame._utils import _coerce_sort_flags
from planframe.groupby import GroupedFrame
from planframe.plan.join_options import JoinOptions
from planframe.plan.nodes import (
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
    Head,
    Join,
    JoinKeyColumn,
    JoinKeyExpr,
    Melt,
    Pivot,
    PlanNode,
    Posexplode,
    Project,
    ProjectExpr,
    ProjectPick,
    Rename,
    RollingAgg,
    Sample,
    Select,
    Slice,
    Sort,
    SortColumnKey,
    SortExprKey,
    Tail,
    Unique,
    Unnest,
    UnnestItem,
    WithColumn,
    WithRowCount,
)
from planframe.schema.ir import Field, Schema, collect_col_names_in_expr
from planframe.selector import ColumnSelector, _apply_strict
from planframe.typing.frame_like import FrameLike
from planframe.typing.scalars import Scalar

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


class FrameOpsMixin(Generic[SchemaT, BackendFrameT, BackendExprT]):
    """Column operations, joins, reshaping, and windowed transforms."""

    __slots__ = ()
    # These are provided by `FramePlanMixin` at runtime; declare them so type checkers
    # understand that ops mixins can access internals without depending on the concrete `Frame`.
    _data: BackendFrameT
    _adapter: Any
    _plan: PlanNode
    _schema: Schema
    if TYPE_CHECKING:
        # Provided by `FramePlanMixin` on the concrete `Frame` class. Declared here so
        # type checkers accept `type(self)(...)` construction in these mixins.
        def __init__(  # noqa: D401
            self, _data: BackendFrameT, _adapter: Any, _plan: PlanNode, _schema: Schema
        ) -> None: ...
        def _normalize_join_keys(self, items: tuple[str | Expr[Any], ...]) -> tuple[Any, ...]: ...

    def select(self, *columns: str | tuple[str, Expr[Any]]) -> Self:
        if not columns:
            cols: tuple[str, ...] = tuple()
            schema2 = self._schema.select(cols)
            return type(self)(
                _data=self._data,
                _adapter=self._adapter,
                _plan=Select(self._plan, cols),
                _schema=schema2,
            )
        if all(isinstance(c, str) for c in columns):
            cols = cast(tuple[str, ...], tuple(columns))
            schema2 = self._schema.select(cols)
            return type(self)(
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
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Project(self._plan, tup),
            _schema=schema3,
        )

    def select_prefix(self, prefix: str) -> Self:
        cols = tuple(n for n in self._schema.names() if n.startswith(prefix))
        schema2 = self._schema.select(cols)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, cols),
            _schema=schema2,
        )

    def select_suffix(self, suffix: str) -> Self:
        cols = tuple(n for n in self._schema.names() if n.endswith(suffix))
        schema2 = self._schema.select(cols)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, cols),
            _schema=schema2,
        )

    def select_regex(self, pattern: str) -> Self:
        rx = re.compile(pattern)
        cols = tuple(n for n in self._schema.names() if rx.search(n))
        schema2 = self._schema.select(cols)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, cols),
            _schema=schema2,
        )

    def select_schema(self, selector: ColumnSelector, *, strict: bool = True) -> Self:
        cols = _apply_strict(cols=selector.select(self._schema), strict=strict, selector=selector)
        schema2 = self._schema.select(cols)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, cols),
            _schema=schema2,
        )

    def select_exclude(self, *columns: str) -> Self:
        cols = tuple(columns)
        schema2 = self._schema.select_exclude(cols)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def reorder_columns(self, *columns: str) -> Self:
        schema2 = self._schema.reorder_columns(columns)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def select_first(self, *columns: str) -> Self:
        schema2 = self._schema.select_first(columns)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def select_last(self, *columns: str) -> Self:
        schema2 = self._schema.select_last(columns)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def move(self, column: str, *, before: str | None = None, after: str | None = None) -> Self:
        schema2 = self._schema.move(column, before=before, after=after)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Select(self._plan, schema2.names()),
            _schema=schema2,
        )

    def drop(self, *columns: str, strict: bool = True) -> Self:
        cols = tuple(columns)
        schema2 = self._schema.drop(cols, strict=strict)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Drop(self._plan, cols, strict=strict),
            _schema=schema2,
        )

    def drop_prefix(self, prefix: str) -> Self:
        cols = tuple(n for n in self._schema.names() if n.startswith(prefix))
        if not cols:
            return self
        schema2 = self._schema.drop(cols)
        return type(self)(
            _data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2
        )

    def drop_suffix(self, suffix: str) -> Self:
        cols = tuple(n for n in self._schema.names() if n.endswith(suffix))
        if not cols:
            return self
        schema2 = self._schema.drop(cols)
        return type(self)(
            _data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2
        )

    def drop_regex(self, pattern: str) -> Self:
        rx = re.compile(pattern)
        cols = tuple(n for n in self._schema.names() if rx.search(n))
        if not cols:
            return self
        schema2 = self._schema.drop(cols)
        return type(self)(
            _data=self._data, _adapter=self._adapter, _plan=Drop(self._plan, cols), _schema=schema2
        )

    def rename(self, *, strict: bool = True, **mapping: str) -> Self:
        if not mapping:
            return self
        schema2 = self._schema.rename(mapping, strict=strict)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping, strict=strict),
            _schema=schema2,
        )

    def rename_prefix(self, prefix: str, *subset: str) -> Self:
        names = subset if subset else self._schema.names()
        mapping = {n: f"{prefix}{n}" for n in names}
        schema2 = self._schema.rename(mapping, strict=True)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping, strict=True),
            _schema=schema2,
        )

    def rename_suffix(self, suffix: str, *subset: str) -> Self:
        names = subset if subset else self._schema.names()
        mapping = {n: f"{n}{suffix}" for n in names}
        schema2 = self._schema.rename(mapping, strict=True)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping, strict=True),
            _schema=schema2,
        )

    def rename_replace(self, old: str, new: str, *subset: str) -> Self:
        names = subset if subset else self._schema.names()
        mapping = {n: n.replace(old, new) for n in names}
        schema2 = self._schema.rename(mapping, strict=True)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping, strict=True),
            _schema=schema2,
        )

    def rename_upper(self, *subset: str, strict: bool = True) -> Self:
        names = subset if subset else self._schema.names()
        if not strict:
            names = tuple(n for n in names if n in self._schema.field_map())
        mapping = {n: n.upper() for n in names}
        if not mapping:
            return self
        schema2 = self._schema.rename(mapping, strict=strict)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping, strict=strict),
            _schema=schema2,
        )

    def rename_lower(self, *subset: str, strict: bool = True) -> Self:
        names = subset if subset else self._schema.names()
        if not strict:
            names = tuple(n for n in names if n in self._schema.field_map())
        mapping = {n: n.lower() for n in names}
        if not mapping:
            return self
        schema2 = self._schema.rename(mapping, strict=strict)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping, strict=strict),
            _schema=schema2,
        )

    def rename_title(self, *subset: str, strict: bool = True) -> Self:
        names = subset if subset else self._schema.names()
        if not strict:
            names = tuple(n for n in names if n in self._schema.field_map())
        mapping = {n: n.title() for n in names}
        if not mapping:
            return self
        schema2 = self._schema.rename(mapping, strict=strict)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping, strict=strict),
            _schema=schema2,
        )

    def rename_strip(
        self,
        *subset: str,
        chars: str | None = None,
        strict: bool = True,
    ) -> Self:
        names = subset if subset else self._schema.names()
        if not strict:
            names = tuple(n for n in names if n in self._schema.field_map())
        mapping = {n: n.strip(chars) for n in names}
        if not mapping:
            return self
        schema2 = self._schema.rename(mapping, strict=strict)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Rename(self._plan, mapping, strict=strict),
            _schema=schema2,
        )

    def with_column(self, name: str, expr: Expr[Any]) -> Self:
        dtype = infer_dtype(expr)
        schema2 = self._schema.with_column(name, dtype=dtype)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=WithColumn(self._plan, name=name, expr=expr),
            _schema=schema2,
        )

    def cast(self, name: str, dtype: object) -> Self:
        schema2 = self._schema.cast(name, dtype=dtype)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Cast(self._plan, name=name, dtype=dtype),
            _schema=schema2,
        )

    def cast_many(
        self,
        mapping: Mapping[str, object],
        *,
        strict: bool = True,
    ) -> Self:
        if not mapping:
            raise ValueError("cast_many requires non-empty mapping")

        out: Self = self
        for name, dtype in mapping.items():
            if strict:
                out._schema.get(name)
                out = out.cast(name, dtype)
            else:
                if name in out._schema.field_map():
                    out = out.cast(name, dtype)
        return out

    def cast_subset(
        self,
        *columns: str,
        dtype: object,
        strict: bool = True,
    ) -> Self:
        if not columns:
            raise ValueError("cast_subset requires at least one column")
        if len(set(columns)) != len(columns):
            raise ValueError("cast_subset columns must be unique")

        out: Self = self
        for name in columns:
            if strict:
                out._schema.get(name)
                out = out.cast(name, dtype)
            else:
                if name in out._schema.field_map():
                    out = out.cast(name, dtype)
        return out

    def with_row_count(self, *, name: str = "row_nr", offset: int = 0) -> Self:
        if offset < 0:
            raise ValueError("with_row_count offset must be non-negative")
        schema2 = self._schema.with_row_count(name)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=WithRowCount(self._plan, name=name, offset=offset),
            _schema=schema2,
        )

    def clip(
        self,
        *,
        lower: Expr[object] | Scalar | None = None,
        upper: Expr[object] | Scalar | None = None,
        subset: Sequence[str] | None = None,
    ) -> Self:
        if lower is None and upper is None:
            raise ValueError("clip requires at least one of lower= or upper=")

        lower_expr: Expr[object] | None
        if isinstance(lower, Expr) or lower is None:
            lower_expr = cast(Expr[object] | None, lower)
        else:
            lower_expr = cast(Expr[object], lit(lower))

        upper_expr: Expr[object] | None
        if isinstance(upper, Expr) or upper is None:
            upper_expr = cast(Expr[object] | None, upper)
        else:
            upper_expr = cast(Expr[object], lit(upper))

        if subset is None:
            cols: list[str] = []
            for f in self._schema.fields:
                dt = f.dtype
                if (
                    isinstance(dt, type)
                    and issubclass(dt, numbers.Real)
                    and not issubclass(dt, bool)
                ):
                    cols.append(f.name)
            if not cols:
                raise PlanFrameSchemaError(
                    "clip subset=None requires at least one numeric column in schema"
                )
        else:
            cols = list(subset)
            if not cols:
                raise ValueError("clip subset must be non-empty when provided")
            if len(set(cols)) != len(cols):
                raise ValueError("clip subset must be unique")
            for c in cols:
                f = self._schema.get(c)
                dt = f.dtype
                if not (
                    isinstance(dt, type)
                    and issubclass(dt, numbers.Real)
                    and not issubclass(dt, bool)
                ):
                    raise PlanFrameSchemaError(
                        f"clip requires numeric columns; got {c!r} with dtype {dt!r}"
                    )

        out: Self = self
        for c in cols:
            out = type(self)(
                _data=out._data,
                _adapter=out._adapter,
                _plan=WithColumn(
                    out._plan, name=c, expr=clip(col(c), lower=lower_expr, upper=upper_expr)
                ),
                _schema=out._schema,  # clip preserves dtype
            )
        return out

    def filter(self, predicate: Expr[bool]) -> Self:
        return type(self)(
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
    ) -> Self:
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
        return type(self)(
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
    ) -> Self:
        sub = tuple(subset) if subset else None
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.unique()
        return type(self)(
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
    ) -> Self:
        return self.unique(*subset, keep=keep, maintain_order=maintain_order)

    def duplicated(
        self,
        *subset: str,
        keep: Literal["first", "last"] | bool = "first",
        out_name: str = "duplicated",
    ) -> Self:
        sub = tuple(subset) if subset else None
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.duplicated(out_name=out_name)
        return type(self)(
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
    ) -> Self:
        if (n is None) == (frac is None):
            raise ValueError("sample requires exactly one of n= or frac=")
        if n is not None and n < 0:
            raise ValueError("sample n must be non-negative")
        if frac is not None and frac < 0:
            raise ValueError("sample frac must be non-negative")
        if frac is not None and frac > 1.0 and not with_replacement:
            raise ValueError("sample frac > 1.0 requires with_replacement=True")
        return type(self)(
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

    def group_by_dynamic(
        self,
        index_column: str,
        *,
        every: str,
        period: str | None = None,
        by: Sequence[str] | None = None,
    ) -> DynamicGroupedFrame[SchemaT, BackendFrameT, BackendExprT]:
        self._schema.get(index_column)
        by_tup = tuple(by) if by is not None else None
        if by_tup is not None:
            self._schema.select(by_tup)
        if not every:
            raise ValueError("group_by_dynamic requires non-empty every")
        if period is not None and not period:
            raise ValueError("group_by_dynamic period must be non-empty when provided")

        return DynamicGroupedFrame(
            _data=self._data,
            _adapter=self._adapter,
            _plan=self._plan,
            _schema=self._schema,
            _index_column=index_column,
            _every=every,
            _period=period,
            _by=by_tup,
        )

    def rolling_agg(
        self,
        *,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: Sequence[str] | None = None,
        min_periods: int = 1,
    ) -> Self:
        self._schema.get(on)
        self._schema.get(column)
        by_tup = tuple(by) if by is not None else None
        if by_tup is not None:
            self._schema.select(by_tup)
        if isinstance(window_size, int) and window_size <= 0:
            raise ValueError("rolling_agg window_size must be positive")
        if isinstance(window_size, str) and not window_size:
            raise ValueError("rolling_agg window_size must be non-empty")
        if min_periods <= 0:
            raise ValueError("rolling_agg min_periods must be positive")
        if not out_name:
            raise ValueError("rolling_agg requires non-empty out_name")

        dtype: object = object
        if op in {"count"}:
            dtype = int
        schema2 = self._schema.with_column(out_name, dtype=dtype)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=RollingAgg(
                self._plan,
                on=on,
                column=column,
                window_size=window_size,
                op=op,
                out_name=out_name,
                by=by_tup,
                min_periods=min_periods,
            ),
            _schema=schema2,
        )

    def drop_nulls(
        self,
        *subset: str,
        how: Literal["any", "all"] = "any",
        threshold: int | None = None,
    ) -> Self:
        sub = tuple(subset) if subset else None
        if how not in ("any", "all"):
            raise ValueError("drop_nulls how must be 'any' or 'all'")
        if threshold is not None and threshold < 0:
            raise ValueError("drop_nulls threshold must be non-negative")
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.drop_nulls()
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=DropNulls(self._plan, subset=sub, how=how, threshold=threshold),
            _schema=schema2,
        )

    def drop_nulls_all(self, *subset: str) -> Self:
        sub = tuple(subset) if subset else None
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.drop_nulls_all()
        return type(self)(
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
    ) -> Self:
        sub = tuple(subset) if subset else None
        if (value is None) == (strategy is None):
            raise ValueError("fill_null requires exactly one of value= or strategy=")
        if sub is not None:
            self._schema.select(sub)  # validate
        schema2 = self._schema.fill_null()
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=FillNull(self._plan, value=value, subset=sub, strategy=strategy),
            _schema=schema2,
        )

    def fill_null_subset(
        self,
        value: Scalar | Expr[Any] | None = None,
        *columns: str,
        strategy: str | None = None,
    ) -> Self:
        if not columns:
            raise ValueError("fill_null_subset requires at least one column")
        return self.fill_null(value, *columns, strategy=strategy)

    def fill_null_many(
        self,
        mapping: Mapping[str, Scalar | Expr[Any]],
        *,
        strict: bool = True,
    ) -> Self:
        if not mapping:
            raise ValueError("fill_null_many requires non-empty mapping")

        out: Self = self
        for name, value in mapping.items():
            if strict:
                out._schema.get(name)
                out = out.fill_null(value, name)
            else:
                if name in out._schema.field_map():
                    out = out.fill_null(value, name)
        return out

    def melt(
        self,
        *,
        id_vars: Sequence[str] | None = None,
        value_vars: Sequence[str] | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> Self:
        names = self._schema.names()
        if id_vars is None and value_vars is None:
            id_tup: tuple[str, ...] = ()
            val_tup: tuple[str, ...] = names
        elif id_vars is None:
            val_tup = tuple(value_vars or ())
            id_tup = tuple(n for n in names if n not in set(val_tup))
        elif value_vars is None:
            id_tup = tuple(id_vars)
            val_tup = tuple(n for n in names if n not in set(id_tup))
        else:
            id_tup = tuple(id_vars)
            val_tup = tuple(value_vars)

        if not val_tup:
            raise PlanFrameSchemaError(
                "melt requires non-empty value_vars (explicitly or via inference)"
            )

        schema2 = self._schema.melt(
            id_vars=id_tup,
            value_vars=val_tup,
            variable_name=variable_name,
            value_name=value_name,
        )
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Melt(
                self._plan,
                id_vars=id_tup,
                value_vars=val_tup,
                variable_name=variable_name,
                value_name=value_name,
            ),
            _schema=schema2,
        )

    def unpivot(
        self,
        *,
        index: Sequence[str] | None = None,
        on: Sequence[str] | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> Self:
        return self.melt(
            id_vars=tuple(index) if index is not None else None,
            value_vars=tuple(on) if on is not None else None,
            variable_name=variable_name,
            value_name=value_name,
        )

    def pivot_longer(
        self,
        *,
        id_vars: Sequence[str] | None = None,
        value_vars: Sequence[str] | None = None,
        names_to: str = "variable",
        values_to: str = "value",
    ) -> Self:
        return self.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            variable_name=names_to,
            value_name=values_to,
        )

    def pivot_wider(
        self,
        *,
        index: Sequence[str],
        names_from: str,
        values_from: str | Sequence[str],
        aggregate_function: Literal[
            "first", "last", "sum", "mean", "min", "max", "count", "len", "median"
        ] = "first",
        on_columns: Sequence[str] | None = None,
        sort_columns: bool = False,
        separator: str = "_",
    ) -> Self:
        return self.pivot(
            index=index,
            columns=names_from,
            values=values_from,
            agg=aggregate_function,
            on_columns=(tuple(on_columns) if on_columns is not None else None),
            sort_columns=sort_columns,
            separator=separator,
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
    ) -> Any:
        if other._adapter.name != self._adapter.name:
            raise PlanFrameBackendError("Cannot join frames from different backends")
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

        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Join(
                self._plan,
                right=cast(FrameLike, other),
                left_keys=lk,
                right_keys=rk,
                how=how,
                suffix=suffix,
                options=options,
            ),
            _schema=schema2,
        )

    def slice(self, offset: int, length: int | None = None) -> Self:
        if length is not None and length < 0:
            raise ValueError("length must be non-negative or None")
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Slice(self._plan, offset=offset, length=length),
            _schema=self._schema,
        )

    def limit(self, n: int) -> Self:
        return self.head(n)

    def head(self, n: int) -> Self:
        if n < 0:
            raise ValueError("n must be non-negative")
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Head(self._plan, n=n),
            _schema=self._schema,
        )

    def tail(self, n: int) -> Self:
        if n < 0:
            raise ValueError("n must be non-negative")
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Tail(self._plan, n=n),
            _schema=self._schema,
        )

    def concat_vertical(self, other: Frame[SchemaT, BackendFrameT, BackendExprT]) -> Self:
        if other._adapter.name != self._adapter.name:
            raise PlanFrameBackendError("Cannot concat frames from different backends")
        if self._schema.names() != other._schema.names():
            raise PlanFrameSchemaError(
                "concat_vertical requires identical column names and ordering"
            )
        for name in self._schema.names():
            if self._schema.get(name).dtype != other._schema.get(name).dtype:
                raise PlanFrameSchemaError(f"concat_vertical dtype mismatch for column: {name}")
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=ConcatVertical(self._plan, other=cast(FrameLike, other)),
            _schema=self._schema,
        )

    def concat_horizontal(self, other: Frame[SchemaT, BackendFrameT, BackendExprT]) -> Self:
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
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=ConcatHorizontal(self._plan, other=cast(FrameLike, other)),
            _schema=schema2,
        )

    def union_distinct(self, other: Frame[SchemaT, BackendFrameT, BackendExprT]) -> Self:
        return self.concat_vertical(other).unique()

    def explode(self, *columns: str, outer: bool = False) -> Self:
        schema2 = self._schema.explode(columns)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Explode(self._plan, columns=tuple(columns), outer=outer),
            _schema=schema2,
        )

    def unnest(self, *columns: str) -> Self:
        schema2, items = self._schema.unnest(columns)
        node_items = tuple(UnnestItem(column=c, fields=f) for c, f in items)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Unnest(self._plan, items=node_items),
            _schema=schema2,
        )

    def posexplode(
        self,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        outer: bool = False,
    ) -> Self:
        value_name = column if value is None else value
        schema2 = self._schema.posexplode(column, pos=pos, value=value_name)
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Posexplode(self._plan, column=column, pos=pos, value=value, outer=outer),
            _schema=schema2,
        )

    def pivot(
        self,
        *,
        index: Sequence[str],
        columns: str | None = None,
        on: str | None = None,
        values: str | Sequence[str],
        agg: Literal[
            "first", "last", "sum", "mean", "min", "max", "count", "len", "median"
        ] = "first",
        on_columns: tuple[str, ...] | None = None,
        separator: str = "_",
        sort_columns: bool = False,
    ) -> Self:
        if columns is None and on is None:
            raise ValueError("pivot requires columns= (preferred) or on=")
        if columns is not None and on is not None:
            raise ValueError("pivot: pass either columns= or on=, not both")
        on_name = columns if columns is not None else cast(str, on)

        idx = tuple(index)
        if not idx:
            raise PlanFrameSchemaError("pivot requires non-empty index")
        self._schema.select(idx)  # validate
        self._schema.get(on_name)

        value_cols = (values,) if isinstance(values, str) else tuple(values)
        if not value_cols:
            raise PlanFrameSchemaError("pivot requires non-empty values")
        for v in value_cols:
            self._schema.get(v)

        if sort_columns and on_columns is not None:
            on_columns = tuple(sorted(on_columns))
        if (
            on_columns is None
            and self._adapter.name == "polars"
            and type(self._data).__name__ == "LazyFrame"
        ):
            raise PlanFrameBackendError(
                "pivot on Polars LazyFrame requires on_columns to be provided"
            )

        out_fields = [self._schema.get(c) for c in idx]
        if on_columns is not None:
            if len(value_cols) == 1:
                for c in on_columns:
                    out_fields.append(Field(name=str(c), dtype=object))
            else:
                for v in value_cols:
                    for c in on_columns:
                        out_fields.append(Field(name=f"{v}{separator}{c}", dtype=object))
        schema2 = Schema(fields=tuple(out_fields))
        return type(self)(
            _data=self._data,
            _adapter=self._adapter,
            _plan=Pivot(
                self._plan,
                index=idx,
                on=on_name,
                values=value_cols,
                agg=agg,
                on_columns=on_columns,
                separator=separator,
                sort_columns=sort_columns,
            ),
            _schema=schema2,
        )
