from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Literal, TypeAlias, TypeVar

from planframe.plan.join_options import JoinOptions
from planframe.schema.ir import Schema
from planframe.typing.scalars import Scalar
from planframe.typing.storage import StorageOptions

BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")

ColumnName: TypeAlias = str
Columns: TypeAlias = tuple[ColumnName, ...]
AggSpec: TypeAlias = tuple[str, ColumnName] | BackendExprT


@dataclass(frozen=True, slots=True)
class CompiledProjectItem(Generic[BackendExprT]):
    """One output column for :meth:`BaseAdapter.project`.

    Exactly one of *from_column* or *expr* must be non-``None``.
    *name* is the output column name (often equal to *from_column* for existing picks).
    """

    name: str
    from_column: str | None = None
    expr: BackendExprT | None = None


@dataclass(frozen=True, slots=True)
class CompiledSortKey(Generic[BackendExprT]):
    """One sort key for :meth:`BaseAdapter.sort`.

    Exactly one of *column* or *expr* must be non-``None``.
    *column* sorts by an existing column name; *expr* sorts by a compiled backend expression.
    """

    column: str | None = None
    expr: BackendExprT | None = None


# Join uses the same column-or-compiled-expr shape as sort keys.
CompiledJoinKey = CompiledSortKey


class BaseAdapter(ABC, Generic[BackendFrameT, BackendExprT]):
    """Backend execution base class.

    Core PlanFrame code must not import backend libraries. Adapters translate PlanFrame
    operations + expression IR into backend-native operations.

    String-only :meth:`select` may be implemented directly or lowered to :meth:`project`
    using :class:`CompiledProjectItem` with *from_column* and *name* set to each column.
    """

    name: str

    @abstractmethod
    def select(self, df: BackendFrameT, columns: Columns) -> BackendFrameT: ...

    @abstractmethod
    def project(
        self,
        df: BackendFrameT,
        items: tuple[CompiledProjectItem[BackendExprT], ...],
    ) -> BackendFrameT:
        """Project *df* to output columns described by *items* (order preserved)."""
        ...

    @abstractmethod
    def drop(self, df: BackendFrameT, columns: Columns, *, strict: bool = True) -> BackendFrameT:
        """Remove columns named in *columns* from *df*.

        When *strict* is True (default), implementations must raise if any name is missing.
        When *strict* is False, implementations must ignore names that are not present in *df*.
        """
        ...

    @abstractmethod
    def rename(
        self, df: BackendFrameT, mapping: dict[ColumnName, ColumnName], *, strict: bool = True
    ) -> BackendFrameT:
        """Rename columns according to *mapping*.

        When *strict* is True (default), implementations must raise if any key in *mapping*
        is not a column of *df*.
        When *strict* is False, implementations must ignore mapping entries whose keys are
        not present in *df*.
        """
        ...

    @abstractmethod
    def with_column(self, df: BackendFrameT, name: str, expr: BackendExprT) -> BackendFrameT: ...

    @abstractmethod
    def cast(self, df: BackendFrameT, name: str, dtype: object) -> BackendFrameT: ...

    @abstractmethod
    def filter(self, df: BackendFrameT, predicate: BackendExprT) -> BackendFrameT: ...

    @abstractmethod
    def sort(
        self,
        df: BackendFrameT,
        keys: tuple[CompiledSortKey[BackendExprT], ...],
        *,
        descending: tuple[bool, ...],
        nulls_last: tuple[bool, ...],
    ) -> BackendFrameT:
        """Sort *df* by *keys* (first key is most significant).

        *descending* and *nulls_last* must have the same length as *keys*.
        Each position applies to that sort key (per-key direction and null placement).
        """
        ...

    @abstractmethod
    def unique(
        self,
        df: BackendFrameT,
        subset: Columns | None,
        *,
        keep: str = "first",
        maintain_order: bool = False,
    ) -> BackendFrameT: ...

    @abstractmethod
    def duplicated(
        self,
        df: BackendFrameT,
        subset: Columns | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> BackendFrameT: ...

    @abstractmethod
    def group_by_agg(
        self,
        df: BackendFrameT,
        *,
        keys: tuple[CompiledJoinKey[BackendExprT], ...],
        named_aggs: dict[ColumnName, AggSpec],
    ) -> BackendFrameT:
        """Group *df* by *keys*, then apply *named_aggs*.

        Each aggregation is either ``(op, column_name)`` or a compiled backend expression
        produced from :class:`planframe.expr.api.AggExpr` (already an aggregation expr).
        """
        ...

    @abstractmethod
    def drop_nulls(
        self,
        df: BackendFrameT,
        subset: Columns | None,
        *,
        how: Literal["any", "all"] = "any",
        threshold: int | None = None,
    ) -> BackendFrameT: ...

    @abstractmethod
    def fill_null(
        self,
        df: BackendFrameT,
        value: Scalar | BackendExprT | None,
        subset: Columns | None,
        *,
        strategy: str | None = None,
    ) -> BackendFrameT: ...

    @abstractmethod
    def melt(
        self,
        df: BackendFrameT,
        *,
        id_vars: Columns,
        value_vars: Columns,
        variable_name: ColumnName,
        value_name: ColumnName,
    ) -> BackendFrameT: ...

    @abstractmethod
    def join(
        self,
        left: BackendFrameT,
        right: BackendFrameT,
        *,
        left_on: tuple[CompiledJoinKey[BackendExprT], ...],
        right_on: tuple[CompiledJoinKey[BackendExprT], ...],
        how: str = "inner",
        suffix: str = "_right",
        options: JoinOptions | None = None,
    ) -> BackendFrameT:
        """Join *right* to *left*.

        Each position in *left_on* pairs with the same index in *right_on*. Each entry is either
        a column name or a compiled expression key (see :class:`CompiledJoinKey`).

        For symmetric keys, *left_on* and *right_on* may be the same tuple object (``on=`` case).
        For a cross join, both are empty tuples. *options* is backend-specific and may be ignored.
        """
        ...

    @abstractmethod
    def slice(
        self,
        df: BackendFrameT,
        *,
        offset: int,
        length: int | None,
    ) -> BackendFrameT: ...

    @abstractmethod
    def head(self, df: BackendFrameT, n: int) -> BackendFrameT: ...

    @abstractmethod
    def tail(self, df: BackendFrameT, n: int) -> BackendFrameT: ...

    @abstractmethod
    def concat_vertical(self, left: BackendFrameT, right: BackendFrameT) -> BackendFrameT: ...

    @abstractmethod
    def concat_horizontal(self, left: BackendFrameT, right: BackendFrameT) -> BackendFrameT: ...

    @abstractmethod
    def pivot(
        self,
        df: BackendFrameT,
        *,
        index: tuple[str, ...],
        on: str,
        values: str,
        agg: str = "first",
        on_columns: tuple[str, ...] | None = None,
        separator: str = "_",
    ) -> BackendFrameT: ...

    @abstractmethod
    def write_parquet(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: StorageOptions | None = None,
    ) -> None: ...

    @abstractmethod
    def write_csv(
        self,
        df: BackendFrameT,
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: StorageOptions | None = None,
    ) -> None: ...

    @abstractmethod
    def write_ndjson(
        self, df: BackendFrameT, path: str, *, storage_options: StorageOptions | None = None
    ) -> None: ...

    @abstractmethod
    def write_ipc(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "uncompressed",
        storage_options: StorageOptions | None = None,
    ) -> None: ...

    @abstractmethod
    def write_database(
        self,
        df: BackendFrameT,
        *,
        table_name: str,
        connection: object,
        if_table_exists: str = "fail",
        engine: str | None = None,
    ) -> None: ...

    @abstractmethod
    def write_excel(self, df: BackendFrameT, path: str, *, worksheet: str = "Sheet1") -> None: ...

    @abstractmethod
    def write_delta(
        self,
        df: BackendFrameT,
        target: str,
        *,
        mode: str = "error",
        storage_options: StorageOptions | None = None,
    ) -> None: ...

    @abstractmethod
    def write_avro(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "uncompressed",
        name: str = "",
    ) -> None: ...

    @abstractmethod
    def explode(self, df: BackendFrameT, column: str) -> BackendFrameT: ...

    @abstractmethod
    def unnest(
        self, df: BackendFrameT, column: str, *, fields: tuple[str, ...]
    ) -> BackendFrameT: ...

    @abstractmethod
    def drop_nulls_all(
        self, df: BackendFrameT, subset: tuple[str, ...] | None
    ) -> BackendFrameT: ...

    @abstractmethod
    def sample(
        self,
        df: BackendFrameT,
        *,
        n: int | None = None,
        frac: float | None = None,
        with_replacement: bool = False,
        shuffle: bool = False,
        seed: int | None = None,
    ) -> BackendFrameT: ...

    @abstractmethod
    def compile_expr(self, expr: object, *, schema: Schema | None = None) -> BackendExprT: ...

    @abstractmethod
    def collect(self, df: BackendFrameT) -> BackendFrameT: ...

    @abstractmethod
    def to_dicts(self, df: BackendFrameT) -> list[dict[str, object]]: ...

    @abstractmethod
    def to_dict(self, df: BackendFrameT) -> dict[str, list[object]]: ...

    async def acollect(self, df: BackendFrameT) -> BackendFrameT:
        """Materialize *df* asynchronously.

        Default: run :meth:`collect` in a worker thread via :func:`asyncio.to_thread`
        so synchronous backends do not block the event loop.

        Async-native backends (HTTP drivers, asyncio clients) should override this
        instead of blocking in :meth:`collect`.
        """

        return await asyncio.to_thread(self.collect, df)

    async def ato_dicts(self, df: BackendFrameT) -> list[dict[str, object]]:
        """Like :meth:`to_dicts`, but awaitable. Default uses :func:`asyncio.to_thread`."""

        return await asyncio.to_thread(self.to_dicts, df)

    async def ato_dict(self, df: BackendFrameT) -> dict[str, list[object]]:
        """Like :meth:`to_dict`, but awaitable. Default uses :func:`asyncio.to_thread`."""

        return await asyncio.to_thread(self.to_dict, df)


# Backwards-compatible name for older imports.
BackendAdapter = BaseAdapter
