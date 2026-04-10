from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Literal, TypeAlias, TypeVar, cast

from planframe.backend.io import (
    AdapterAsyncReader,
    AdapterAsyncWriter,
    AdapterReader,
    AdapterWriter,
)
from planframe.execution_options import ExecutionOptions
from planframe.plan.join_options import JoinOptions
from planframe.plan.nodes import UnnestItem
from planframe.schema.ir import Schema
from planframe.typing.scalars import Scalar
from planframe.typing.storage import StorageOptions

BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")

ColumnName: TypeAlias = str
Columns: TypeAlias = tuple[ColumnName, ...]
AggSpec: TypeAlias = tuple[str, ColumnName] | BackendExprT


@dataclass(frozen=True, slots=True)
class AdapterCapabilities:
    """Optional backend capability flags.

    Adapters should expose these conservatively (False unless truly supported) so
    frontend layers can decide whether to offer a method as exact parity,
    typed-parity, or explicitly unsupported.
    """

    # ---- Plan node capabilities (transform-time) ----
    explode_outer: bool = False
    posexplode_outer: bool = False
    lazy_sample: bool = False

    # ---- IO capabilities (read/sink surfaces) ----
    #
    # These flags are intended to avoid "discovering" missing support only when a sink
    # or reader method is called. PlanFrame uses them to fail fast with actionable errors.
    scan_delta: bool = False
    read_delta: bool = False
    sink_delta: bool = False

    read_avro: bool = False
    sink_avro: bool = False

    read_excel: bool = False
    sink_excel: bool = False

    read_database_uri: bool = False
    sink_database: bool = False

    # Whether `storage_options=` is accepted on IO methods that expose it.
    storage_options: bool = False


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


@dataclass(frozen=True, slots=True)
class _DefaultAdapterWriter(Generic[BackendFrameT, BackendExprT]):
    """Default `AdapterWriter` wrapper around legacy `write_*` adapter methods."""

    adapter: BaseAdapter[BackendFrameT, BackendExprT]

    def sink_parquet(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: StorageOptions | None = None,
    ) -> None:
        self.adapter.write_parquet(
            df,
            path,
            compression=compression,
            row_group_size=row_group_size,
            partition_by=partition_by,
            storage_options=storage_options,
        )

    def sink_csv(
        self,
        df: BackendFrameT,
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: StorageOptions | None = None,
    ) -> None:
        self.adapter.write_csv(
            df,
            path,
            separator=separator,
            include_header=include_header,
            storage_options=storage_options,
        )

    def sink_ndjson(
        self, df: BackendFrameT, path: str, *, storage_options: StorageOptions | None = None
    ) -> None:
        self.adapter.write_ndjson(df, path, storage_options=storage_options)

    def sink_ipc(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "uncompressed",
        storage_options: StorageOptions | None = None,
    ) -> None:
        self.adapter.write_ipc(df, path, compression=compression, storage_options=storage_options)

    def sink_database(
        self,
        df: BackendFrameT,
        *,
        table_name: str,
        connection: object,
        if_table_exists: str = "fail",
        engine: str | None = None,
    ) -> None:
        self.adapter.write_database(
            df,
            table_name=table_name,
            connection=connection,
            if_table_exists=if_table_exists,
            engine=engine,
        )

    def sink_excel(self, df: BackendFrameT, path: str, *, worksheet: str = "Sheet1") -> None:
        self.adapter.write_excel(df, path, worksheet=worksheet)

    def sink_delta(
        self,
        df: BackendFrameT,
        target: str,
        *,
        mode: str = "error",
        storage_options: StorageOptions | None = None,
    ) -> None:
        self.adapter.write_delta(df, target, mode=mode, storage_options=storage_options)

    def sink_avro(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "uncompressed",
        name: str = "",
    ) -> None:
        self.adapter.write_avro(df, path, compression=compression, name=name)


@dataclass(frozen=True, slots=True)
class _DefaultAdapterAsyncReader(Generic[BackendFrameT]):
    """Default async reader wrapper around sync `AdapterReader`."""

    reader: AdapterReader[BackendFrameT]

    async def scan_parquet(
        self,
        path: str,
        *,
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT:
        return await asyncio.to_thread(
            self.reader.scan_parquet,
            path,
            hive_partitioning=hive_partitioning,
            storage_options=storage_options,
        )

    async def scan_parquet_dataset(
        self,
        path_or_glob: str,
        *,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT:
        return await asyncio.to_thread(
            self.reader.scan_parquet_dataset,
            path_or_glob,
            storage_options=storage_options,
        )

    async def scan_csv(
        self, path: str, *, storage_options: StorageOptions | None = None
    ) -> BackendFrameT:
        return await asyncio.to_thread(self.reader.scan_csv, path, storage_options=storage_options)

    async def scan_ndjson(
        self, path: str, *, storage_options: StorageOptions | None = None
    ) -> BackendFrameT:
        return await asyncio.to_thread(
            self.reader.scan_ndjson, path, storage_options=storage_options
        )

    async def scan_ipc(
        self,
        path: str,
        *,
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT:
        return await asyncio.to_thread(
            self.reader.scan_ipc,
            path,
            hive_partitioning=hive_partitioning,
            storage_options=storage_options,
        )

    async def scan_delta(
        self,
        source: str,
        *,
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT:
        return await asyncio.to_thread(
            self.reader.scan_delta,
            source,
            version=version,
            storage_options=storage_options,
        )

    async def read_delta(
        self,
        source: str,
        *,
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT:
        return await asyncio.to_thread(
            self.reader.read_delta,
            source,
            version=version,
            storage_options=storage_options,
        )

    async def read_excel(self, path: str, *, sheet_name: str | None = None) -> BackendFrameT:
        return await asyncio.to_thread(self.reader.read_excel, path, sheet_name=sheet_name)

    async def read_avro(self, path: str) -> BackendFrameT:
        return await asyncio.to_thread(self.reader.read_avro, path)

    async def read_database(self, query: str, *, connection: object) -> BackendFrameT:
        return await asyncio.to_thread(self.reader.read_database, query, connection=connection)

    async def read_database_uri(
        self,
        query: str,
        *,
        uri: str,
        engine: Literal["connectorx", "adbc"] | None = None,
    ) -> BackendFrameT:
        return await asyncio.to_thread(self.reader.read_database_uri, query, uri=uri, engine=engine)


@dataclass(frozen=True, slots=True)
class _DefaultAdapterAsyncWriter(Generic[BackendFrameT]):
    """Default async writer wrapper around sync `AdapterWriter`."""

    writer: AdapterWriter[BackendFrameT]

    async def sink_parquet(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: StorageOptions | None = None,
    ) -> None:
        await asyncio.to_thread(
            self.writer.sink_parquet,
            df,
            path,
            compression=compression,
            row_group_size=row_group_size,
            partition_by=partition_by,
            storage_options=storage_options,
        )

    async def sink_csv(
        self,
        df: BackendFrameT,
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: StorageOptions | None = None,
    ) -> None:
        await asyncio.to_thread(
            self.writer.sink_csv,
            df,
            path,
            separator=separator,
            include_header=include_header,
            storage_options=storage_options,
        )

    async def sink_ndjson(
        self, df: BackendFrameT, path: str, *, storage_options: StorageOptions | None = None
    ) -> None:
        await asyncio.to_thread(self.writer.sink_ndjson, df, path, storage_options=storage_options)

    async def sink_ipc(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "uncompressed",
        storage_options: StorageOptions | None = None,
    ) -> None:
        await asyncio.to_thread(
            self.writer.sink_ipc, df, path, compression=compression, storage_options=storage_options
        )

    async def sink_database(
        self,
        df: BackendFrameT,
        *,
        table_name: str,
        connection: object,
        if_table_exists: str = "fail",
        engine: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self.writer.sink_database,
            df,
            table_name=table_name,
            connection=connection,
            if_table_exists=if_table_exists,
            engine=engine,
        )

    async def sink_excel(self, df: BackendFrameT, path: str, *, worksheet: str = "Sheet1") -> None:
        await asyncio.to_thread(self.writer.sink_excel, df, path, worksheet=worksheet)

    async def sink_delta(
        self,
        df: BackendFrameT,
        target: str,
        *,
        mode: str = "error",
        storage_options: StorageOptions | None = None,
    ) -> None:
        await asyncio.to_thread(
            self.writer.sink_delta, df, target, mode=mode, storage_options=storage_options
        )

    async def sink_avro(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "uncompressed",
        name: str = "",
    ) -> None:
        await asyncio.to_thread(self.writer.sink_avro, df, path, compression=compression, name=name)


class BaseAdapter(ABC, Generic[BackendFrameT, BackendExprT]):
    """Backend execution base class.

    Core PlanFrame code must not import backend libraries. Adapters translate PlanFrame
    operations + expression IR into backend-native operations.

    String-only :meth:`select` may be implemented directly or lowered to :meth:`project`
    using :class:`CompiledProjectItem` with *from_column* and *name* set to each column.
    """

    name: str

    @property
    def reader(self) -> AdapterReader[BackendFrameT]:
        """Adapter-owned IO reader.\n\n        Default returns `self` for adapters that implement the reader surface.\n"""

        return cast(AdapterReader[BackendFrameT], self)

    @property
    def writer(self) -> AdapterWriter[BackendFrameT]:
        """Adapter-owned IO writer.\n\n        Default wraps legacy adapter `write_*` methods as `sink_*`.\n"""

        return cast(AdapterWriter[BackendFrameT], _DefaultAdapterWriter(self))

    @property
    def areader(self) -> AdapterAsyncReader[BackendFrameT]:
        """Async adapter-owned IO reader.\n\n        Default wraps `reader` via `asyncio.to_thread`.\n"""

        return cast(AdapterAsyncReader[BackendFrameT], _DefaultAdapterAsyncReader(self.reader))

    @property
    def awriter(self) -> AdapterAsyncWriter[BackendFrameT]:
        """Async adapter-owned IO writer.\n\n        Default wraps `writer` via `asyncio.to_thread`.\n"""

        return cast(AdapterAsyncWriter[BackendFrameT], _DefaultAdapterAsyncWriter(self.writer))

    @property
    def capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities()

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
    def with_row_count(
        self, df: BackendFrameT, *, name: str = "row_nr", offset: int = 0
    ) -> BackendFrameT:
        """Add a monotonically increasing row number column."""
        ...

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
    def group_by_dynamic_agg(
        self,
        df: BackendFrameT,
        *,
        index_column: str,
        every: str,
        period: str | None = None,
        by: Columns | None = None,
        named_aggs: dict[ColumnName, AggSpec],
    ) -> BackendFrameT:
        """Dynamic time-window group-by aggregation."""
        ...

    @abstractmethod
    def rolling_agg(
        self,
        df: BackendFrameT,
        *,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: Columns | None = None,
        min_periods: int = 1,
    ) -> BackendFrameT:
        """Rolling window aggregation producing a new column."""
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
        values: Columns,
        agg: str = "first",
        on_columns: tuple[str, ...] | None = None,
        separator: str = "_",
        sort_columns: bool = False,
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
    def explode(
        self, df: BackendFrameT, columns: Columns, *, outer: bool = False
    ) -> BackendFrameT: ...

    @abstractmethod
    def unnest(self, df: BackendFrameT, items: tuple[UnnestItem, ...]) -> BackendFrameT: ...

    @abstractmethod
    def posexplode(
        self,
        df: BackendFrameT,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        outer: bool = False,
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
    def collect(
        self, df: BackendFrameT, *, options: ExecutionOptions | None = None
    ) -> BackendFrameT: ...

    @abstractmethod
    def to_dicts(
        self, df: BackendFrameT, *, options: ExecutionOptions | None = None
    ) -> list[dict[str, object]]: ...

    @abstractmethod
    def to_dict(
        self, df: BackendFrameT, *, options: ExecutionOptions | None = None
    ) -> dict[str, list[object]]: ...

    async def acollect(
        self, df: BackendFrameT, *, options: ExecutionOptions | None = None
    ) -> BackendFrameT:
        """Materialize *df* asynchronously.

        Default: run :meth:`collect` in a worker thread via :func:`asyncio.to_thread`
        so synchronous backends do not block the event loop.

        Async-native backends (HTTP drivers, asyncio clients) should override this
        instead of blocking in :meth:`collect`.
        """

        return await asyncio.to_thread(self.collect, df, options=options)

    async def ato_dicts(
        self, df: BackendFrameT, *, options: ExecutionOptions | None = None
    ) -> list[dict[str, object]]:
        """Like :meth:`to_dicts`, but awaitable. Default uses :func:`asyncio.to_thread`."""

        return await asyncio.to_thread(self.to_dicts, df, options=options)

    async def ato_dict(
        self, df: BackendFrameT, *, options: ExecutionOptions | None = None
    ) -> dict[str, list[object]]:
        """Like :meth:`to_dict`, but awaitable. Default uses :func:`asyncio.to_thread`."""

        return await asyncio.to_thread(self.to_dict, df, options=options)

    def hint(
        self, df: BackendFrameT, *, hints: tuple[str, ...], kv: dict[str, object]
    ) -> BackendFrameT:
        """Optional execution hint hook (Spark-inspired).

        Default is a no-op. Backends that support such hints may override.
        """

        _ = hints, kv
        return df


# Backwards-compatible name for older imports.
BackendAdapter = BaseAdapter
