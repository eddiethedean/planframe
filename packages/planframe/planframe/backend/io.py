from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Literal, Protocol, TypeVar, runtime_checkable

from planframe.execution_options import ExecutionOptions
from planframe.typing.storage import StorageOptions

BackendFrameT = TypeVar("BackendFrameT")


class AdapterReader(Protocol[BackendFrameT]):
    """Adapter-owned IO reader surface.

    These methods are used by backend-bound Frame classes (`PolarsFrame`, `PandasFrame`, ...)
    to standardize IO entrypoints while allowing each adapter to define behavior.
    """

    # ---- File/lake scans (prefer lazy where supported) ----
    def scan_parquet(
        self,
        path: str,
        *,
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT: ...

    def scan_parquet_dataset(
        self,
        path_or_glob: str,
        *,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT: ...

    def scan_csv(
        self, path: str, *, storage_options: StorageOptions | None = None
    ) -> BackendFrameT: ...

    def scan_ndjson(
        self, path: str, *, storage_options: StorageOptions | None = None
    ) -> BackendFrameT: ...

    def scan_ipc(
        self,
        path: str,
        *,
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT: ...

    def scan_delta(
        self,
        source: str,
        *,
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT: ...

    # ---- Eager reads (even if backend is lazy-first) ----
    def read_delta(
        self,
        source: str,
        *,
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT: ...

    def read_excel(
        self,
        path: str,
        *,
        sheet_name: str | None = None,
    ) -> BackendFrameT: ...

    def read_avro(self, path: str) -> BackendFrameT: ...

    def read_database(self, query: str, *, connection: object) -> BackendFrameT: ...

    def read_database_uri(
        self,
        query: str,
        *,
        uri: str,
        engine: Literal["connectorx", "adbc"] | None = None,
    ) -> BackendFrameT: ...


class AdapterWriter(Protocol[BackendFrameT]):
    """Adapter-owned IO writer surface.

    These methods are used by core `FrameIOMixin.sink_*` methods.
    """

    def sink_parquet(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: StorageOptions | None = None,
    ) -> None: ...

    def sink_csv(
        self,
        df: BackendFrameT,
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: StorageOptions | None = None,
    ) -> None: ...

    def sink_ndjson(
        self, df: BackendFrameT, path: str, *, storage_options: StorageOptions | None = None
    ) -> None: ...

    def sink_ipc(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "uncompressed",
        storage_options: StorageOptions | None = None,
    ) -> None: ...

    def sink_database(
        self,
        df: BackendFrameT,
        *,
        table_name: str,
        connection: object,
        if_table_exists: str = "fail",
        engine: str | None = None,
    ) -> None: ...

    def sink_excel(self, df: BackendFrameT, path: str, *, worksheet: str = "Sheet1") -> None: ...

    def sink_delta(
        self,
        df: BackendFrameT,
        target: str,
        *,
        mode: str = "error",
        storage_options: StorageOptions | None = None,
    ) -> None: ...

    def sink_avro(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "uncompressed",
        name: str = "",
    ) -> None: ...


class AdapterAsyncReader(Protocol[BackendFrameT]):
    """Async variant of `AdapterReader`."""

    async def scan_parquet(
        self,
        path: str,
        *,
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT: ...

    async def scan_parquet_dataset(
        self,
        path_or_glob: str,
        *,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT: ...

    async def scan_csv(
        self, path: str, *, storage_options: StorageOptions | None = None
    ) -> BackendFrameT: ...

    async def scan_ndjson(
        self, path: str, *, storage_options: StorageOptions | None = None
    ) -> BackendFrameT: ...

    async def scan_ipc(
        self,
        path: str,
        *,
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT: ...

    async def scan_delta(
        self,
        source: str,
        *,
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT: ...

    async def read_delta(
        self,
        source: str,
        *,
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> BackendFrameT: ...

    async def read_excel(
        self,
        path: str,
        *,
        sheet_name: str | None = None,
    ) -> BackendFrameT: ...

    async def read_avro(self, path: str) -> BackendFrameT: ...

    async def read_database(self, query: str, *, connection: object) -> BackendFrameT: ...

    async def read_database_uri(
        self,
        query: str,
        *,
        uri: str,
        engine: Literal["connectorx", "adbc"] | None = None,
    ) -> BackendFrameT: ...


class AdapterAsyncWriter(Protocol[BackendFrameT]):
    """Async variant of `AdapterWriter`."""

    async def sink_parquet(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: StorageOptions | None = None,
    ) -> None: ...

    async def sink_csv(
        self,
        df: BackendFrameT,
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: StorageOptions | None = None,
    ) -> None: ...

    async def sink_ndjson(
        self, df: BackendFrameT, path: str, *, storage_options: StorageOptions | None = None
    ) -> None: ...

    async def sink_ipc(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "uncompressed",
        storage_options: StorageOptions | None = None,
    ) -> None: ...

    async def sink_database(
        self,
        df: BackendFrameT,
        *,
        table_name: str,
        connection: object,
        if_table_exists: str = "fail",
        engine: str | None = None,
    ) -> None: ...

    async def sink_excel(
        self, df: BackendFrameT, path: str, *, worksheet: str = "Sheet1"
    ) -> None: ...

    async def sink_delta(
        self,
        df: BackendFrameT,
        target: str,
        *,
        mode: str = "error",
        storage_options: StorageOptions | None = None,
    ) -> None: ...

    async def sink_avro(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "uncompressed",
        name: str = "",
    ) -> None: ...


@runtime_checkable
class AdapterRowStreamer(Protocol[BackendFrameT]):
    """Optional adapter hook for true row streaming.

    **Contract:** implement **both** ``stream_dicts`` *and* ``astream_dicts``.
    PlanFrame uses ``isinstance(..., AdapterRowStreamer)``; adapters that only implement
    the sync iterator are **not** treated as streamers and ``Frame.stream_dicts`` /
    ``Frame.astream_dicts`` fall back to materializing via ``to_dicts`` / ``ato_dicts``.
    """

    def stream_dicts(
        self, df: BackendFrameT, *, options: ExecutionOptions | None = None
    ) -> Iterator[dict[str, object]]: ...

    def astream_dicts(
        self, df: BackendFrameT, *, options: ExecutionOptions | None = None
    ) -> AsyncIterator[dict[str, object]]: ...


@runtime_checkable
class AdapterColumnarStreamer(Protocol[BackendFrameT]):
    """Optional adapter surface for **chunked columnar** export (design spike for 1.3+).

    Each chunk is a columnar mapping ``dict[column_name, list[values]]`` where every value
    list has the **same length** (rows in that chunk). Chunk boundaries are adapter-defined
    (e.g. engine batch size). Column names should be consistent across chunks for a given
    materialization.

    This is **not** the same as :class:`AdapterRowStreamer`, which streams **rows**
    (``dict[str, object]`` per row) and is integrated with ``Frame.stream_dicts`` /
    ``Frame.astream_dicts``. Columnar chunking is for hosts that want to build Arrow tables,
    batched numpy/Pandas loads, etc., without holding a full ``dict[str, list[object]]`` in
    memory.

    **Integration status:** PlanFrame core does **not** yet call this protocol from
    :func:`planframe.materialize.materialize_columns` or ``Frame.to_dict``. Adapters may
    implement it so hosts can ``isinstance(adapter, AdapterColumnarStreamer)`` and call
    the iterators after ``collect`` / ``acollect``, forwarding the same
    :class:`~planframe.execution_options.ExecutionOptions` you would pass to ``to_dict``.
    Use ``streaming`` / ``engine_streaming`` hints the same way as for other materializers.

    **Contract:** if you claim support, implement **both** :meth:`iter_columnar_chunks` and
    :meth:`aiter_columnar_chunks` (mirroring :class:`AdapterRowStreamer`).

    See the PlanFrame design note *Columnar streaming* (``docs/planframe/design/columnar-streaming.md``).
    """

    def iter_columnar_chunks(
        self, df: BackendFrameT, *, options: ExecutionOptions | None = None
    ) -> Iterator[dict[str, list[object]]]: ...

    def aiter_columnar_chunks(
        self, df: BackendFrameT, *, options: ExecutionOptions | None = None
    ) -> AsyncIterator[dict[str, list[object]]]: ...
