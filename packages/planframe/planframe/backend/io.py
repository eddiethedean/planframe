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
