"""Materialization, serialization, and model helpers."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any, Generic, Literal, Protocol, TypeVar

from planframe.backend.adapter import AdapterCapabilities
from planframe.backend.errors import PlanFrameExecutionError
from planframe.backend.io import AdapterRowStreamer
from planframe.execution_options import ExecutionOptions
from planframe.plan.nodes import PlanNode
from planframe.schema.ir import Schema
from planframe.schema.materialize import materialize_model
from planframe.typing.storage import StorageOptions

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


class _HasFrameIODeps(Protocol[BackendFrameT, BackendExprT]):
    # Defined as a Protocol so type checkers understand that this mixin requires
    # `FramePlanMixin`-provided internals, without needing a hard import dependency.
    _plan: PlanNode
    _schema: Schema
    _adapter: Any

    def _eval(self, node: object) -> BackendFrameT: ...

    def collect_backend(self, *, options: ExecutionOptions | None = None) -> BackendFrameT: ...

    async def acollect_backend(
        self, *, options: ExecutionOptions | None = None
    ) -> BackendFrameT: ...

    # Methods from `FrameIOMixin` itself. Declared so that helpers on this mixin can
    # call each other (e.g. `write_*` delegating to `sink_*`) without confusing type checkers.
    def sink_parquet(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4"],
        row_group_size: int | None,
        partition_by: tuple[str, ...] | None,
        storage_options: StorageOptions | None,
    ) -> None: ...

    def sink_csv(
        self,
        path: str,
        *,
        separator: str,
        include_header: bool,
        storage_options: StorageOptions | None,
    ) -> None: ...

    def sink_ndjson(self, path: str, *, storage_options: StorageOptions | None) -> None: ...

    def sink_ipc(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "lz4", "zstd"],
        storage_options: StorageOptions | None,
    ) -> None: ...

    def sink_database(
        self,
        table_name: str,
        *,
        connection: object,
        if_table_exists: Literal["fail", "replace", "append"],
        engine: str | None,
    ) -> None: ...

    def sink_excel(self, path: str, *, worksheet: str) -> None: ...

    def sink_delta(
        self,
        target: str,
        *,
        mode: Literal["error", "append", "overwrite", "ignore", "merge"],
        storage_options: StorageOptions | None,
    ) -> None: ...

    def sink_avro(
        self,
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "deflate"],
        name: str,
    ) -> None: ...

    def to_dicts(
        self,
        *,
        options: ExecutionOptions | None = None,
    ) -> list[dict[str, object]]: ...

    async def ato_dicts(
        self,
        *,
        options: ExecutionOptions | None = None,
    ) -> list[dict[str, object]]: ...

    async def acollect(
        self,
        *,
        name: str = ...,
        options: ExecutionOptions | None = ...,
    ) -> list[Any]: ...

    async def ato_dict(
        self,
        *,
        options: ExecutionOptions | None = ...,
    ) -> dict[str, list[object]]: ...

    async def collect_async(
        self,
        *,
        name: str = ...,
        options: ExecutionOptions | None = ...,
    ) -> list[Any]: ...

    async def collect_backend_async(
        self, *, options: ExecutionOptions | None = ...
    ) -> BackendFrameT: ...

    async def to_dicts_async(
        self, *, options: ExecutionOptions | None = ...
    ) -> list[dict[str, object]]: ...

    async def to_dict_async(
        self, *, options: ExecutionOptions | None = ...
    ) -> dict[str, list[object]]: ...

    def stream_dicts(
        self,
        *,
        options: ExecutionOptions | None = None,
    ) -> Iterator[dict[str, object]]: ...

    def astream_dicts(
        self,
        *,
        options: ExecutionOptions | None = None,
    ) -> AsyncIterator[dict[str, object]]: ...


class FrameIOMixin(Generic[SchemaT, BackendFrameT, BackendExprT]):
    """Collect, async materialization, and sink IO."""

    __slots__ = ()

    def collect_backend(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        options: ExecutionOptions | None = None,
    ) -> BackendFrameT:
        """Materialize and return the backend-native frame object."""

        try:
            planned = self._eval(self._plan)
            return self._adapter.collect(planned, options=options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(f"Backend collect failed for {self._adapter.name}") from e

    async def acollect_backend(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        options: ExecutionOptions | None = None,
    ) -> BackendFrameT:
        """Async materialization returning the backend-native frame object."""

        try:
            planned = self._eval(self._plan)
            return await self._adapter.acollect(planned, options=options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend acollect failed for {self._adapter.name}"
            ) from e

    def collect(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        name: str = "Row",
        options: ExecutionOptions | None = None,
    ) -> list[Any]:
        out = self.collect_backend(options=options)

        Model = materialize_model(name=name, schema=self._schema, kind="pydantic")
        try:
            rows = self._adapter.to_dicts(out, options=options)
            return [Model(**r) for r in rows]
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(f"Backend collect failed for {self._adapter.name}") from e

    async def acollect(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        name: str = "Row",
        options: ExecutionOptions | None = None,
    ) -> list[Any]:
        out = await self.acollect_backend(options=options)

        Model = materialize_model(name=name, schema=self._schema, kind="pydantic")
        try:
            rows = await self._adapter.ato_dicts(out, options=options)
            return [Model(**r) for r in rows]
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend acollect failed for {self._adapter.name}"
            ) from e

    def to_dicts(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        options: ExecutionOptions | None = None,
    ) -> list[dict[str, object]]:
        try:
            planned = self._eval(self._plan)
            return self._adapter.to_dicts(planned, options=options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend to_dicts failed for {self._adapter.name}"
            ) from e

    def to_dict(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        options: ExecutionOptions | None = None,
    ) -> dict[str, list[object]]:
        try:
            planned = self._eval(self._plan)
            return self._adapter.to_dict(planned, options=options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(f"Backend to_dict failed for {self._adapter.name}") from e

    def stream_dicts(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        options: ExecutionOptions | None = None,
    ) -> Iterator[dict[str, object]]:
        adapter = self._adapter
        if isinstance(adapter, AdapterRowStreamer):
            planned = self._eval(self._plan)
            yield from adapter.stream_dicts(planned, options=options)
            return
        yield from self.to_dicts(options=options)

    def astream_dicts(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        options: ExecutionOptions | None = None,
    ) -> AsyncIterator[dict[str, object]]:
        adapter = self._adapter
        if isinstance(adapter, AdapterRowStreamer):
            planned = self._eval(self._plan)
            return adapter.astream_dicts(planned, options=options)

        async def _fallback() -> AsyncIterator[dict[str, object]]:
            for row in await self.ato_dicts(options=options):
                yield row

        return _fallback()

    def stream(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        name: str = "Row",
        options: ExecutionOptions | None = None,
    ) -> Iterator[Any]:
        Model = materialize_model(name=name, schema=self._schema, kind="pydantic")
        for r in self.stream_dicts(options=options):
            yield Model(**r)

    async def astream(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        name: str = "Row",
        options: ExecutionOptions | None = None,
    ) -> AsyncIterator[Any]:
        Model = materialize_model(name=name, schema=self._schema, kind="pydantic")

        async for r in self.astream_dicts(options=options):
            yield Model(**r)

    async def ato_dicts(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        options: ExecutionOptions | None = None,
    ) -> list[dict[str, object]]:
        try:
            planned = self._eval(self._plan)
            return await self._adapter.ato_dicts(planned, options=options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend ato_dicts failed for {self._adapter.name}"
            ) from e

    async def ato_dict(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        options: ExecutionOptions | None = None,
    ) -> dict[str, list[object]]:
        try:
            planned = self._eval(self._plan)
            return await self._adapter.ato_dict(planned, options=options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend ato_dict failed for {self._adapter.name}"
            ) from e

    async def collect_async(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        name: str = "Row",
        options: ExecutionOptions | None = None,
    ) -> list[Any]:
        """Alias for :meth:`acollect` (async materialize rows as schema models)."""

        return await self.acollect(name=name, options=options)

    async def collect_backend_async(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        options: ExecutionOptions | None = None,
    ) -> BackendFrameT:
        """Alias for :meth:`acollect_backend` (async materialize backend-native frame)."""

        return await self.acollect_backend(options=options)

    async def to_dicts_async(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        options: ExecutionOptions | None = None,
    ) -> list[dict[str, object]]:
        """Alias for :meth:`ato_dicts`."""

        return await self.ato_dicts(options=options)

    async def to_dict_async(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        *,
        options: ExecutionOptions | None = None,
    ) -> dict[str, list[object]]:
        """Alias for :meth:`ato_dict`."""

        return await self.ato_dict(options=options)

    def write_parquet(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4"] = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: StorageOptions | None = None,
    ) -> None:
        self.sink_parquet(
            path,
            compression=compression,
            row_group_size=row_group_size,
            partition_by=partition_by,
            storage_options=storage_options,
        )

    def write_csv(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: StorageOptions | None = None,
    ) -> None:
        self.sink_csv(
            path,
            separator=separator,
            include_header=include_header,
            storage_options=storage_options,
        )

    def write_ndjson(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        path: str,
        *,
        storage_options: StorageOptions | None = None,
    ) -> None:
        self.sink_ndjson(path, storage_options=storage_options)

    def write_ipc(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        path: str,
        *,
        compression: Literal["uncompressed", "lz4", "zstd"] = "uncompressed",
        storage_options: StorageOptions | None = None,
    ) -> None:
        self.sink_ipc(path, compression=compression, storage_options=storage_options)

    def write_database(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        table_name: str,
        *,
        connection: object,
        if_table_exists: Literal["fail", "replace", "append"] = "fail",
        engine: str | None = None,
    ) -> None:
        self.sink_database(
            table_name,
            connection=connection,
            if_table_exists=if_table_exists,
            engine=engine,
        )

    def write_excel(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT], path: str, *, worksheet: str = "Sheet1"
    ) -> None:
        self.sink_excel(path, worksheet=worksheet)

    def write_delta(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        target: str,
        *,
        mode: Literal["error", "append", "overwrite", "ignore", "merge"] = "error",
        storage_options: StorageOptions | None = None,
    ) -> None:
        self.sink_delta(target, mode=mode, storage_options=storage_options)

    def write_avro(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "deflate"] = "uncompressed",
        name: str = "",
    ) -> None:
        self.sink_avro(path, compression=compression, name=name)

    def sink_parquet(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4"] = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: StorageOptions | None = None,
    ) -> None:
        try:
            caps: AdapterCapabilities = self._adapter.capabilities
            if storage_options is not None and not caps.storage_options:
                raise PlanFrameExecutionError(
                    f"Backend sink_parquet does not support storage_options for {self._adapter.name} "
                    "(capabilities.storage_options=False)"
                )
            planned = self._eval(self._plan)
            self._adapter.writer.sink_parquet(
                planned,
                path,
                compression=compression,
                row_group_size=row_group_size,
                partition_by=partition_by,
                storage_options=storage_options,
            )
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend sink_parquet failed for {self._adapter.name}"
            ) from e

    def sink_csv(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: StorageOptions | None = None,
    ) -> None:
        try:
            caps: AdapterCapabilities = self._adapter.capabilities
            if storage_options is not None and not caps.storage_options:
                raise PlanFrameExecutionError(
                    f"Backend sink_csv does not support storage_options for {self._adapter.name} "
                    "(capabilities.storage_options=False)"
                )
            planned = self._eval(self._plan)
            self._adapter.writer.sink_csv(
                planned,
                path,
                separator=separator,
                include_header=include_header,
                storage_options=storage_options,
            )
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend sink_csv failed for {self._adapter.name}"
            ) from e

    def sink_ndjson(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        path: str,
        *,
        storage_options: StorageOptions | None = None,
    ) -> None:
        try:
            caps: AdapterCapabilities = self._adapter.capabilities
            if storage_options is not None and not caps.storage_options:
                raise PlanFrameExecutionError(
                    f"Backend sink_ndjson does not support storage_options for {self._adapter.name} "
                    "(capabilities.storage_options=False)"
                )
            planned = self._eval(self._plan)
            self._adapter.writer.sink_ndjson(planned, path, storage_options=storage_options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend sink_ndjson failed for {self._adapter.name}"
            ) from e

    def sink_ipc(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        path: str,
        *,
        compression: Literal["uncompressed", "lz4", "zstd"] = "uncompressed",
        storage_options: StorageOptions | None = None,
    ) -> None:
        try:
            caps: AdapterCapabilities = self._adapter.capabilities
            if storage_options is not None and not caps.storage_options:
                raise PlanFrameExecutionError(
                    f"Backend sink_ipc does not support storage_options for {self._adapter.name} "
                    "(capabilities.storage_options=False)"
                )
            planned = self._eval(self._plan)
            self._adapter.writer.sink_ipc(
                planned, path, compression=compression, storage_options=storage_options
            )
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend sink_ipc failed for {self._adapter.name}"
            ) from e

    def sink_database(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        table_name: str,
        *,
        connection: object,
        if_table_exists: Literal["fail", "replace", "append"] = "fail",
        engine: str | None = None,
    ) -> None:
        try:
            caps: AdapterCapabilities = self._adapter.capabilities
            if not caps.sink_database:
                raise PlanFrameExecutionError(
                    f"Backend sink_database is not supported for {self._adapter.name} "
                    "(capabilities.sink_database=False)"
                )
            planned = self._eval(self._plan)
            self._adapter.writer.sink_database(
                planned,
                table_name=table_name,
                connection=connection,
                if_table_exists=if_table_exists,
                engine=engine,
            )
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend sink_database failed for {self._adapter.name}"
            ) from e

    def sink_excel(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT], path: str, *, worksheet: str = "Sheet1"
    ) -> None:
        try:
            caps: AdapterCapabilities = self._adapter.capabilities
            if not caps.sink_excel:
                raise PlanFrameExecutionError(
                    f"Backend sink_excel is not supported for {self._adapter.name} "
                    "(capabilities.sink_excel=False)"
                )
            planned = self._eval(self._plan)
            self._adapter.writer.sink_excel(planned, path, worksheet=worksheet)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend sink_excel failed for {self._adapter.name}"
            ) from e

    def sink_delta(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        target: str,
        *,
        mode: Literal["error", "append", "overwrite", "ignore", "merge"] = "error",
        storage_options: StorageOptions | None = None,
    ) -> None:
        try:
            caps: AdapterCapabilities = self._adapter.capabilities
            if not caps.sink_delta:
                raise PlanFrameExecutionError(
                    f"Backend sink_delta is not supported for {self._adapter.name} "
                    "(capabilities.sink_delta=False)"
                )
            if storage_options is not None and not caps.storage_options:
                raise PlanFrameExecutionError(
                    f"Backend sink_delta does not support storage_options for {self._adapter.name} "
                    "(capabilities.storage_options=False)"
                )
            planned = self._eval(self._plan)
            self._adapter.writer.sink_delta(
                planned, target, mode=mode, storage_options=storage_options
            )
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend sink_delta failed for {self._adapter.name}"
            ) from e

    def sink_avro(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        path: str,
        *,
        compression: Literal["uncompressed", "snappy", "deflate"] = "uncompressed",
        name: str = "",
    ) -> None:
        try:
            caps: AdapterCapabilities = self._adapter.capabilities
            if not caps.sink_avro:
                raise PlanFrameExecutionError(
                    f"Backend sink_avro is not supported for {self._adapter.name} "
                    "(capabilities.sink_avro=False)"
                )
            planned = self._eval(self._plan)
            self._adapter.writer.sink_avro(planned, path, compression=compression, name=name)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend sink_avro failed for {self._adapter.name}"
            ) from e

    def materialize_model(
        self: _HasFrameIODeps[BackendFrameT, BackendExprT],
        name: str,
        *,
        kind: Literal["dataclass", "pydantic"] = "dataclass",
    ) -> type[Any]:
        return materialize_model(name=name, schema=self._schema, kind=kind)
