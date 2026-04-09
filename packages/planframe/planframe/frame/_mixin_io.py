"""Materialization, serialization, and model helpers."""

from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar

from planframe.backend.errors import PlanFrameExecutionError
from planframe.execution_options import ExecutionOptions
from planframe.schema.materialize import materialize_model
from planframe.typing.storage import StorageOptions

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


class FrameIOMixin(Generic[SchemaT, BackendFrameT, BackendExprT]):
    """Collect, async materialization, and sink IO."""

    __slots__ = ()

    def collect(
        self,
        *,
        kind: Literal["dataclass", "pydantic"] | None = None,
        name: str = "Row",
        options: ExecutionOptions | None = None,
    ) -> BackendFrameT | list[Any]:
        try:
            planned = self._eval(self._plan)
            out = self._adapter.collect(planned, options=options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(f"Backend collect failed for {self._adapter.name}") from e

        if kind is None:
            return out

        # Build row models from the derived schema.
        Model = materialize_model(name=name, schema=self._schema, kind=kind)
        try:
            rows = self._adapter.to_dicts(out, options=options)
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
        options: ExecutionOptions | None = None,
    ) -> BackendFrameT | list[Any]:
        try:
            planned = self._eval(self._plan)
            out = await self._adapter.acollect(planned, options=options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend acollect failed for {self._adapter.name}"
            ) from e

        if kind is None:
            return out

        Model = materialize_model(name=name, schema=self._schema, kind=kind)
        try:
            rows = self._adapter.to_dicts(out, options=options)
            return [Model(**r) for r in rows]
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend acollect(kind={kind!r}) failed for {self._adapter.name}"
            ) from e

    def to_dicts(self, *, options: ExecutionOptions | None = None) -> list[dict[str, object]]:
        try:
            planned = self._eval(self._plan)
            return self._adapter.to_dicts(planned, options=options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend to_dicts failed for {self._adapter.name}"
            ) from e

    def to_dict(self, *, options: ExecutionOptions | None = None) -> dict[str, list[object]]:
        try:
            planned = self._eval(self._plan)
            return self._adapter.to_dict(planned, options=options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(f"Backend to_dict failed for {self._adapter.name}") from e

    async def ato_dicts(
        self, *, options: ExecutionOptions | None = None
    ) -> list[dict[str, object]]:
        try:
            planned = self._eval(self._plan)
            return await self._adapter.ato_dicts(planned, options=options)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameExecutionError(
                f"Backend ato_dicts failed for {self._adapter.name}"
            ) from e

    async def ato_dict(self, *, options: ExecutionOptions | None = None) -> dict[str, list[object]]:
        try:
            planned = self._eval(self._plan)
            return await self._adapter.ato_dict(planned, options=options)
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
