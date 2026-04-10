from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, TypeVar

import polars as pl

from planframe import Frame
from planframe.typing import StorageOptions

SchemaT = TypeVar("SchemaT")
PolarsFrameT = TypeVar("PolarsFrameT", bound="PolarsFrame")

PolarsBackendFrame = pl.DataFrame | pl.LazyFrame

class PolarsFrame(Frame[Any, PolarsBackendFrame, pl.Expr]):
    def __init__(
        self: PolarsFrameT,
        data: Mapping[str, Sequence[object]] | Sequence[Mapping[str, object]],
        *,
        lazy: bool = ...,
    ) -> None: ...
    @classmethod
    def scan_parquet(
        cls: type[PolarsFrameT],
        path: str,
        *,
        schema: type[SchemaT],
        hive_partitioning: bool | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrameT: ...
    @classmethod
    def scan_parquet_dataset(
        cls: type[PolarsFrameT],
        path_or_glob: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrameT: ...
    @classmethod
    def scan_csv(
        cls: type[PolarsFrameT],
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrameT: ...
    @classmethod
    def scan_ndjson(
        cls: type[PolarsFrameT],
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrameT: ...
    @classmethod
    def scan_ipc(
        cls: type[PolarsFrameT],
        path: str,
        *,
        schema: type[SchemaT],
        hive_partitioning: bool | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrameT: ...
    @classmethod
    def scan_delta(
        cls: type[PolarsFrameT],
        source: str,
        *,
        schema: type[SchemaT],
        version: int | str | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrameT: ...
    @classmethod
    def read_delta(
        cls: type[PolarsFrameT],
        source: str,
        *,
        schema: type[SchemaT],
        version: int | str | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrameT: ...
    @classmethod
    def read_excel(
        cls: type[PolarsFrameT],
        path: str,
        *,
        schema: type[SchemaT],
        sheet_name: str | None = ...,
    ) -> PolarsFrameT: ...
    @classmethod
    def read_avro(cls: type[PolarsFrameT], path: str, *, schema: type[SchemaT]) -> PolarsFrameT: ...
    @classmethod
    def read_database(
        cls: type[PolarsFrameT], query: str, *, connection: object, schema: type[SchemaT]
    ) -> PolarsFrameT: ...
    @classmethod
    def read_database_uri(
        cls: type[PolarsFrameT],
        query: str,
        *,
        uri: str,
        engine: Literal["connectorx", "adbc"] | None = ...,
        schema: type[SchemaT],
    ) -> PolarsFrameT: ...
