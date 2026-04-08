from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, TypeVar

import polars as pl

from planframe.frame import Frame
from planframe.typing.storage import StorageOptions

SchemaT = TypeVar("SchemaT")

PolarsBackendFrame = pl.DataFrame | pl.LazyFrame

class PolarsFrame(Frame[Any, PolarsBackendFrame, pl.Expr]):
    def __new__(
        cls,
        data: Mapping[str, Sequence[object]] | Sequence[Mapping[str, object]],
        *,
        lazy: bool = ...,
    ) -> PolarsFrame: ...
    @classmethod
    def scan_parquet(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        hive_partitioning: bool | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrame: ...
    @classmethod
    def scan_parquet_dataset(
        cls,
        path_or_glob: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrame: ...
    @classmethod
    def scan_csv(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrame: ...
    @classmethod
    def scan_ndjson(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrame: ...
    @classmethod
    def scan_ipc(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        hive_partitioning: bool | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrame: ...
    @classmethod
    def scan_delta(
        cls,
        source: str,
        *,
        schema: type[SchemaT],
        version: int | str | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrame: ...
    @classmethod
    def read_delta(
        cls,
        source: str,
        *,
        schema: type[SchemaT],
        version: int | str | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> PolarsFrame: ...
    @classmethod
    def read_excel(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        sheet_name: str | None = ...,
    ) -> PolarsFrame: ...
    @classmethod
    def read_avro(cls, path: str, *, schema: type[SchemaT]) -> PolarsFrame: ...
    @classmethod
    def read_database(
        cls, query: str, *, connection: object, schema: type[SchemaT]
    ) -> PolarsFrame: ...
    @classmethod
    def read_database_uri(
        cls,
        query: str,
        *,
        uri: str,
        engine: Literal["connectorx", "adbc"] | None = ...,
        schema: type[SchemaT],
    ) -> PolarsFrame: ...
