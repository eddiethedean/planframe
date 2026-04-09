from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, TypeVar

import pandas as pd

from planframe.pandas import PandasLikeFrame
from planframe.typing.storage import StorageOptions

SchemaT = TypeVar("SchemaT")
PandasFrameT = TypeVar("PandasFrameT", bound="PandasFrame")

PandasBackendFrame = pd.DataFrame

class PandasFrame(PandasLikeFrame[Any, PandasBackendFrame, object]):
    def __init__(
        self: PandasFrameT,
        data: Mapping[str, Sequence[object]] | Sequence[Mapping[str, object]],
    ) -> None: ...
    @classmethod
    def scan_parquet(
        cls: type[PandasFrameT],
        path: str,
        *,
        schema: type[SchemaT],
        hive_partitioning: bool | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> PandasFrameT: ...
    @classmethod
    def scan_parquet_dataset(
        cls: type[PandasFrameT],
        path_or_glob: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = ...,
    ) -> PandasFrameT: ...
    @classmethod
    def scan_csv(
        cls: type[PandasFrameT],
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = ...,
    ) -> PandasFrameT: ...
    @classmethod
    def scan_ndjson(
        cls: type[PandasFrameT],
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = ...,
    ) -> PandasFrameT: ...
    @classmethod
    def scan_ipc(
        cls: type[PandasFrameT],
        path: str,
        *,
        schema: type[SchemaT],
        hive_partitioning: bool | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> PandasFrameT: ...
    @classmethod
    def scan_delta(
        cls: type[PandasFrameT],
        source: str,
        *,
        schema: type[SchemaT],
        version: int | str | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> PandasFrameT: ...
    @classmethod
    def read_delta(
        cls: type[PandasFrameT],
        source: str,
        *,
        schema: type[SchemaT],
        version: int | str | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> PandasFrameT: ...
    @classmethod
    def read_excel(
        cls: type[PandasFrameT],
        path: str,
        *,
        schema: type[SchemaT],
        sheet_name: str | None = ...,
    ) -> PandasFrameT: ...
    @classmethod
    def read_avro(
        cls: type[PandasFrameT],
        path: str,
        *,
        schema: type[SchemaT],
    ) -> PandasFrameT: ...
    @classmethod
    def read_database(
        cls: type[PandasFrameT],
        query: str,
        *,
        connection: object,
        schema: type[SchemaT],
    ) -> PandasFrameT: ...
    @classmethod
    def read_database_uri(
        cls: type[PandasFrameT],
        query: str,
        *,
        uri: str,
        engine: Literal["connectorx", "adbc"] | None = ...,
        schema: type[SchemaT],
    ) -> PandasFrameT: ...
