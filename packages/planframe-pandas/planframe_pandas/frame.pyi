from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, NoReturn, TypeVar

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
    def read_parquet(
        cls: type[PandasFrameT],
        path: str,
        *,
        schema: type[SchemaT],
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
    def read_csv(
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
    def read_json(
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
    def to_csv(
        self,
        path: str,
        *,
        sep: str = ...,
        header: bool = ...,
        storage_options: StorageOptions | None = ...,
    ) -> None: ...
    def to_parquet(
        self,
        path: str,
        *,
        compression: str = ...,
        row_group_size: int | None = ...,
        partition_cols: tuple[str, ...] | None = ...,
        storage_options: StorageOptions | None = ...,
    ) -> None: ...

    # Core/Polars-style verbs are blocked on the pandas backend package.
    def select(self, *_: object, **__: object) -> NoReturn: ...
    def with_columns(self, *_: object, **__: object) -> NoReturn: ...
    def with_row_index(self, *_: object, **__: object) -> NoReturn: ...
    def drop_nulls(self, *_: object, **__: object) -> NoReturn: ...
    def drop_nulls_all(self, *_: object, **__: object) -> NoReturn: ...
    def unpivot(self, *_: object, **__: object) -> NoReturn: ...
    def vstack(self, *_: object, **__: object) -> NoReturn: ...
    def hstack(self, *_: object, **__: object) -> NoReturn: ...
    def sort(self, *_: object, **__: object) -> NoReturn: ...
    def join(self, *_: object, **__: object) -> NoReturn: ...
    def unique(self, *_: object, **__: object) -> NoReturn: ...
    def duplicated(self, *_: object, **__: object) -> NoReturn: ...
