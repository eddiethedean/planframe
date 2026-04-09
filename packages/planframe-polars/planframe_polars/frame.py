from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Generic, Literal, TypeVar, cast

import polars as pl

from planframe.frame import Frame
from planframe.typing.storage import StorageOptions
from planframe_polars.adapter import PolarsAdapter, PolarsBackendFrame

SchemaT = TypeVar("SchemaT")

PolarsData = Mapping[str, Sequence[object]] | Sequence[Mapping[str, object]]


def _schema_defaults(schema: type[Any]) -> dict[str, object]:
    ann = dict(getattr(schema, "__dict__", {}).get("__annotations__", {}))
    out: dict[str, object] = {}
    for name in ann:
        if name in getattr(schema, "__dict__", {}):
            out[name] = getattr(schema, name)
    return out


def _fill_missing_from_defaults(data: PolarsData, *, defaults: dict[str, object]) -> PolarsData:
    if not defaults:
        return data

    if isinstance(data, Mapping):
        data_map = cast(Mapping[str, Sequence[object]], data)
        if not data_map:
            return dict(data_map)
        # Infer row count from the first column.
        first = next(iter(data_map.values()))
        n = len(first)
        out: dict[str, list[object]] = {k: list(v) for k, v in data_map.items()}
        for k, dv in defaults.items():
            if k not in out:
                out[k] = [dv] * n
        return out

    # list-of-dicts
    out_rows: list[dict[str, object]] = []
    for row in data:
        r = dict(row)
        for k, dv in defaults.items():
            if k not in r:
                r[k] = dv
        out_rows.append(r)
    return out_rows


def _to_polars_backend_frame(
    data: PolarsData, *, schema: type[Any], lazy: bool
) -> PolarsBackendFrame:
    defaults = _schema_defaults(schema)
    data2 = _fill_missing_from_defaults(data, defaults=defaults)
    df = pl.DataFrame(data2)  # type: ignore[arg-type]
    return df.lazy() if lazy else df


class _PolarsFrameMeta(type):
    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        # Allow normal dataclass construction when `Frame.source(...)` calls `cls(_data=..., ...)`.
        if "_data" in kwargs and "_adapter" in kwargs and "_plan" in kwargs and "_schema" in kwargs:
            return super().__call__(*args, **kwargs)

        data = args[0] if args else kwargs.pop("data")
        if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
            raise TypeError(
                "PolarsFrame constructors accept only Python data (dict-of-lists or list-of-dicts). "
                "Use `Frame.source(...)` for advanced usage."
            )
        lazy = kwargs.pop("lazy", True)
        if kwargs:
            raise TypeError(f"Unexpected constructor kwargs: {sorted(kwargs)}")
        df = _to_polars_backend_frame(data, schema=cls, lazy=lazy)
        # Type checkers can't always see `Frame`-style classmethods/attrs on metaclass `cls`.
        cls_any = cast(Any, cls)
        return cls_any.source(
            df,
            adapter=cls_any._adapter_singleton,
            schema=cast(type[SchemaT], cls),
        )


class PolarsFrame(
    Frame[SchemaT, PolarsBackendFrame, pl.Expr], Generic[SchemaT], metaclass=_PolarsFrameMeta
):
    """A PlanFrame `Frame` bound to the Polars backend."""

    _adapter_singleton: ClassVar[PolarsAdapter] = PolarsAdapter()
    __planframe_model__ = True

    @classmethod
    def scan_parquet(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> PolarsFrame[SchemaT]:
        lf = cls._adapter_singleton.reader.scan_parquet(
            path,
            hive_partitioning=hive_partitioning,
            storage_options=storage_options,
        )
        return cls.source(lf, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_parquet_dataset(
        cls,
        path_or_glob: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> PolarsFrame[SchemaT]:
        lf = cls._adapter_singleton.reader.scan_parquet_dataset(
            path_or_glob, storage_options=storage_options
        )
        return cls.source(lf, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_csv(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> PolarsFrame[SchemaT]:
        lf = cls._adapter_singleton.reader.scan_csv(path, storage_options=storage_options)
        return cls.source(lf, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_ndjson(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> PolarsFrame[SchemaT]:
        lf = cls._adapter_singleton.reader.scan_ndjson(path, storage_options=storage_options)
        return cls.source(lf, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_ipc(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> PolarsFrame[SchemaT]:
        lf = cls._adapter_singleton.reader.scan_ipc(
            path,
            hive_partitioning=hive_partitioning,
            storage_options=storage_options,
        )
        return cls.source(lf, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_delta(
        cls,
        source: str,
        *,
        schema: type[SchemaT],
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> PolarsFrame[SchemaT]:
        lf = cls._adapter_singleton.reader.scan_delta(
            source,
            version=version,
            storage_options=storage_options,
        )
        return cls.source(lf, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def read_delta(
        cls,
        source: str,
        *,
        schema: type[SchemaT],
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> PolarsFrame[SchemaT]:
        df = cls._adapter_singleton.reader.read_delta(
            source,
            version=version,
            storage_options=storage_options,
        )
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def read_excel(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        sheet_name: str | None = None,
    ) -> PolarsFrame[SchemaT]:
        df = cls._adapter_singleton.reader.read_excel(path, sheet_name=sheet_name)
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def read_avro(cls, path: str, *, schema: type[SchemaT]) -> PolarsFrame[SchemaT]:
        df = cls._adapter_singleton.reader.read_avro(path)
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def read_database(
        cls, query: str, *, connection: object, schema: type[SchemaT]
    ) -> PolarsFrame[SchemaT]:
        df = cls._adapter_singleton.reader.read_database(query, connection=connection)
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def read_database_uri(
        cls,
        query: str,
        *,
        uri: str,
        engine: Literal["connectorx", "adbc"] | None = None,
        schema: type[SchemaT],
    ) -> PolarsFrame[SchemaT]:
        df = cls._adapter_singleton.reader.read_database_uri(query, uri=uri, engine=engine)
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)
