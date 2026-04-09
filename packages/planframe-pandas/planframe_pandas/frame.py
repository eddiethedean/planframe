from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Generic, Literal, NoReturn, TypeVar, cast

import pandas as pd

from planframe.pandas import PandasLikeFrame
from planframe.typing.storage import StorageOptions
from planframe_pandas.adapter import PandasAdapter, PandasBackendExpr, PandasBackendFrame

PandasData = Mapping[str, Sequence[object]] | Sequence[Mapping[str, object]]


def _schema_defaults(schema: type[Any]) -> dict[str, object]:
    ann = dict(getattr(schema, "__dict__", {}).get("__annotations__", {}))
    out: dict[str, object] = {}
    for name in ann:
        if name in getattr(schema, "__dict__", {}):
            out[name] = getattr(schema, name)
    return out


def _fill_missing_from_defaults(data: PandasData, *, defaults: dict[str, object]) -> PandasData:
    if not defaults:
        return data

    if isinstance(data, Mapping):
        data_map = cast(Mapping[str, Sequence[object]], data)
        if not data_map:
            return dict(data_map)
        first = next(iter(data_map.values()))
        n = len(first)
        out: dict[str, list[object]] = {k: list(v) for k, v in data_map.items()}
        for k, dv in defaults.items():
            if k not in out:
                out[k] = [dv] * n
        return out

    out_rows: list[dict[str, object]] = []
    for row in data:
        r = dict(row)
        for k, dv in defaults.items():
            if k not in r:
                r[k] = dv
        out_rows.append(r)
    return out_rows


def _to_pandas_df(data: object, *, schema: type[Any]) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        raise TypeError(
            "PandasFrame constructors accept only Python data (dict-of-lists or list-of-dicts). "
            "Use `Frame.source(...)` for advanced usage."
        )
    defaults = _schema_defaults(schema)
    if isinstance(data, list):
        data2 = _fill_missing_from_defaults(cast(PandasData, data), defaults=defaults)
        return pd.DataFrame.from_records(cast(list[dict[str, object]], data2))
    if isinstance(data, dict):
        data2 = _fill_missing_from_defaults(cast(PandasData, data), defaults=defaults)
        return pd.DataFrame(cast(dict[str, list[object]], data2))
    raise TypeError("PandasFrame expects dict-of-lists or list-of-dicts")


class _PandasFrameMeta(type):
    def __call__(cls, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        # Allow normal construction when `Frame.source(...)` calls `cls(_data=..., ...)`.
        if "_data" in kwargs and "_adapter" in kwargs and "_plan" in kwargs and "_schema" in kwargs:
            return super().__call__(*args, **kwargs)

        data = args[0] if args else kwargs.pop("data")
        if kwargs:
            raise TypeError(f"Unexpected constructor kwargs: {sorted(kwargs)}")
        df = _to_pandas_df(data, schema=cast(type[Any], cls))
        # Type checkers can't always see `Frame`-style classmethods/attrs on metaclass `cls`.
        cls_any = cast(Any, cls)
        return cls_any.source(
            df,
            adapter=cls_any._adapter_singleton,
            schema=cast(type[Any], cls),
        )


SchemaT = TypeVar("SchemaT")


class PandasFrame(
    PandasLikeFrame[SchemaT, PandasBackendFrame, PandasBackendExpr],
    Generic[SchemaT],
    metaclass=_PandasFrameMeta,
):
    """A PlanFrame `Frame` bound to the pandas backend."""

    _adapter_singleton: ClassVar[PandasAdapter] = PandasAdapter()
    __planframe_model__ = True

    # ---- pandas-only runtime UI ----
    #
    # PandasFrame is intentionally a pandas-flavored surface. The core/Polars-style
    # verbs are still available on the underlying plan engine, but are blocked on
    # this backend package to keep the runtime API aligned with pandas.

    def select(self, *_: object, **__: object) -> NoReturn:
        raise NotImplementedError(
            'PandasFrame.select is not supported. Use df[["col", ...]] or df.filter(...).'
        )

    def with_columns(self, *_: object, **__: object) -> NoReturn:
        raise NotImplementedError(
            "PandasFrame.with_columns is not supported. Use .assign(...) instead."
        )

    def with_row_index(self, *_: object, **__: object) -> NoReturn:
        raise NotImplementedError("PandasFrame.with_row_index is not supported in the pandas UI.")

    def drop_nulls(self, *_: object, **__: object) -> NoReturn:
        raise NotImplementedError(
            "PandasFrame.drop_nulls is not supported. Use .dropna(...) instead."
        )

    def drop_nulls_all(self, *_: object, **__: object) -> NoReturn:
        raise NotImplementedError(
            "PandasFrame.drop_nulls_all is not supported. Use .dropna(how='all', ...) instead."
        )

    def unpivot(self, *_: object, **__: object) -> NoReturn:
        raise NotImplementedError("PandasFrame.unpivot is not supported. Use .melt(...) instead.")

    def vstack(self, *_: object, **__: object) -> NoReturn:
        raise NotImplementedError("PandasFrame.vstack is not supported in the pandas UI.")

    def hstack(self, *_: object, **__: object) -> NoReturn:
        raise NotImplementedError("PandasFrame.hstack is not supported in the pandas UI.")

    def sort(self, *_: object, **__: object) -> NoReturn:
        raise NotImplementedError(
            "PandasFrame.sort is not supported. Use .sort_values(...) instead."
        )

    def join(self, *_: object, **__: object) -> NoReturn:
        raise NotImplementedError("PandasFrame.join is not supported. Use .merge(...) instead.")

    def unique(self, *_: object, **__: object) -> NoReturn:
        raise NotImplementedError(
            "PandasFrame.unique is not supported. Use .drop_duplicates(...) instead."
        )

    def duplicated(self, *_: object, **__: object) -> NoReturn:
        raise NotImplementedError(
            "PandasFrame.duplicated is not supported. Use .duplicated(...) (pandas UI) instead."
        )

    # ---- pandas-style IO aliases ----
    @classmethod
    def read_csv(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        return cls.scan_csv(path, schema=schema, storage_options=storage_options)

    @classmethod
    def read_parquet(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        return cls.scan_parquet(path, schema=schema, storage_options=storage_options)

    @classmethod
    def read_json(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        # NDJSON / JSON-lines
        return cls.scan_ndjson(path, schema=schema, storage_options=storage_options)

    def to_csv(
        self,
        path: str,
        *,
        sep: str = ",",
        header: bool = True,
        storage_options: StorageOptions | None = None,
    ) -> None:
        self.sink_csv(path, separator=sep, include_header=header, storage_options=storage_options)

    def to_parquet(
        self,
        path: str,
        *,
        compression: str = "zstd",
        row_group_size: int | None = None,
        partition_cols: tuple[str, ...] | None = None,
        storage_options: StorageOptions | None = None,
    ) -> None:
        self.sink_parquet(
            path,
            compression=cast(Any, compression),
            row_group_size=row_group_size,
            partition_by=cast(Any, partition_cols),
            storage_options=storage_options,
        )

    @classmethod
    def scan_parquet(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        df = cls._adapter_singleton.reader.scan_parquet(
            path,
            hive_partitioning=hive_partitioning,
            storage_options=storage_options,
        )
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_parquet_dataset(
        cls,
        path_or_glob: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        df = cls._adapter_singleton.reader.scan_parquet_dataset(
            path_or_glob, storage_options=storage_options
        )
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_csv(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        df = cls._adapter_singleton.reader.scan_csv(path, storage_options=storage_options)
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_ndjson(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        df = cls._adapter_singleton.reader.scan_ndjson(path, storage_options=storage_options)
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_ipc(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        df = cls._adapter_singleton.reader.scan_ipc(
            path,
            hive_partitioning=hive_partitioning,
            storage_options=storage_options,
        )
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_delta(
        cls,
        source: str,
        *,
        schema: type[SchemaT],
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        df = cls._adapter_singleton.reader.scan_delta(
            source,
            version=version,
            storage_options=storage_options,
        )
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def read_delta(
        cls,
        source: str,
        *,
        schema: type[SchemaT],
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
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
    ) -> PandasFrame[SchemaT]:
        df = cls._adapter_singleton.reader.read_excel(path, sheet_name=sheet_name)
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def read_avro(cls, path: str, *, schema: type[SchemaT]) -> PandasFrame[SchemaT]:
        df = cls._adapter_singleton.reader.read_avro(path)
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def read_database(
        cls, query: str, *, connection: object, schema: type[SchemaT]
    ) -> PandasFrame[SchemaT]:
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
    ) -> PandasFrame[SchemaT]:
        df = cls._adapter_singleton.reader.read_database_uri(query, uri=uri, engine=engine)
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)
