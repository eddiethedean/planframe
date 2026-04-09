from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Generic, Literal, TypeVar, cast

import pandas as pd

from planframe.backend.errors import PlanFrameBackendError
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

    @classmethod
    def scan_parquet(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        # pandas is eager; this is a convenience loader, not a lazy scan.
        try:
            df = pd.read_parquet(path, storage_options=storage_options)  # type: ignore[call-arg]
        except ImportError as e:
            raise PlanFrameBackendError(
                "Parquet support requires installing planframe-pandas[parquet]"
            ) from e
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_parquet_dataset(
        cls,
        path_or_glob: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        raise PlanFrameBackendError(
            "pandas adapter does not implement scan_parquet_dataset; "
            "use scan_parquet on a single file or implement dataset loading externally."
        )

    @classmethod
    def scan_csv(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        df = pd.read_csv(path, storage_options=storage_options)  # type: ignore[call-arg]
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_ndjson(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        df = pd.read_json(path, lines=True, storage_options=storage_options)  # type: ignore[call-arg]
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
        raise PlanFrameBackendError("pandas adapter does not implement scan_ipc")

    @classmethod
    def scan_delta(
        cls,
        source: str,
        *,
        schema: type[SchemaT],
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        raise PlanFrameBackendError("pandas adapter does not implement scan_delta")

    @classmethod
    def read_delta(
        cls,
        source: str,
        *,
        schema: type[SchemaT],
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> PandasFrame[SchemaT]:
        raise PlanFrameBackendError("pandas adapter does not implement read_delta")

    @classmethod
    def read_excel(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        sheet_name: str | None = None,
    ) -> PandasFrame[SchemaT]:
        # pandas uses openpyxl for xlsx by default.
        try:
            df = pd.read_excel(path, sheet_name=sheet_name)  # type: ignore[call-arg]
        except ImportError as e:
            raise PlanFrameBackendError(
                "Excel support requires installing planframe-pandas[excel]"
            ) from e
        if isinstance(df, dict):
            raise PlanFrameBackendError(
                "pandas read_excel returned multiple sheets; pass sheet_name= to select one"
            )
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def read_avro(cls, path: str, *, schema: type[SchemaT]) -> PandasFrame[SchemaT]:
        raise PlanFrameBackendError("pandas adapter does not implement read_avro")

    @classmethod
    def read_database(
        cls, query: str, *, connection: object, schema: type[SchemaT]
    ) -> PandasFrame[SchemaT]:
        df = pd.read_sql_query(query, con=connection)  # type: ignore[arg-type]
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
        if engine is not None:
            raise PlanFrameBackendError(
                "pandas adapter does not support engine= for read_database_uri"
            )
        raise PlanFrameBackendError(
            "pandas adapter does not implement read_database_uri. "
            "Use read_database(query, connection=...) with a DBAPI/SQLAlchemy connection."
        )
