from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Generic, TypeVar, cast

from planframe.frame import Frame
from planframe.spark import SparkFrame
from planframe.typing.storage import StorageOptions
from planframe_sparkless._spark import _spark
from planframe_sparkless.adapter import (
    SparklessAdapter,
    SparklessBackendExpr,
    SparklessBackendFrame,
)

SchemaT = TypeVar("SchemaT")

SparklessData = Mapping[str, Sequence[object]] | Sequence[Mapping[str, object]]


def _schema_defaults(schema: type[Any]) -> dict[str, object]:
    ann = dict(getattr(schema, "__dict__", {}).get("__annotations__", {}))
    out: dict[str, object] = {}
    for name in ann:
        if name in getattr(schema, "__dict__", {}):
            out[name] = getattr(schema, name)
    return out


def _fill_missing_from_defaults(
    data: SparklessData, *, defaults: dict[str, object]
) -> SparklessData:
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


def _to_sparkless_df(data: SparklessData, *, schema: type[Any]) -> SparklessBackendFrame:
    defaults = _schema_defaults(schema)
    data2 = _fill_missing_from_defaults(data, defaults=defaults)
    return _spark().createDataFrame(data2)  # type: ignore[arg-type]


class _SparklessFrameMeta(type):
    def __call__(cls, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        # Allow normal construction when `Frame.source(...)` calls `cls(_data=..., ...)`.
        if "_data" in kwargs and "_adapter" in kwargs and "_plan" in kwargs and "_schema" in kwargs:
            return super().__call__(*args, **kwargs)

        data = args[0] if args else kwargs.pop("data")
        if kwargs:
            raise TypeError(f"Unexpected constructor kwargs: {sorted(kwargs)}")

        if not isinstance(data, (dict, list)):
            raise TypeError("SparklessFrame expects dict-of-lists or list-of-dicts")

        df = _to_sparkless_df(cast(SparklessData, data), schema=cast(type[Any], cls))
        cls_any = cast(Any, cls)
        return cls_any.source(df, adapter=cls_any._adapter_singleton, schema=cast(type[Any], cls))


class SparklessFrame(
    SparkFrame[SchemaT, SparklessBackendFrame, SparklessBackendExpr],
    Frame[SchemaT, SparklessBackendFrame, SparklessBackendExpr],
    Generic[SchemaT],
    metaclass=_SparklessFrameMeta,
):
    """A PlanFrame `Frame` bound to the sparkless backend, using the SparkFrame UI."""

    _adapter_singleton: ClassVar[SparklessAdapter] = SparklessAdapter()
    __planframe_model__ = True

    # ---- IO (Spark-style engine readers) ----
    @classmethod
    def scan_parquet(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> SparklessFrame[SchemaT]:
        df = cls._adapter_singleton.reader.scan_parquet(
            path, hive_partitioning=hive_partitioning, storage_options=storage_options
        )
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_csv(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> SparklessFrame[SchemaT]:
        df = cls._adapter_singleton.reader.scan_csv(path, storage_options=storage_options)
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    @classmethod
    def scan_ndjson(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> SparklessFrame[SchemaT]:
        df = cls._adapter_singleton.reader.scan_ndjson(path, storage_options=storage_options)
        return cls.source(df, adapter=cls._adapter_singleton, schema=schema)

    # Eager read aliases (Sparkless is lazy-ish; these are just naming aliases)
    @classmethod
    def read_parquet(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> SparklessFrame[SchemaT]:
        return cls.scan_parquet(path, schema=schema, storage_options=storage_options)

    @classmethod
    def read_csv(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> SparklessFrame[SchemaT]:
        return cls.scan_csv(path, schema=schema, storage_options=storage_options)

    @classmethod
    def read_json(
        cls,
        path: str,
        *,
        schema: type[SchemaT],
        storage_options: StorageOptions | None = None,
    ) -> SparklessFrame[SchemaT]:
        return cls.scan_ndjson(path, schema=schema, storage_options=storage_options)
