from __future__ import annotations

import importlib
from typing import Any, TypeAlias

from planframe.backend.adapter import BaseAdapter
from planframe.backend.errors import PlanFrameBackendError

SparkDataFrame: TypeAlias = Any
SparkColumn: TypeAlias = Any


def _require_pyspark() -> Any:
    try:
        pyspark = importlib.import_module("pyspark")
        importlib.import_module("pyspark.sql")
        return pyspark
    except Exception as e:  # noqa: BLE001
        raise PlanFrameBackendError(
            "PySpark is required for the planframe-spark adapter. "
            'Install with `pip install "planframe-spark[pyspark]"`.'
        ) from e


class PySparkAdapter(BaseAdapter[SparkDataFrame, SparkColumn]):
    """PlanFrame backend adapter for `pyspark.sql.DataFrame`.

    This adapter is intentionally minimal and will raise for operations that
    don't have a straightforward, backend-agnostic Spark implementation yet.
    """

    name = "pyspark"

    def select(self, df: SparkDataFrame, columns: tuple[str, ...]) -> SparkDataFrame:
        _require_pyspark()
        return df.select(*columns)

    def project(self, df: SparkDataFrame, items: tuple[Any, ...]) -> SparkDataFrame:
        # Implementing expression compilation to Spark Columns is future work.
        raise PlanFrameBackendError("pyspark adapter does not implement project() yet")

    def drop(
        self, df: SparkDataFrame, columns: tuple[str, ...], *, strict: bool = True
    ) -> SparkDataFrame:
        _ = strict
        _require_pyspark()
        return df.drop(*columns)

    def rename(
        self, df: SparkDataFrame, mapping: dict[str, str], *, strict: bool = True
    ) -> SparkDataFrame:
        _ = strict
        _require_pyspark()
        out = df
        for k, v in mapping.items():
            out = out.withColumnRenamed(k, v)
        return out

    def with_column(self, df: SparkDataFrame, name: str, expr: SparkColumn) -> SparkDataFrame:
        _require_pyspark()
        return df.withColumn(name, expr)

    def cast(self, df: SparkDataFrame, name: str, dtype: object) -> SparkDataFrame:
        _require_pyspark()
        return df.withColumn(name, df[name].cast(dtype))  # type: ignore[arg-type]

    def with_row_count(
        self, df: SparkDataFrame, *, name: str = "row_nr", offset: int = 0
    ) -> SparkDataFrame:
        _ = name
        _ = offset
        raise PlanFrameBackendError("pyspark adapter does not implement with_row_count() yet")

    def filter(self, df: SparkDataFrame, predicate: SparkColumn) -> SparkDataFrame:
        _require_pyspark()
        return df.filter(predicate)

    def sort(
        self,
        df: SparkDataFrame,
        keys: tuple[Any, ...],
        *,
        descending: tuple[bool, ...],
        nulls_last: tuple[bool, ...],
    ) -> SparkDataFrame:
        _ = keys
        _ = descending
        _ = nulls_last
        raise PlanFrameBackendError("pyspark adapter does not implement sort() yet")

    def unique(
        self,
        df: SparkDataFrame,
        subset: tuple[str, ...] | None,
        *,
        keep: str = "first",
        maintain_order: bool = False,
    ) -> SparkDataFrame:
        _ = subset
        _ = keep
        _ = maintain_order
        raise PlanFrameBackendError("pyspark adapter does not implement unique() yet")

    def duplicated(
        self,
        df: SparkDataFrame,
        subset: tuple[str, ...] | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> SparkDataFrame:
        _ = df
        _ = subset
        _ = keep
        _ = out_name
        raise PlanFrameBackendError("pyspark adapter does not implement duplicated() yet")

    def group_by_agg(
        self, df: SparkDataFrame, *, keys: tuple[Any, ...], named_aggs: dict[str, Any]
    ) -> SparkDataFrame:
        _ = df
        _ = keys
        _ = named_aggs
        raise PlanFrameBackendError("pyspark adapter does not implement group_by_agg() yet")

    def group_by_dynamic_agg(
        self,
        df: SparkDataFrame,
        *,
        index_column: str,
        every: str,
        period: str | None = None,
        by: tuple[str, ...] | None = None,
        named_aggs: dict[str, Any],
    ) -> SparkDataFrame:
        _ = df
        _ = index_column
        _ = every
        _ = period
        _ = by
        _ = named_aggs
        raise PlanFrameBackendError("pyspark adapter does not implement group_by_dynamic_agg() yet")

    def rolling_agg(
        self,
        df: SparkDataFrame,
        *,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: tuple[str, ...] | None = None,
        min_periods: int = 1,
    ) -> SparkDataFrame:
        _ = df
        _ = on
        _ = column
        _ = window_size
        _ = op
        _ = out_name
        _ = by
        _ = min_periods
        raise PlanFrameBackendError("pyspark adapter does not implement rolling_agg() yet")

    def drop_nulls(
        self,
        df: SparkDataFrame,
        subset: tuple[str, ...] | None,
        *,
        how: str = "any",
        threshold: int | None = None,
    ) -> SparkDataFrame:
        _ = how
        _ = threshold
        _require_pyspark()
        if subset is None:
            return df.na.drop()
        return df.na.drop(subset=list(subset))

    def fill_null(
        self,
        df: SparkDataFrame,
        value: object,
        subset: tuple[str, ...] | None,
        *,
        strategy: str | None = None,
    ) -> SparkDataFrame:
        _ = strategy
        _require_pyspark()
        if subset is None:
            return df.na.fill(value)
        return df.na.fill(value, subset=list(subset))

    def melt(
        self,
        df: SparkDataFrame,
        *,
        id_vars: tuple[str, ...],
        value_vars: tuple[str, ...],
        variable_name: str,
        value_name: str,
    ) -> SparkDataFrame:
        _ = df
        _ = id_vars
        _ = value_vars
        _ = variable_name
        _ = value_name
        raise PlanFrameBackendError("pyspark adapter does not implement melt()/unpivot yet")

    def join(
        self,
        left: SparkDataFrame,
        right: SparkDataFrame,
        *,
        left_on: tuple[Any, ...],
        right_on: tuple[Any, ...],
        how: str = "inner",
        suffix: str = "_right",
        options: Any = None,
    ) -> SparkDataFrame:
        _ = left_on
        _ = right_on
        _ = suffix
        _ = options
        _require_pyspark()
        # Only the simplest shape is supported for now: symmetric string keys via on= lowering.
        raise PlanFrameBackendError("pyspark adapter does not implement join() yet")

    def slice(self, df: SparkDataFrame, *, offset: int, length: int | None) -> SparkDataFrame:
        _ = df
        _ = offset
        _ = length
        raise PlanFrameBackendError("pyspark adapter does not implement slice() yet")

    def head(self, df: SparkDataFrame, n: int) -> SparkDataFrame:
        _require_pyspark()
        return df.limit(n)

    def tail(self, df: SparkDataFrame, n: int) -> SparkDataFrame:
        _ = df
        _ = n
        raise PlanFrameBackendError("pyspark adapter does not implement tail() yet")

    def concat_vertical(self, left: SparkDataFrame, right: SparkDataFrame) -> SparkDataFrame:
        _require_pyspark()
        return left.unionByName(right)

    def concat_horizontal(self, left: SparkDataFrame, right: SparkDataFrame) -> SparkDataFrame:
        _ = left
        _ = right
        raise PlanFrameBackendError("pyspark adapter does not implement concat_horizontal() yet")

    def explode(
        self, df: SparkDataFrame, columns: tuple[str, ...], *, outer: bool = False
    ) -> SparkDataFrame:
        _ = df
        _ = columns
        _ = outer
        raise PlanFrameBackendError("pyspark adapter does not implement explode() yet")

    def posexplode(
        self,
        df: SparkDataFrame,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        outer: bool = False,
    ) -> SparkDataFrame:
        _ = df
        _ = column
        _ = pos
        _ = value
        _ = outer
        raise PlanFrameBackendError("pyspark adapter does not implement posexplode() yet")

    def unnest(self, df: SparkDataFrame, items: tuple[Any, ...]) -> SparkDataFrame:
        _ = df
        _ = items
        raise PlanFrameBackendError("pyspark adapter does not implement unnest() yet")

    def sample(
        self,
        df: SparkDataFrame,
        *,
        n: int | None = None,
        frac: float | None = None,
        with_replacement: bool = False,
        shuffle: bool = False,
        seed: int | None = None,
    ) -> SparkDataFrame:
        _ = n
        _ = shuffle
        _require_pyspark()
        if frac is None:
            raise ValueError("sample requires frac=")
        return df.sample(withReplacement=with_replacement, fraction=frac, seed=seed)

    def collect(self, df: SparkDataFrame, *, options: Any = None) -> SparkDataFrame:
        _ = options
        return df

    def to_dicts(self, df: SparkDataFrame, *, options: Any = None) -> list[dict[str, object]]:
        _ = options
        _require_pyspark()
        return [row.asDict(recursive=True) for row in df.collect()]
