from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Literal, cast

from sparkless.sql import functions as F
from sparkless.sql.window import Window

from planframe.backend.adapter import (
    AdapterCapabilities,
    BaseAdapter,
    ColumnName,
    Columns,
    CompiledJoinKey,
    CompiledProjectItem,
    CompiledSortKey,
    CompileExprContext,
)
from planframe.backend.errors import PlanFrameBackendError, PlanFrameExpressionError
from planframe.execution_options import ExecutionOptions
from planframe.plan.join_options import JoinOptions
from planframe.plan.nodes import UnnestItem
from planframe.schema.ir import Schema
from planframe.typing.scalars import Scalar
from planframe.typing.storage import StorageOptions
from planframe_sparkless._spark import _spark
from planframe_sparkless.compile_expr import compile_expr

SparklessBackendFrame = Any  # runtime type is `builtins.PyDataFrame`
SparklessBackendExpr = Any  # runtime type is `builtins.PyColumn`


class SparklessAdapter(BaseAdapter[SparklessBackendFrame, SparklessBackendExpr]):
    name = "sparkless"

    @property
    def capabilities(self) -> AdapterCapabilities:
        # sparkless adapter currently implements a subset of IO and plan nodes.
        return AdapterCapabilities(
            explode_outer=False,
            posexplode_outer=False,
            lazy_sample=True,
            scan_delta=False,
            read_delta=False,
            sink_delta=False,
            read_avro=False,
            sink_avro=False,
            read_excel=False,
            sink_excel=False,
            read_database_uri=False,
            sink_database=False,
            storage_options=False,
        )

    # ---- AdapterReader surface (used by SparklessFrame classmethods) ----
    def scan_parquet(
        self,
        path: str,
        *,
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> SparklessBackendFrame:
        _ = hive_partitioning, storage_options
        return _spark().read.parquet(path)

    def scan_parquet_dataset(
        self, path_or_glob: str, *, storage_options: StorageOptions | None = None
    ) -> SparklessBackendFrame:
        _ = storage_options
        # Spark-style readers generally accept globs as path patterns.
        return _spark().read.parquet(path_or_glob)

    def scan_csv(
        self, path: str, *, storage_options: StorageOptions | None = None
    ) -> SparklessBackendFrame:
        _ = storage_options
        return _spark().read.csv(path)

    def scan_ndjson(
        self, path: str, *, storage_options: StorageOptions | None = None
    ) -> SparklessBackendFrame:
        _ = storage_options
        return _spark().read.json(path)

    def scan_ipc(
        self,
        path: str,
        *,
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> SparklessBackendFrame:
        _ = path, hive_partitioning, storage_options
        raise PlanFrameBackendError("sparkless adapter does not implement scan_ipc")

    def scan_delta(
        self,
        source: str,
        *,
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> SparklessBackendFrame:
        _ = source, version, storage_options
        raise PlanFrameBackendError("sparkless adapter does not implement scan_delta")

    def read_delta(
        self,
        source: str,
        *,
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> SparklessBackendFrame:
        _ = source, version, storage_options
        raise PlanFrameBackendError("sparkless adapter does not implement read_delta")

    def read_excel(
        self,
        path: str,
        *,
        sheet_name: str | None = None,
    ) -> SparklessBackendFrame:
        _ = path, sheet_name
        raise PlanFrameBackendError("sparkless adapter does not implement read_excel")

    def read_avro(self, path: str) -> SparklessBackendFrame:
        _ = path
        raise PlanFrameBackendError("sparkless adapter does not implement read_avro")

    def read_database(self, query: str, *, connection: object) -> SparklessBackendFrame:
        _ = query, connection
        raise PlanFrameBackendError("sparkless adapter does not implement read_database")

    def read_database_uri(
        self,
        query: str,
        *,
        uri: str,
        engine: Literal["connectorx", "adbc"] | None = None,
    ) -> SparklessBackendFrame:
        _ = query, uri, engine
        raise PlanFrameBackendError("sparkless adapter does not implement read_database_uri")

    # ---- Core transforms ----
    def select(self, df: SparklessBackendFrame, columns: Columns) -> SparklessBackendFrame:
        return df.select(*columns)

    def project(
        self,
        df: SparklessBackendFrame,
        items: tuple[CompiledProjectItem[SparklessBackendExpr], ...],
    ) -> SparklessBackendFrame:
        cols: list[Any] = []
        for it in items:
            if it.from_column is not None:
                cols.append(it.from_column)
            elif it.expr is not None:
                cols.append(cast(Any, it.expr).alias(it.name))
            else:
                raise AssertionError("Invalid CompiledProjectItem")
        return df.select(*cols)

    def drop(
        self, df: SparklessBackendFrame, columns: Columns, *, strict: bool = True
    ) -> SparklessBackendFrame:
        existing = set(df.columns)
        if strict:
            missing = [c for c in columns if c not in existing]
            if missing:
                raise PlanFrameBackendError(f"Columns not found for drop: {missing}")
            return df.drop(*columns)

        cols2 = tuple(c for c in columns if c in existing)
        if not cols2:
            return df
        return df.drop(*cols2)

    def rename(
        self,
        df: SparklessBackendFrame,
        mapping: dict[ColumnName, ColumnName],
        *,
        strict: bool = True,
    ) -> SparklessBackendFrame:
        if strict:
            missing = [c for c in mapping if c not in set(df.columns)]
            if missing:
                raise PlanFrameBackendError(f"Columns not found for rename: {missing}")
        out = df
        for old, new in mapping.items():
            if old in set(out.columns):
                out = out.withColumnRenamed(old, new)
        return out

    def with_column(
        self, df: SparklessBackendFrame, name: str, expr: SparklessBackendExpr
    ) -> SparklessBackendFrame:
        return df.withColumn(name, expr)

    def cast(self, df: SparklessBackendFrame, name: str, dtype: object) -> SparklessBackendFrame:
        # PlanFrame dtypes are backend-agnostic; sparkless expects Spark SQL type strings.
        # We accept `object` here and rely on adapter users passing strings when needed.
        if not isinstance(dtype, str):
            raise PlanFrameBackendError(
                "sparkless cast expects dtype as Spark SQL string (e.g. 'int')"
            )
        return df.withColumn(name, F.col(name).cast(dtype))

    def with_row_count(
        self, df: SparklessBackendFrame, *, name: str = "row_nr", offset: int = 0
    ) -> SparklessBackendFrame:
        # Spark requires an ordering for row_number(). Sparkless does not accept ordering
        # by a pure literal expression, so we order by the first column.
        first_col = df.columns[0] if df.columns else None
        if first_col is None:
            raise PlanFrameBackendError("Cannot add row count to empty-column sparkless DataFrame")
        w = Window.orderBy(F.col(first_col))
        return df.withColumn(name, F.row_number().over(w) + F.lit(offset) - F.lit(1))

    def filter(
        self, df: SparklessBackendFrame, predicate: SparklessBackendExpr
    ) -> SparklessBackendFrame:
        return df.filter(predicate)

    def sort(
        self,
        df: SparklessBackendFrame,
        keys: tuple[CompiledSortKey[SparklessBackendExpr], ...],
        *,
        descending: tuple[bool, ...],
        nulls_last: tuple[bool, ...],
    ) -> SparklessBackendFrame:
        # sparkless Columns support Spark-style null ordering (asc_nulls_* / desc_nulls_*).
        cols: list[Any] = []
        for k, desc, nl in zip(keys, descending, nulls_last, strict=True):
            if k.column is not None:
                c = F.col(k.column)
            else:
                if k.expr is None:
                    raise PlanFrameBackendError("Sort key expr cannot be None")
                c = k.expr
            if desc:
                cols.append(c.desc_nulls_last() if nl else c.desc_nulls_first())
            else:
                cols.append(c.asc_nulls_last() if nl else c.asc_nulls_first())
        return df.orderBy(*cols)

    def unique(
        self,
        df: SparklessBackendFrame,
        subset: Columns | None,
        *,
        keep: str = "first",
        maintain_order: bool = False,
    ) -> SparklessBackendFrame:
        _ = keep, maintain_order
        if subset is None:
            return df.distinct()
        return df.dropDuplicates(list(subset))

    def duplicated(
        self,
        df: SparklessBackendFrame,
        subset: Columns | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> SparklessBackendFrame:
        # Approximate via window count > 1.
        _ = keep
        cols = list(subset) if subset is not None else list(df.columns)
        w = Window.partitionBy(*[F.col(c) for c in cols]).orderBy(F.lit(1))
        return df.withColumn(out_name, (F.count(F.lit(1)).over(w) > F.lit(1)))

    def group_by_agg(
        self,
        df: SparklessBackendFrame,
        *,
        keys: tuple[CompiledJoinKey[SparklessBackendExpr], ...],
        named_aggs: dict[ColumnName, Any],
    ) -> SparklessBackendFrame:
        group_cols: list[Any] = []
        for k in keys:
            if k.column is not None:
                group_cols.append(F.col(k.column))
            else:
                group_cols.append(k.expr)
        g = df.groupBy(*group_cols)

        aggs: list[Any] = []
        for out_name, spec in named_aggs.items():
            if isinstance(spec, tuple):
                op, col = spec
                if op == "count":
                    aggs.append(F.count(F.col(col)).alias(out_name))
                elif op == "sum":
                    aggs.append(F.sum(F.col(col)).alias(out_name))
                elif op == "mean":
                    aggs.append(F.avg(F.col(col)).alias(out_name))
                elif op == "min":
                    aggs.append(F.min(F.col(col)).alias(out_name))
                elif op == "max":
                    aggs.append(F.max(F.col(col)).alias(out_name))
                elif op == "n_unique":
                    aggs.append(F.countDistinct(F.col(col)).alias(out_name))
                else:
                    raise PlanFrameBackendError(f"Unsupported aggregation op: {op!r}")
            else:
                aggs.append(cast(Any, spec).alias(out_name))
        return g.agg(*aggs)

    def group_by_dynamic_agg(self, df: SparklessBackendFrame, **_: Any) -> SparklessBackendFrame:
        raise PlanFrameBackendError("sparkless adapter does not implement dynamic group_by yet")

    def rolling_agg(self, df: SparklessBackendFrame, **_: Any) -> SparklessBackendFrame:
        raise PlanFrameBackendError("sparkless adapter does not implement rolling_agg yet")

    def drop_nulls(
        self,
        df: SparklessBackendFrame,
        subset: Columns | None,
        *,
        how: Literal["any", "all"] = "any",
        threshold: int | None = None,
    ) -> SparklessBackendFrame:
        # Spark uses DataFrame.na.drop
        subset_list = None if subset is None else list(subset)
        if threshold is not None:
            return df.na.drop(thresh=threshold, subset=subset_list)
        return df.na.drop(how=how, subset=subset_list)

    def fill_null(
        self,
        df: SparklessBackendFrame,
        value: Scalar | SparklessBackendExpr | None,
        subset: Columns | None,
        *,
        strategy: str | None = None,
    ) -> SparklessBackendFrame:
        _ = strategy
        subset_list = None if subset is None else list(subset)
        if value is None:
            raise PlanFrameBackendError("sparkless fill_null does not support value=None")
        if isinstance(value, (int, float, str, bool)):
            return df.na.fill(value=value, subset=subset_list)
        raise PlanFrameBackendError("sparkless fill_null only supports scalar values currently")

    def melt(self, df: SparklessBackendFrame, **_: Any) -> SparklessBackendFrame:
        raise PlanFrameBackendError("sparkless adapter does not implement melt yet")

    def join(
        self,
        left: SparklessBackendFrame,
        right: SparklessBackendFrame,
        *,
        left_on: tuple[CompiledJoinKey[SparklessBackendExpr], ...],
        right_on: tuple[CompiledJoinKey[SparklessBackendExpr], ...],
        how: str = "inner",
        suffix: str = "_right",
        options: JoinOptions | None = None,
    ) -> SparklessBackendFrame:
        _ = suffix, options
        if not left_on and not right_on:
            return left.crossJoin(right)
        if len(left_on) != len(right_on):
            raise ValueError("Join keys must match in length")

        # Prefer Spark-style ``on=`` / ``left_on=``/``right_on=`` so join keys are not
        # ambiguous when column names overlap (unqualified ``F.col`` in a boolean ``on``).
        simple_name_keys = True
        left_names: list[str] = []
        right_names: list[str] = []
        for lk, rk in zip(left_on, right_on, strict=True):
            if lk.column is None or lk.expr is not None or rk.column is None or rk.expr is not None:
                simple_name_keys = False
                break
            left_names.append(lk.column)
            right_names.append(rk.column)

        if simple_name_keys:
            if left_names == right_names:
                on_arg = left_names[0] if len(left_names) == 1 else left_names
                return left.join(right, on=on_arg, how=how)
            return left.join(right, left_on=left_names, right_on=right_names, how=how)

        conds: list[Any] = []
        for lk, rk in zip(left_on, right_on, strict=True):
            lcol = F.col(lk.column) if lk.column is not None else lk.expr
            rcol = F.col(rk.column) if rk.column is not None else rk.expr
            conds.append(lcol == rcol)
        cond = conds[0]
        for c in conds[1:]:
            cond = cond & c
        return left.join(right, on=cond, how=how)

    def slice(
        self, df: SparklessBackendFrame, *, offset: int, length: int | None
    ) -> SparklessBackendFrame:
        if offset != 0:
            raise PlanFrameBackendError("sparkless adapter does not support offset slicing yet")
        return df.limit(length) if length is not None else df

    def head(self, df: SparklessBackendFrame, n: int) -> SparklessBackendFrame:
        return df.limit(n)

    def tail(self, df: SparklessBackendFrame, n: int) -> SparklessBackendFrame:
        # No direct tail in Spark; approximate by collecting and re-creating.
        rows = df.collect()[-n:]
        dicts: list[dict[str, object]] = []
        for r in rows:
            if hasattr(r, "asDict"):
                dicts.append(cast(dict[str, object], r.asDict()))
            else:
                raise PlanFrameBackendError(
                    f"Unexpected row type from sparkless collect(): {type(r)!r}"
                )
        return _spark().createDataFrame(dicts)

    def concat_vertical(
        self, left: SparklessBackendFrame, right: SparklessBackendFrame
    ) -> SparklessBackendFrame:
        return left.unionByName(right, allowMissingColumns=True)

    def concat_horizontal(
        self, left: SparklessBackendFrame, right: SparklessBackendFrame
    ) -> SparklessBackendFrame:
        raise PlanFrameBackendError("sparkless adapter does not implement concat_horizontal yet")

    def pivot(self, df: SparklessBackendFrame, **_: Any) -> SparklessBackendFrame:
        raise PlanFrameBackendError("sparkless adapter does not implement pivot yet")

    # ---- Writes ----
    def write_parquet(
        self,
        df: SparklessBackendFrame,
        path: str,
        *,
        compression: str = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: StorageOptions | None = None,
    ) -> None:
        _ = compression, row_group_size, partition_by, storage_options
        df.write.parquet(path)

    def write_csv(
        self,
        df: SparklessBackendFrame,
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: StorageOptions | None = None,
    ) -> None:
        _ = storage_options
        df.write.csv(path, sep=separator, header=include_header)

    def write_ndjson(
        self, df: SparklessBackendFrame, path: str, *, storage_options: StorageOptions | None = None
    ) -> None:
        _ = storage_options
        df.write.json(path)

    def write_ipc(
        self,
        df: SparklessBackendFrame,
        path: str,
        *,
        compression: str = "uncompressed",
        storage_options: StorageOptions | None = None,
    ) -> None:
        _ = df, path, compression, storage_options
        raise PlanFrameBackendError("sparkless adapter does not implement IPC writing")

    def write_database(
        self,
        df: SparklessBackendFrame,
        *,
        table_name: str,
        connection: object,
        if_table_exists: str = "fail",
        engine: str | None = None,
    ) -> None:
        _ = df, table_name, connection, if_table_exists, engine
        raise PlanFrameBackendError("sparkless adapter does not implement database writing")

    def write_excel(
        self, df: SparklessBackendFrame, path: str, *, worksheet: str = "Sheet1"
    ) -> None:
        _ = df, path, worksheet
        raise PlanFrameBackendError("sparkless adapter does not implement Excel writing")

    def write_delta(
        self,
        df: SparklessBackendFrame,
        target: str,
        *,
        mode: str = "error",
        storage_options: StorageOptions | None = None,
    ) -> None:
        _ = df, target, mode, storage_options
        raise PlanFrameBackendError("sparkless adapter does not implement Delta writing")

    def write_avro(
        self,
        df: SparklessBackendFrame,
        path: str,
        *,
        compression: str = "uncompressed",
        name: str = "",
    ) -> None:
        _ = df, path, compression, name
        raise PlanFrameBackendError("sparkless adapter does not implement Avro writing")

    # ---- Nested/array ops ----
    def explode(
        self, df: SparklessBackendFrame, columns: Columns, *, outer: bool = False
    ) -> SparklessBackendFrame:
        _ = outer
        out = df
        for c in columns:
            out = out.withColumn(c, F.explode(F.col(c)))
        return out

    def unnest(
        self, df: SparklessBackendFrame, items: tuple[UnnestItem, ...]
    ) -> SparklessBackendFrame:
        _ = df, items
        raise PlanFrameBackendError("sparkless adapter does not implement unnest yet")

    def posexplode(
        self,
        df: SparklessBackendFrame,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        outer: bool = False,
    ) -> SparklessBackendFrame:
        _ = outer
        value_name = value or column
        return df.select("*", F.posexplode(F.col(column)).alias(pos, value_name))

    def drop_nulls_all(
        self, df: SparklessBackendFrame, subset: tuple[str, ...] | None
    ) -> SparklessBackendFrame:
        return self.drop_nulls(df, subset, how="all")

    def sample(
        self,
        df: SparklessBackendFrame,
        *,
        n: int | None = None,
        frac: float | None = None,
        with_replacement: bool = False,
        shuffle: bool = False,
        seed: int | None = None,
    ) -> SparklessBackendFrame:
        _ = shuffle
        if frac is None and n is None:
            raise ValueError("sample requires n or frac")
        if frac is None:
            # Approximate n via fraction; requires count (expensive). Keep simple.
            raise PlanFrameBackendError("sparkless adapter sample(n=...) is not implemented")
        return df.sample(withReplacement=with_replacement, fraction=frac, seed=seed)

    def resolve_backend_dtype_from_frame(
        self, df: SparklessBackendFrame, name: str
    ) -> object | None:
        schema = getattr(df, "schema", None)
        if schema is None:
            return None
        for field in getattr(schema, "fields", []):
            if getattr(field, "name", None) == name:
                dt = getattr(field, "dataType", None)
                if dt is not None:
                    return str(dt)
                return getattr(field, "data_type", None)
        return None

    # ---- Expression compilation + materialization ----
    def compile_expr(
        self,
        expr: object,
        *,
        schema: Schema | None = None,
        ctx: CompileExprContext | None = None,
    ) -> SparklessBackendExpr:
        if ctx is None:
            ctx = CompileExprContext(schema=schema)
        if isinstance(expr, object) and hasattr(expr, "__class__"):
            # PlanFrame Expr nodes are dataclasses; compile using our mapping.
            from planframe.expr.api import Expr as PFExpr

            if isinstance(expr, PFExpr):
                return compile_expr(
                    cast(Any, expr),
                    dtype_for=lambda n: self.resolve_dtype(n, ctx=ctx),
                )
        raise PlanFrameExpressionError(f"Unsupported expr type for sparkless: {type(expr)!r}")

    def collect(
        self, df: SparklessBackendFrame, *, options: ExecutionOptions | None = None
    ) -> SparklessBackendFrame:
        # Return backend-native dataframe (lazy plan). Row export methods execute.
        _ = options
        return df

    def to_dicts(
        self, df: SparklessBackendFrame, *, options: ExecutionOptions | None = None
    ) -> list[dict[str, object]]:
        _ = options
        rows = df.collect()
        out: list[dict[str, object]] = []
        for r in rows:
            if hasattr(r, "asDict"):
                out.append(cast(dict[str, object], r.asDict()))
            elif isinstance(r, dict):
                out.append(cast(dict[str, object], r))
            else:
                raise PlanFrameBackendError(
                    f"Unexpected row type from sparkless collect(): {type(r)!r}"
                )
        return out

    def to_dict(
        self, df: SparklessBackendFrame, *, options: ExecutionOptions | None = None
    ) -> dict[str, list[object]]:
        rows = self.to_dicts(df, options=options)
        if not rows:
            return {str(c): [] for c in df.columns}
        out: dict[str, list[object]] = {k: [] for k in rows[0]}
        for r in rows:
            for k, v in r.items():
                out[k].append(v)
        return out

    def stream_dicts(
        self, df: SparklessBackendFrame, *, options: ExecutionOptions | None = None
    ) -> Iterator[dict[str, object]]:
        # sparkless currently doesn't expose toLocalIterator; fall back to materializing rows.
        yield from self.to_dicts(df, options=options)
