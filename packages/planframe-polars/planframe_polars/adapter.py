from __future__ import annotations

import inspect
from typing import Any, Literal, cast

import polars as pl

from planframe.backend.adapter import (
    AdapterCapabilities,
    BaseAdapter,
    Columns,
    CompiledJoinKey,
    CompiledProjectItem,
    CompiledSortKey,
)
from planframe.backend.errors import PlanFrameBackendError
from planframe.execution_options import ExecutionOptions
from planframe.expr.api import Expr
from planframe.plan.join_options import JoinOptions
from planframe.plan.nodes import UnnestItem
from planframe.typing.scalars import Scalar
from planframe.typing.storage import StorageOptions
from planframe_polars.compile_expr import compile_expr

PolarsBackendFrame = pl.DataFrame | pl.LazyFrame

# Polars adds join kwargs over time; only forward hints the installed version supports.
_LAZYFRAME_JOIN_PARAM_NAMES = frozenset(inspect.signature(pl.LazyFrame.join).parameters)


class PolarsAdapter(BaseAdapter[PolarsBackendFrame, pl.Expr]):
    name = "polars"

    @property
    def capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            explode_outer=False,
            posexplode_outer=False,
            lazy_sample=False,
            storage_options=False,
        )

    def _collect_df(self, df: PolarsBackendFrame) -> pl.DataFrame:
        out = df.collect() if isinstance(df, pl.LazyFrame) else df
        # Polars' type stubs may model `collect()` as returning an intermediate query.
        if not isinstance(out, pl.DataFrame):
            raise TypeError("Expected Polars collect() to return a DataFrame")
        return out

    def select(self, df: PolarsBackendFrame, columns: tuple[str, ...]) -> PolarsBackendFrame:
        return df.select(list(columns))

    def project(
        self,
        df: PolarsBackendFrame,
        items: tuple[CompiledProjectItem[pl.Expr], ...],
    ) -> PolarsBackendFrame:
        out: list[pl.Expr] = []
        for it in items:
            if it.from_column is not None:
                out.append(pl.col(it.from_column).alias(it.name))
            elif it.expr is not None:
                out.append(it.expr.alias(it.name))
            else:
                raise ValueError("CompiledProjectItem requires from_column or expr")
        return df.select(out)

    def drop(
        self, df: PolarsBackendFrame, columns: tuple[str, ...], *, strict: bool = True
    ) -> PolarsBackendFrame:
        return df.drop(*columns, strict=strict)

    def rename(
        self, df: PolarsBackendFrame, mapping: dict[str, str], *, strict: bool = True
    ) -> PolarsBackendFrame:
        return df.rename(mapping, strict=strict)

    def with_column(self, df: PolarsBackendFrame, name: str, expr: pl.Expr) -> PolarsBackendFrame:
        return df.with_columns(expr.alias(name))

    def cast(self, df: PolarsBackendFrame, name: str, dtype: object) -> PolarsBackendFrame:
        return df.with_columns(pl.col(name).cast(cast(Any, dtype)))

    def with_row_count(
        self, df: PolarsBackendFrame, *, name: str = "row_nr", offset: int = 0
    ) -> PolarsBackendFrame:
        # Polars row_index is 0-based, offset supported natively.
        return df.with_row_index(name=name, offset=offset)

    def filter(self, df: PolarsBackendFrame, predicate: pl.Expr) -> PolarsBackendFrame:
        return df.filter(predicate)

    def sort(
        self,
        df: PolarsBackendFrame,
        keys: tuple[CompiledSortKey[pl.Expr], ...],
        *,
        descending: tuple[bool, ...],
        nulls_last: tuple[bool, ...],
    ) -> PolarsBackendFrame:
        if not keys:
            return df
        by: list[str | pl.Expr] = []
        for k in keys:
            if k.column is not None:
                by.append(k.column)
            elif k.expr is not None:
                by.append(k.expr)
            else:
                raise ValueError("CompiledSortKey requires column or expr")
        return df.sort(by, descending=descending, nulls_last=nulls_last)

    def unique(
        self,
        df: PolarsBackendFrame,
        subset: tuple[str, ...] | None,
        *,
        keep: str = "first",
        maintain_order: bool = False,
    ) -> PolarsBackendFrame:
        kwargs: dict[str, Any] = {"keep": keep, "maintain_order": maintain_order}
        if subset is not None:
            kwargs["subset"] = list(subset)
        return df.unique(**kwargs)  # type: ignore[arg-type]

    def duplicated(
        self,
        df: PolarsBackendFrame,
        subset: tuple[str, ...] | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> PolarsBackendFrame:
        if keep not in {"first", "last", False}:
            raise ValueError("keep must be 'first', 'last', or False")

        cols = list(subset) if subset is not None else None
        expr = pl.struct(cols) if cols is not None else pl.struct(pl.all())
        if keep == "first":
            mask_expr = ~expr.is_first_distinct()
        elif keep == "last":
            mask_expr = ~expr.is_last_distinct()
        else:
            # Mark *all* duplicated rows (including the first and last occurrences).
            mask_expr = (~expr.is_first_distinct()) | (~expr.is_last_distinct())

        if isinstance(df, pl.LazyFrame):
            return df.select(mask_expr.alias(out_name))
        mask = df.select(mask_expr.alias(out_name))[out_name]
        return pl.DataFrame({out_name: mask})

    def compile_expr(self, expr: object, *, schema: Any = None) -> pl.Expr:
        if not isinstance(expr, Expr):
            raise TypeError(f"Expected PlanFrame Expr, got {type(expr)!r}")
        return compile_expr(expr)

    def group_by_agg(
        self,
        df: PolarsBackendFrame,
        *,
        keys: tuple[CompiledJoinKey[pl.Expr], ...],
        named_aggs: dict[str, tuple[str, str] | pl.Expr],
    ) -> PolarsBackendFrame:
        if not keys:
            raise ValueError("keys must be non-empty")
        by_exprs: list[pl.Expr] = []
        for i, k in enumerate(keys):
            if k.column is not None:
                by_exprs.append(pl.col(k.column))
            elif k.expr is not None:
                by_exprs.append(k.expr.alias(f"__pf_g{i}"))
            else:
                raise ValueError("CompiledJoinKey requires column or expr")
        agg_exprs: list[pl.Expr] = []
        for out_name, spec in named_aggs.items():
            if (
                isinstance(spec, tuple)
                and len(spec) == 2
                and isinstance(spec[0], str)
                and isinstance(spec[1], str)
            ):
                op = spec[0]
                col: str = spec[1]
                e = pl.col(col)
                if op == "count":
                    ex = e.count()
                elif op == "sum":
                    ex = e.sum()
                elif op == "mean":
                    ex = e.mean()
                elif op == "min":
                    ex = e.min()
                elif op == "max":
                    ex = e.max()
                elif op == "n_unique":
                    ex = e.n_unique()
                else:
                    raise ValueError(f"Unsupported agg op: {op!r}")
                agg_exprs.append(ex.alias(out_name))
            else:
                expr = cast(pl.Expr, spec)
                agg_exprs.append(expr.alias(out_name))
        return df.group_by(*by_exprs).agg(agg_exprs)

    def group_by_dynamic_agg(
        self,
        df: PolarsBackendFrame,
        *,
        index_column: str,
        every: str,
        period: str | None = None,
        by: tuple[str, ...] | None = None,
        named_aggs: dict[str, tuple[str, str] | pl.Expr],
    ) -> PolarsBackendFrame:
        if not every:
            raise ValueError("every must be non-empty")

        # Build aggregation expressions (same handling as group_by_agg).
        agg_exprs: list[pl.Expr] = []
        for out_name, spec in named_aggs.items():
            if (
                isinstance(spec, tuple)
                and len(spec) == 2
                and isinstance(spec[0], str)
                and isinstance(spec[1], str)
            ):
                op = spec[0]
                col: str = spec[1]
                e = pl.col(col)
                if op == "count":
                    ex = e.count()
                elif op == "sum":
                    ex = e.sum()
                elif op == "mean":
                    ex = e.mean()
                elif op == "min":
                    ex = e.min()
                elif op == "max":
                    ex = e.max()
                elif op == "n_unique":
                    ex = e.n_unique()
                else:
                    raise ValueError(f"Unsupported agg op: {op!r}")
                agg_exprs.append(ex.alias(out_name))
            else:
                expr = cast(pl.Expr, spec)
                agg_exprs.append(expr.alias(out_name))

        kwargs: dict[str, Any] = {"every": every}
        if period is not None:
            kwargs["period"] = period
        if by is not None:
            kwargs["by"] = list(by)

        return df.group_by_dynamic(index_column, **kwargs).agg(agg_exprs)

    def rolling_agg(
        self,
        df: PolarsBackendFrame,
        *,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: tuple[str, ...] | None = None,
        min_periods: int = 1,
    ) -> PolarsBackendFrame:
        # Ensure deterministic ordering for rolling windows.
        df2 = df.sort(on)
        base = pl.col(column)

        if isinstance(window_size, int):
            kwargs: dict[str, Any] = {"window_size": window_size, "min_periods": min_periods}
            if op == "sum":
                expr = base.rolling_sum(**kwargs)  # type: ignore[call-arg]
            elif op == "mean":
                expr = base.rolling_mean(**kwargs)  # type: ignore[call-arg]
            elif op == "min":
                expr = base.rolling_min(**kwargs)  # type: ignore[call-arg]
            elif op == "max":
                expr = base.rolling_max(**kwargs)  # type: ignore[call-arg]
            elif op == "count":
                expr = (
                    base.is_not_null().cast(pl.Int64).rolling_sum(**kwargs)  # type: ignore[call-arg]
                )
            else:
                raise ValueError(f"Unsupported rolling op: {op!r}")
        else:
            # time-based rolling, use *_by on the on-column.
            by_col = pl.col(on)
            kwargs2: dict[str, Any] = {"window_size": window_size, "min_periods": min_periods}
            if op == "sum":
                expr = base.rolling_sum_by(by_col, **kwargs2)  # type: ignore[call-arg]
            elif op == "mean":
                expr = base.rolling_mean_by(by_col, **kwargs2)  # type: ignore[call-arg]
            elif op == "min":
                expr = base.rolling_min_by(by_col, **kwargs2)  # type: ignore[call-arg]
            elif op == "max":
                expr = base.rolling_max_by(by_col, **kwargs2)  # type: ignore[call-arg]
            elif op == "count":
                expr = (
                    base.is_not_null().cast(pl.Int64).rolling_sum_by(by_col, **kwargs2)  # type: ignore[call-arg]
                )
            else:
                raise ValueError(f"Unsupported rolling op: {op!r}")

        if by is not None:
            expr = expr.over(list(by))

        return df2.with_columns(expr.alias(out_name))

    def drop_nulls(
        self,
        df: PolarsBackendFrame,
        subset: tuple[str, ...] | None,
        *,
        how: Literal["any", "all"] = "any",
        threshold: int | None = None,
    ) -> PolarsBackendFrame:
        if how not in ("any", "all"):
            raise ValueError("drop_nulls how must be 'any' or 'all'")
        if threshold is not None and threshold < 0:
            raise ValueError("drop_nulls threshold must be non-negative")

        if subset is not None:
            cols = list(subset)
        elif isinstance(df, pl.LazyFrame):
            cols = list(df.collect_schema().names())
        else:
            cols = list(df.columns)

        # Fast path: Polars has built-in drop_nulls = "any" semantics.
        if how == "any" and threshold is None:
            if subset is None:
                return df.drop_nulls()
            return df.drop_nulls(cols)

        if not cols:
            return df

        # Keep rows based on row-wise null logic.
        if threshold is not None:
            # Keep if non-null count >= threshold.
            nn = pl.sum_horizontal([pl.col(c).is_not_null().cast(pl.Int64) for c in cols])
            return df.filter(nn >= threshold)

        if how == "all":
            all_null = pl.all_horizontal([pl.col(c).is_null() for c in cols])
            return df.filter(~all_null)

        # (how == "any", threshold is None) handled earlier.
        return df

    def fill_null(
        self,
        df: PolarsBackendFrame,
        value: Scalar | pl.Expr | None,
        subset: tuple[str, ...] | None,
        *,
        strategy: str | None = None,
    ) -> PolarsBackendFrame:
        if (value is None) == (strategy is None):
            raise ValueError("fill_null requires exactly one of value or strategy")

        if strategy is not None:
            allowed = {"forward", "backward", "min", "max", "mean", "zero", "one"}
            if strategy not in allowed:
                raise ValueError(f"Unsupported fill_null strategy={strategy!r}")
            strat_lit = cast(
                Literal["forward", "backward", "min", "max", "mean", "zero", "one"],
                strategy,
            )
            if subset is None:
                # Polars supports strategy-based fill on eager and lazy frames.
                return df.fill_null(strategy=strat_lit)
            exprs = [pl.col(c).fill_null(strategy=strat_lit) for c in subset]
            return df.with_columns(exprs)

        # value-based fill (literal or Expr)
        if subset is None:
            return df.fill_null(value)  # type: ignore[arg-type]
        exprs = [pl.col(c).fill_null(value) for c in subset]  # type: ignore[arg-type]
        return df.with_columns(exprs)

    def melt(
        self,
        df: PolarsBackendFrame,
        *,
        id_vars: tuple[str, ...],
        value_vars: tuple[str, ...],
        variable_name: str,
        value_name: str,
    ) -> PolarsBackendFrame:
        # Prefer unpivot (polars deprecates melt on LazyFrame).
        return df.unpivot(
            index=list(id_vars),
            on=list(value_vars),
            variable_name=variable_name,
            value_name=value_name,
        )

    def join(
        self,
        left: PolarsBackendFrame,
        right: PolarsBackendFrame,
        *,
        left_on: tuple[CompiledJoinKey[pl.Expr], ...],
        right_on: tuple[CompiledJoinKey[pl.Expr], ...],
        how: str = "inner",
        suffix: str = "_right",
        options: JoinOptions | None = None,
    ) -> PolarsBackendFrame:
        allowed_how = {
            "inner",
            "left",
            "right",
            "full",
            "semi",
            "anti",
            "cross",
        }
        if how not in allowed_how:
            raise ValueError(f"Unsupported join how={how!r}")
        if how != "cross" and (not left_on or not right_on):
            raise ValueError("join left_on and right_on must be non-empty unless how='cross'")
        if how != "cross" and len(left_on) != len(right_on):
            raise ValueError("join left_on and right_on must have the same length")

        # Keep plans always-lazy: coerce eager frames to LazyFrame.
        left_lf = left.lazy() if isinstance(left, pl.DataFrame) else left
        right_lf = right.lazy() if isinstance(right, pl.DataFrame) else right

        how_lit = cast(
            Literal["inner", "left", "right", "full", "semi", "anti", "cross"],
            how,
        )
        join_kwargs: dict[str, Any] = {"how": how_lit, "suffix": suffix}

        def to_polars_key(k: CompiledJoinKey[pl.Expr]) -> str | pl.Expr:
            if k.column is not None:
                return k.column
            if k.expr is not None:
                return k.expr
            raise ValueError("CompiledJoinKey requires column or expr")

        if how == "cross":
            pass
        elif left_on is right_on:
            join_kwargs["on"] = [to_polars_key(k) for k in left_on]
        else:
            join_kwargs["left_on"] = [to_polars_key(k) for k in left_on]
            join_kwargs["right_on"] = [to_polars_key(k) for k in right_on]

        if options is not None:
            if options.coalesce is not None:
                join_kwargs["coalesce"] = options.coalesce
            if options.validate is not None:
                join_kwargs["validate"] = options.validate
            if options.join_nulls is not None:
                join_kwargs["nulls_equal"] = options.join_nulls
            if options.maintain_order is not None:
                join_kwargs["maintain_order"] = options.maintain_order
            if options.streaming is not None:
                join_kwargs["allow_parallel"] = not options.streaming
            if options.allow_parallel is not None:
                join_kwargs["allow_parallel"] = options.allow_parallel
            if (
                options.force_parallel is not None
                and "force_parallel" in _LAZYFRAME_JOIN_PARAM_NAMES
            ):
                join_kwargs["force_parallel"] = options.force_parallel
            if (
                options.engine_streaming is not None
                and "engine_streaming" in _LAZYFRAME_JOIN_PARAM_NAMES
            ):
                join_kwargs["engine_streaming"] = options.engine_streaming

        return left_lf.join(right_lf, **join_kwargs)

    def slice(
        self, df: PolarsBackendFrame, *, offset: int, length: int | None
    ) -> PolarsBackendFrame:
        return df.slice(offset, length)

    def head(self, df: PolarsBackendFrame, n: int) -> PolarsBackendFrame:
        return df.head(n)

    def tail(self, df: PolarsBackendFrame, n: int) -> PolarsBackendFrame:
        return df.tail(n)

    def concat_vertical(
        self, left: PolarsBackendFrame, right: PolarsBackendFrame
    ) -> PolarsBackendFrame:
        return pl.concat([left, right], how="vertical")

    def concat_horizontal(
        self, left: PolarsBackendFrame, right: PolarsBackendFrame
    ) -> PolarsBackendFrame:
        return pl.concat([left, right], how="horizontal")

    def pivot(
        self,
        df: PolarsBackendFrame,
        *,
        index: tuple[str, ...],
        on: str,
        values: tuple[str, ...],
        agg: str = "first",
        on_columns: tuple[str, ...] | None = None,
        separator: str = "_",
        sort_columns: bool = False,
    ) -> PolarsBackendFrame:
        allowed_agg = {
            "min",
            "max",
            "first",
            "last",
            "sum",
            "mean",
            "median",
            "len",  # polars name for "count" in pivot
            "count",
            "n_unique",
        }
        if agg not in allowed_agg:
            raise ValueError(f"Unsupported pivot agg={agg!r}")
        if agg == "count":
            agg_arg: (
                Literal[
                    "min",
                    "max",
                    "first",
                    "last",
                    "sum",
                    "mean",
                    "median",
                    "len",
                ]
                | pl.Expr
            ) = "len"
        elif agg == "n_unique":
            if len(values) != 1:
                raise ValueError("pivot agg='n_unique' requires a single values column")
            agg_arg = pl.col(values[0]).n_unique()
        else:
            agg_arg = cast(
                Literal[
                    "min",
                    "max",
                    "first",
                    "last",
                    "sum",
                    "mean",
                    "median",
                    "len",
                ],
                agg,
            )

        if sort_columns and on_columns is not None:
            on_columns = tuple(sorted(on_columns))

        if isinstance(df, pl.LazyFrame):
            if on_columns is None:
                raise ValueError("Lazy pivot requires on_columns to be provided")
            return df.pivot(
                index=list(index),
                on=on,
                values=list(values),
                aggregate_function=agg_arg,
                on_columns=list(on_columns),
                separator=separator,
            )

        return df.pivot(
            index=list(index),
            on=on,
            values=list(values),
            aggregate_function=agg_arg,
            on_columns=list(on_columns) if on_columns is not None else None,
            separator=separator,
        )

    def write_parquet(
        self,
        df: PolarsBackendFrame,
        path: str,
        *,
        compression: str = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: StorageOptions | None = None,
    ) -> None:
        out = self._collect_df(df)
        if compression not in {"uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4"}:
            raise ValueError(f"Unsupported parquet compression={compression!r}")
        comp_lit = cast(
            Literal["uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4"],
            compression,
        )
        out.write_parquet(
            path,
            compression=comp_lit,
            row_group_size=row_group_size,
            partition_by=list(partition_by) if partition_by is not None else None,
            storage_options=storage_options,
        )

    def write_csv(
        self,
        df: PolarsBackendFrame,
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: StorageOptions | None = None,
    ) -> None:
        out = self._collect_df(df)
        out.write_csv(
            path,
            separator=separator,
            include_header=include_header,
            storage_options=storage_options,
        )

    def write_ndjson(
        self, df: PolarsBackendFrame, path: str, *, storage_options: StorageOptions | None = None
    ) -> None:
        out = self._collect_df(df)
        # Polars write_ndjson does not currently accept storage_options.
        # The path may still be a cloud URI if the Polars build supports it implicitly.
        out.write_ndjson(path)

    def write_ipc(
        self,
        df: PolarsBackendFrame,
        path: str,
        *,
        compression: str = "uncompressed",
        storage_options: StorageOptions | None = None,
    ) -> None:
        out = self._collect_df(df)
        if compression not in {"uncompressed", "lz4", "zstd"}:
            raise ValueError(f"Unsupported ipc compression={compression!r}")
        comp_lit = cast(Literal["uncompressed", "lz4", "zstd"], compression)
        # Polars write_ipc does not currently accept storage_options.
        out.write_ipc(path, compression=comp_lit)

    def write_database(
        self,
        df: PolarsBackendFrame,
        *,
        table_name: str,
        connection: object,
        if_table_exists: str = "fail",
        engine: str | None = None,
    ) -> None:
        out = self._collect_df(df)
        kwargs: dict[str, Any] = {"if_table_exists": if_table_exists}
        if engine is not None:
            kwargs["engine"] = engine
        out.write_database(table_name=table_name, connection=connection, **kwargs)

    def write_excel(self, df: PolarsBackendFrame, path: str, *, worksheet: str = "Sheet1") -> None:
        out = self._collect_df(df)
        out.write_excel(workbook=path, worksheet=worksheet)

    def write_delta(
        self,
        df: PolarsBackendFrame,
        target: str,
        *,
        mode: str = "error",
        storage_options: StorageOptions | None = None,
    ) -> None:
        out = self._collect_df(df)
        if mode not in {"error", "append", "overwrite", "ignore"}:
            raise ValueError(f"Unsupported delta mode={mode!r}")
        mode_lit = cast(Literal["error", "append", "overwrite", "ignore"], mode)
        out.write_delta(target, mode=mode_lit, storage_options=storage_options)

    def write_avro(
        self,
        df: PolarsBackendFrame,
        path: str,
        *,
        compression: str = "uncompressed",
        name: str = "",
    ) -> None:
        out = self._collect_df(df)
        if compression not in {"uncompressed", "snappy", "deflate"}:
            raise ValueError(f"Unsupported avro compression={compression!r}")
        comp_lit = cast(Literal["uncompressed", "snappy", "deflate"], compression)
        out.write_avro(path, compression=comp_lit, name=name)

    def explode(
        self, df: PolarsBackendFrame, columns: Columns, *, outer: bool = False
    ) -> PolarsBackendFrame:
        if outer:
            raise PlanFrameBackendError(
                "polars adapter does not support explode(outer=True) "
                "(capabilities.explode_outer=False)"
            )
        return df.explode(list(columns))

    def unnest(self, df: PolarsBackendFrame, items: tuple[UnnestItem, ...]) -> PolarsBackendFrame:
        out = df
        for it in items:
            out = out.unnest(it.column)
        return out

    def posexplode(
        self,
        df: PolarsBackendFrame,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        outer: bool = False,
    ) -> PolarsBackendFrame:
        if outer:
            raise PlanFrameBackendError(
                "polars adapter does not support posexplode(outer=True) "
                "(capabilities.posexplode_outer=False)"
            )

        value_name = column if value is None else value
        # Build a parallel position list column, then explode both.
        df2 = df.with_columns(pl.int_ranges(0, pl.col(column).list.len()).alias(pos))
        out = df2.explode([pos, column])
        if value_name != column:
            out = out.rename({column: value_name})
        return out

    def drop_nulls_all(
        self, df: PolarsBackendFrame, subset: tuple[str, ...] | None
    ) -> PolarsBackendFrame:
        if subset is None:
            # Drop rows where *all* columns are null.
            return df.filter(~pl.all_horizontal(pl.all().is_null()))
        cols = list(subset)
        if not cols:
            return df
        mask = pl.all_horizontal([pl.col(c).is_null() for c in cols])
        return df.filter(~mask)

    def sample(
        self,
        df: PolarsBackendFrame,
        *,
        n: int | None = None,
        frac: float | None = None,
        with_replacement: bool = False,
        shuffle: bool = False,
        seed: int | None = None,
    ) -> PolarsBackendFrame:
        kwargs: dict[str, Any] = {
            "with_replacement": with_replacement,
            "shuffle": shuffle,
            "seed": seed,
        }
        if isinstance(df, pl.LazyFrame):
            raise PlanFrameBackendError(
                "Polars LazyFrame does not support sample() in this adapter. "
                "Collect first, or construct the frame with lazy=False. "
                "(capabilities.lazy_sample=False)"
            )
        df2 = self._collect_df(df)
        if n is not None:
            return df2.sample(n=n, **kwargs)
        return df2.sample(fraction=frac, **kwargs)

    def collect(
        self, df: PolarsBackendFrame, *, options: ExecutionOptions | None = None
    ) -> PolarsBackendFrame:
        # NOTE: We accept options for a stable execution-time API surface.
        # This adapter currently ignores them.
        _ = options
        return self._collect_df(df) if isinstance(df, pl.LazyFrame) else df

    def to_dicts(
        self, df: PolarsBackendFrame, *, options: ExecutionOptions | None = None
    ) -> list[dict[str, object]]:
        _ = options
        return self._collect_df(df).to_dicts()

    def to_dict(
        self, df: PolarsBackendFrame, *, options: ExecutionOptions | None = None
    ) -> dict[str, list[object]]:
        _ = options
        return self._collect_df(df).to_dict(as_series=False)  # type: ignore[return-value]
