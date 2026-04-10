from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any, Literal, cast

import pandas as pd

from planframe.backend.adapter import (
    AdapterCapabilities,
    BaseAdapter,
    Columns,
    CompiledJoinKey,
    CompiledProjectItem,
    CompiledSortKey,
    CompileExprContext,
)
from planframe.backend.errors import PlanFrameBackendError
from planframe.execution_options import ExecutionOptions
from planframe.expr.api import Expr
from planframe.plan.join_options import JoinOptions
from planframe.plan.nodes import UnnestItem
from planframe.schema.ir import Schema
from planframe.typing.scalars import Scalar
from planframe.typing.storage import StorageOptions
from planframe_pandas.compile_expr import AggExprSpec, PandasExpr, compile_expr

PandasBackendFrame = pd.DataFrame
PandasBackendExpr = PandasExpr | AggExprSpec


@dataclass(frozen=True, slots=True)
class _JoinKey:
    name: str
    series: pd.Series


class PandasAdapter(BaseAdapter[PandasBackendFrame, PandasBackendExpr]):
    name = "pandas"

    # ---- AdapterReader surface (used by PandasFrame classmethods) ----
    def scan_parquet(
        self,
        path: str,
        *,
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> pd.DataFrame:
        # pandas is eager; this is a convenience loader, not a lazy scan.
        _ = hive_partitioning
        try:
            return pd.read_parquet(path, storage_options=storage_options)  # type: ignore[call-arg]
        except ImportError as e:
            raise PlanFrameBackendError(
                "Parquet support requires installing planframe-pandas[parquet]"
            ) from e

    def scan_parquet_dataset(
        self, path_or_glob: str, *, storage_options: StorageOptions | None = None
    ) -> pd.DataFrame:
        _ = path_or_glob, storage_options
        raise PlanFrameBackendError(
            "pandas adapter does not implement scan_parquet_dataset; "
            "use scan_parquet on a single file or implement dataset loading externally."
        )

    def scan_csv(self, path: str, *, storage_options: StorageOptions | None = None) -> pd.DataFrame:
        return pd.read_csv(path, storage_options=storage_options)  # type: ignore[call-arg]

    def scan_ndjson(
        self, path: str, *, storage_options: StorageOptions | None = None
    ) -> pd.DataFrame:
        return pd.read_json(path, lines=True, storage_options=storage_options)  # type: ignore[call-arg]

    def scan_ipc(
        self,
        path: str,
        *,
        hive_partitioning: bool | None = None,
        storage_options: StorageOptions | None = None,
    ) -> pd.DataFrame:
        _ = path, hive_partitioning, storage_options
        raise PlanFrameBackendError("pandas adapter does not implement scan_ipc")

    def scan_delta(
        self,
        source: str,
        *,
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> pd.DataFrame:
        _ = source, version, storage_options
        raise PlanFrameBackendError("pandas adapter does not implement scan_delta")

    def read_delta(
        self,
        source: str,
        *,
        version: int | str | None = None,
        storage_options: StorageOptions | None = None,
    ) -> pd.DataFrame:
        _ = source, version, storage_options
        raise PlanFrameBackendError("pandas adapter does not implement read_delta")

    def read_excel(self, path: str, *, sheet_name: str | None = None) -> pd.DataFrame:
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
        return df

    def read_avro(self, path: str) -> pd.DataFrame:
        _ = path
        raise PlanFrameBackendError("pandas adapter does not implement read_avro")

    def read_database(self, query: str, *, connection: object) -> pd.DataFrame:
        return pd.read_sql_query(query, con=connection)  # type: ignore[arg-type]

    def read_database_uri(
        self,
        query: str,
        *,
        uri: str,
        engine: Literal["connectorx", "adbc"] | None = None,
    ) -> pd.DataFrame:
        _ = query, uri
        if engine is not None:
            raise PlanFrameBackendError(
                "pandas adapter does not support engine= for read_database_uri"
            )
        raise PlanFrameBackendError(
            "pandas adapter does not implement read_database_uri. "
            "Use read_database(query, connection=...) with a DBAPI/SQLAlchemy connection."
        )

    @property
    def capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            explode_outer=False,
            posexplode_outer=True,
            lazy_sample=True,
            scan_delta=False,
            read_delta=False,
            sink_delta=False,
            read_avro=False,
            sink_avro=False,
            read_excel=False,
            sink_excel=True,
            read_database_uri=False,
            sink_database=True,
            storage_options=False,
        )

    def select(self, df: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
        return df.loc[:, list(columns)].copy()

    def project(
        self, df: pd.DataFrame, items: tuple[CompiledProjectItem[PandasBackendExpr], ...]
    ) -> pd.DataFrame:
        out: dict[str, Any] = {}
        for it in items:
            if it.from_column is not None:
                out[it.name] = df[it.from_column]
            elif it.expr is not None:
                expr = it.expr
                if isinstance(expr, AggExprSpec):
                    raise PlanFrameBackendError(
                        "AggExpr is only supported inside group_by(...).agg(...)"
                    )
                out[it.name] = cast(PandasExpr, expr)(df)
            else:
                raise ValueError("CompiledProjectItem requires from_column or expr")
        return pd.DataFrame(out, copy=False).copy()

    def drop(
        self, df: pd.DataFrame, columns: tuple[str, ...], *, strict: bool = True
    ) -> pd.DataFrame:
        missing = [c for c in columns if c not in df.columns]
        if missing and strict:
            raise KeyError(f"Missing columns: {missing}")
        cols = [c for c in columns if c in df.columns]
        return df.drop(columns=cols).copy()

    def rename(
        self, df: pd.DataFrame, mapping: dict[str, str], *, strict: bool = True
    ) -> pd.DataFrame:
        if strict:
            missing = [k for k in mapping if k not in df.columns]
            if missing:
                raise KeyError(f"Missing columns: {missing}")
        effective = (
            {k: v for k, v in mapping.items() if (not strict) and k in df.columns}
            if not strict
            else mapping
        )
        return df.rename(columns=effective).copy()

    def with_column(self, df: pd.DataFrame, name: str, expr: PandasBackendExpr) -> pd.DataFrame:
        if isinstance(expr, AggExprSpec):
            raise PlanFrameBackendError("AggExpr is only supported inside group_by(...).agg(...)")
        out = df.copy()
        out[name] = cast(PandasExpr, expr)(df)
        return out

    def cast(self, df: pd.DataFrame, name: str, dtype: object) -> pd.DataFrame:
        out = df.copy()
        out[name] = out[name].astype(dtype)  # type: ignore[call-arg]
        return out

    def with_row_count(
        self, df: pd.DataFrame, *, name: str = "row_nr", offset: int = 0
    ) -> pd.DataFrame:
        out = df.copy()
        out[name] = range(offset, offset + len(out))
        return out

    def filter(self, df: pd.DataFrame, predicate: PandasBackendExpr) -> pd.DataFrame:
        if isinstance(predicate, AggExprSpec):
            raise PlanFrameBackendError("AggExpr is only supported inside group_by(...).agg(...)")
        mask = cast(PandasExpr, predicate)(df)
        if not isinstance(mask, pd.Series):
            raise TypeError("predicate must compile to a pandas Series[bool]")
        return df.loc[mask].copy()

    def sort(
        self,
        df: pd.DataFrame,
        keys: tuple[CompiledSortKey[PandasBackendExpr], ...],
        *,
        descending: tuple[bool, ...],
        nulls_last: tuple[bool, ...],
    ) -> pd.DataFrame:
        if not keys:
            return df

        by_cols: list[str] = []
        asc: list[bool] = []
        na_position = "last" if all(nulls_last) else "first"
        out = df.copy()

        for i, k in enumerate(keys):
            if k.column is not None:
                colname = k.column
                series = out[colname]
            elif k.expr is not None:
                expr = k.expr
                if isinstance(expr, AggExprSpec):
                    raise PlanFrameBackendError("AggExpr is not supported in sort keys")
                v = cast(PandasExpr, expr)(out)
                series = (
                    v if isinstance(v, pd.Series) else pd.Series([v] * len(out), index=out.index)
                )
                colname = f"__pf_sort_{i}"
                out[colname] = series
            else:
                raise ValueError("CompiledSortKey requires column or expr")

            by_cols.append(colname)
            asc.append(not descending[i])

        out2 = out.sort_values(by=by_cols, ascending=asc, na_position=na_position, kind="mergesort")
        # Drop temporary sort columns.
        tmp_cols = [c for c in out2.columns if c.startswith("__pf_sort_")]
        if tmp_cols:
            out2 = out2.drop(columns=tmp_cols)
        return out2

    def unique(
        self,
        df: pd.DataFrame,
        subset: tuple[str, ...] | None,
        *,
        keep: str = "first",
        maintain_order: bool = False,
    ) -> pd.DataFrame:
        out = df.drop_duplicates(
            subset=list(subset) if subset is not None else None,
            keep=cast(Literal["first", "last", False], keep),
        )
        return out.copy()

    def duplicated(
        self,
        df: pd.DataFrame,
        subset: tuple[str, ...] | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> pd.DataFrame:
        mask = df.duplicated(
            subset=list(subset) if subset is not None else None,
            keep=cast(Literal["first", "last", False], keep),
        )
        return pd.DataFrame({out_name: mask})

    def compile_expr(
        self,
        expr: object,
        *,
        schema: Schema | None = None,
        ctx: CompileExprContext | None = None,
    ) -> PandasBackendExpr:
        if not isinstance(expr, Expr):
            raise TypeError(f"Expected PlanFrame Expr, got {type(expr)!r}")
        if ctx is None:
            ctx = CompileExprContext(schema=schema)
        return compile_expr(
            cast(Expr[Any], expr),
            dtype_for=lambda n: self.resolve_dtype(n, ctx=ctx),
        )

    def group_by_agg(
        self,
        df: pd.DataFrame,
        *,
        keys: tuple[CompiledJoinKey[PandasBackendExpr], ...],
        named_aggs: dict[str, tuple[str, str] | PandasBackendExpr],
    ) -> pd.DataFrame:
        if not keys:
            raise ValueError("keys must be non-empty")

        key_cols: list[str] = []
        tmp_cols: list[str] = []
        out = df.copy()

        for i, k in enumerate(keys):
            if k.column is not None:
                key_cols.append(k.column)
            elif k.expr is not None:
                expr = k.expr
                if isinstance(expr, AggExprSpec):
                    raise PlanFrameBackendError("AggExpr is not supported as a group key")
                v = cast(PandasExpr, expr)(out)
                series = (
                    v if isinstance(v, pd.Series) else pd.Series([v] * len(out), index=out.index)
                )
                name = f"__pf_g{i}"
                out[name] = series
                key_cols.append(name)
                tmp_cols.append(name)
            else:
                raise ValueError("CompiledJoinKey requires column or expr")

        grouped = out.groupby(key_cols, dropna=False, sort=False)

        agg_out: dict[str, Any] = {}
        for out_name, spec in named_aggs.items():
            if (
                isinstance(spec, tuple)
                and len(spec) == 2
                and isinstance(spec[0], str)
                and isinstance(spec[1], str)
            ):
                op, col = spec
                s = grouped[col]
                if op == "count":
                    agg_out[out_name] = s.count()
                elif op == "sum":
                    agg_out[out_name] = s.sum()
                elif op == "mean":
                    agg_out[out_name] = s.mean()
                elif op == "min":
                    agg_out[out_name] = s.min()
                elif op == "max":
                    agg_out[out_name] = s.max()
                elif op == "n_unique":
                    agg_out[out_name] = s.nunique(dropna=False)
                else:
                    raise ValueError(f"Unsupported agg op: {op!r}")
            else:
                expr = spec
                if isinstance(expr, AggExprSpec):
                    ser_v = expr.inner(out)
                    ser = (
                        ser_v
                        if isinstance(ser_v, pd.Series)
                        else pd.Series([ser_v] * len(out), index=out.index)
                    )
                    tmp = pd.DataFrame({"__v": ser, **{k: out[k] for k in key_cols}})
                    g2 = tmp.groupby(key_cols, dropna=False, sort=False)["__v"]
                    if expr.op == "sum":
                        agg_out[out_name] = g2.sum()
                    elif expr.op == "mean":
                        agg_out[out_name] = g2.mean()
                    elif expr.op == "min":
                        agg_out[out_name] = g2.min()
                    elif expr.op == "max":
                        agg_out[out_name] = g2.max()
                    elif expr.op == "count":
                        agg_out[out_name] = g2.count()
                    elif expr.op == "n_unique":
                        agg_out[out_name] = g2.nunique(dropna=False)
                    else:
                        raise ValueError(f"Unsupported AggExpr op: {expr.op!r}")
                else:
                    raise PlanFrameBackendError(
                        "group_by_agg expects tuple aggs or compiled AggExpr"
                    )

        res = pd.concat(agg_out, axis=1)
        res = res.reset_index()
        if tmp_cols:
            out = out.drop(columns=tmp_cols)
        return res

    def group_by_dynamic_agg(
        self,
        df: pd.DataFrame,
        *,
        index_column: str,
        every: str,
        period: str | None = None,
        by: tuple[str, ...] | None = None,
        named_aggs: dict[str, tuple[str, str] | PandasBackendExpr],
    ) -> pd.DataFrame:
        raise PlanFrameBackendError("pandas adapter does not implement group_by_dynamic_agg")

    def rolling_agg(
        self,
        df: pd.DataFrame,
        *,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: tuple[str, ...] | None = None,
        min_periods: int = 1,
    ) -> pd.DataFrame:
        raise PlanFrameBackendError("pandas adapter does not implement rolling_agg")

    def melt(
        self,
        df: pd.DataFrame,
        *,
        id_vars: tuple[str, ...],
        value_vars: tuple[str, ...],
        variable_name: str,
        value_name: str,
    ) -> pd.DataFrame:
        return pd.melt(
            df,
            id_vars=list(id_vars),
            value_vars=list(value_vars),
            var_name=variable_name,
            value_name=value_name,
        )

    def pivot(
        self,
        df: pd.DataFrame,
        *,
        index: tuple[str, ...],
        on: str,
        values: tuple[str, ...],
        agg: str = "first",
        on_columns: tuple[str, ...] | None = None,
        separator: str = "_",
        sort_columns: bool = False,
    ) -> pd.DataFrame:
        if agg == "len":
            aggfunc: str | Any = "size"
        else:
            aggfunc = agg
        pt = df.pivot_table(
            index=list(index),
            columns=on,
            values=list(values) if len(values) != 1 else values[0],
            aggfunc=aggfunc,
            dropna=False,
        )
        pt = pt.reset_index()
        if isinstance(pt.columns, pd.MultiIndex):
            pt.columns = [
                separator.join([str(c) for c in tup if c != ""])
                for tup in pt.columns.to_flat_index()
            ]
        else:
            pt.columns = [str(c) for c in pt.columns]

        if sort_columns and on_columns is not None:
            on_columns = tuple(sorted(on_columns))

        if on_columns is not None:
            # Ensure columns exist (fill with NA) and order them.
            if len(values) == 1:
                for c in on_columns:
                    if c not in pt.columns:
                        pt[c] = pd.NA
                ordered = list(index) + list(on_columns)
                pt = pt.loc[:, ordered]
            else:
                want = [f"{v}{separator}{c}" for v in values for c in on_columns]
                for c in want:
                    if c not in pt.columns:
                        pt[c] = pd.NA
                ordered = list(index) + want
                pt = pt.loc[:, ordered]
        return pt

    def explode(self, df: pd.DataFrame, columns: Columns, *, outer: bool = False) -> pd.DataFrame:
        if outer:
            raise PlanFrameBackendError("pandas adapter does not support explode(outer=True)")
        return df.explode(list(columns)).reset_index(drop=True)

    def unnest(self, df: pd.DataFrame, items: tuple[UnnestItem, ...]) -> pd.DataFrame:
        out = df.copy()
        for it in items:
            ser = out[it.column]
            expanded = pd.json_normalize(ser).reindex(out.index)
            expanded = expanded.loc[:, list(it.fields)]
            for c in expanded.columns:
                if c in out.columns and c != it.column:
                    raise PlanFrameBackendError(f"unnest would overwrite existing column: {c!r}")
            out = out.drop(columns=[it.column])
            for c in expanded.columns:
                out[c] = expanded[c]
        return out

    def posexplode(
        self,
        df: pd.DataFrame,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        outer: bool = False,
    ) -> pd.DataFrame:
        value_name = column if value is None else value
        out = df.copy()

        def _as_list(x: Any) -> list[Any]:
            if isinstance(x, (list, tuple)):
                return list(x)
            if x is None:
                return []
            return [x]

        def _as_list_outer(x: Any) -> list[Any]:
            xs = _as_list(x)
            return xs if xs else [None]

        seqs = out[column].map(_as_list_outer if outer else _as_list)
        out[column] = seqs
        out[pos] = seqs.map(lambda xs: list(range(len(xs))))
        out = out.explode([pos, column], ignore_index=True)
        if value_name != column:
            out = out.rename(columns={column: value_name})
        return out

    def join(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        *,
        left_on: tuple[CompiledJoinKey[PandasBackendExpr], ...],
        right_on: tuple[CompiledJoinKey[PandasBackendExpr], ...],
        how: str = "inner",
        suffix: str = "_right",
        options: JoinOptions | None = None,
    ) -> pd.DataFrame:
        if how == "cross":
            ltmp = left.copy()
            rtmp = right.copy()
            key = "__pf_cross"
            ltmp[key] = 1
            rtmp[key] = 1
            out = ltmp.merge(rtmp, on=key, how="inner", suffixes=("", suffix))
            return out.drop(columns=[key])

        def _materialize_keys(
            df: pd.DataFrame, keys: tuple[CompiledJoinKey[PandasBackendExpr], ...], prefix: str
        ) -> tuple[list[str], pd.DataFrame]:
            out = df.copy()
            cols: list[str] = []
            for i, k in enumerate(keys):
                if k.column is not None:
                    cols.append(k.column)
                elif k.expr is not None:
                    expr = k.expr
                    if isinstance(expr, AggExprSpec):
                        raise PlanFrameBackendError("AggExpr is not supported in join keys")
                    v = cast(PandasExpr, expr)(out)
                    series = (
                        v
                        if isinstance(v, pd.Series)
                        else pd.Series([v] * len(out), index=out.index)
                    )
                    name = f"__pf_join_{prefix}{i}"
                    out[name] = series
                    cols.append(name)
                else:
                    raise ValueError("CompiledJoinKey requires column or expr")
            return cols, out

        lcols, ldf = _materialize_keys(left, left_on, "l")
        rcols, rdf = _materialize_keys(right, right_on, "r")
        out = ldf.merge(
            rdf,
            how=cast(Literal["left", "right", "inner", "outer", "cross"], how),
            left_on=lcols,
            right_on=rcols,
            suffixes=("", suffix),
        )
        # Match PlanFrame semantics: when joining on different key names, drop the RHS key columns.
        for lc, rc in zip(lcols, rcols, strict=False):
            if lc != rc and rc in out.columns and rc in right.columns:
                out = out.drop(columns=[rc])
        drop_cols = [c for c in out.columns if c.startswith("__pf_join_")]
        if drop_cols:
            out = out.drop(columns=drop_cols)
        return out

    def slice(self, df: pd.DataFrame, *, offset: int, length: int | None) -> pd.DataFrame:
        if length is None:
            return df.iloc[offset:].copy()
        return df.iloc[offset : offset + length].copy()

    def head(self, df: pd.DataFrame, n: int) -> pd.DataFrame:
        return df.head(n).copy()

    def tail(self, df: pd.DataFrame, n: int) -> pd.DataFrame:
        return df.tail(n).copy()

    def concat_vertical(self, left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([left, right], axis=0, ignore_index=True)

    def concat_horizontal(self, left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([left.reset_index(drop=True), right.reset_index(drop=True)], axis=1)

    def drop_nulls(
        self,
        df: pd.DataFrame,
        subset: tuple[str, ...] | None,
        *,
        how: Literal["any", "all"] = "any",
        threshold: int | None = None,
    ) -> pd.DataFrame:
        # pandas rejects passing both how= and thresh=; thresh-only matches Polars threshold path.
        kwargs: dict[str, Any] = {}
        if subset is not None:
            kwargs["subset"] = list(subset)
        if threshold is not None:
            kwargs["thresh"] = threshold
        else:
            kwargs["how"] = how
        return df.dropna(**kwargs).copy()

    def drop_nulls_all(self, df: pd.DataFrame, subset: tuple[str, ...] | None) -> pd.DataFrame:
        return df.dropna(
            subset=list(subset) if subset is not None else None,
            how="all",
        ).copy()

    def fill_null(
        self,
        df: pd.DataFrame,
        value: Scalar | PandasBackendExpr | None,
        subset: tuple[str, ...] | None,
        *,
        strategy: str | None = None,
    ) -> pd.DataFrame:
        if strategy is not None:
            if strategy not in ("forward", "backward"):
                raise PlanFrameBackendError(
                    f"Unsupported fill_null strategy for pandas: {strategy!r}"
                )
            out = df.copy()
            cols = list(subset) if subset is not None else list(out.columns)
            for c in cols:
                if strategy == "forward":
                    out[c] = out[c].ffill()
                else:
                    out[c] = out[c].bfill()
            return out
        if value is None:
            raise PlanFrameBackendError("fill_null requires value when strategy is not provided")

        out = df.copy()
        cols = list(subset) if subset is not None else list(out.columns)
        if isinstance(value, AggExprSpec):
            raise PlanFrameBackendError("AggExpr is only supported inside group_by(...).agg(...)")
        for c in cols:
            if callable(value):
                out[c] = out[c].fillna(cast(PandasExpr, value)(out))
            else:
                out[c] = out[c].fillna(value)
        return out

    def sample(
        self,
        df: pd.DataFrame,
        *,
        n: int | None = None,
        frac: float | None = None,
        with_replacement: bool = False,
        shuffle: bool = False,
        seed: int | None = None,
    ) -> pd.DataFrame:
        return df.sample(n=n, frac=frac, replace=with_replacement, random_state=seed).copy()

    def collect(self, df: pd.DataFrame, *, options: ExecutionOptions | None = None) -> pd.DataFrame:
        _ = options
        return df

    def to_dicts(
        self, df: pd.DataFrame, *, options: ExecutionOptions | None = None
    ) -> list[dict[str, object]]:
        _ = options
        return cast(list[dict[str, object]], df.to_dict(orient="records"))

    def to_dict(
        self, df: pd.DataFrame, *, options: ExecutionOptions | None = None
    ) -> dict[str, list[object]]:
        _ = options
        return cast(dict[str, list[object]], df.to_dict(orient="list"))

    def stream_dicts(
        self, df: pd.DataFrame, *, options: ExecutionOptions | None = None
    ) -> Iterator[dict[str, object]]:
        _ = options
        cols = list(df.columns)
        for row in df.itertuples(index=False, name=None):
            yield dict(zip(cols, row, strict=False))

    async def astream_dicts(
        self, df: pd.DataFrame, *, options: ExecutionOptions | None = None
    ) -> AsyncIterator[dict[str, object]]:
        for row in self.stream_dicts(df, options=options):
            yield row

    def write_parquet(
        self,
        df: pd.DataFrame,
        path: str,
        *,
        compression: str = "snappy",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: StorageOptions | None = None,
    ) -> None:
        try:
            # BaseAdapter uses plain str; pandas stubs narrow compression literals.
            df.to_parquet(path, compression=cast(Any, compression), index=False)
        except ImportError as e:
            raise PlanFrameBackendError(
                "Parquet support requires installing planframe-pandas[parquet]"
            ) from e

    def write_csv(
        self,
        df: pd.DataFrame,
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: StorageOptions | None = None,
    ) -> None:
        df.to_csv(path, sep=separator, header=include_header, index=False)

    def write_ndjson(
        self,
        df: pd.DataFrame,
        path: str,
        *,
        storage_options: StorageOptions | None = None,
    ) -> None:
        df.to_json(path, orient="records", lines=True)

    def write_ipc(
        self,
        df: pd.DataFrame,
        path: str,
        *,
        compression: str = "uncompressed",
        storage_options: StorageOptions | None = None,
    ) -> None:
        raise PlanFrameBackendError("IPC writing is not implemented for pandas adapter")

    def write_excel(
        self,
        df: pd.DataFrame,
        path: str,
        *,
        worksheet: str = "Sheet1",
    ) -> None:
        try:
            df.to_excel(path, index=False, sheet_name=worksheet)  # type: ignore[call-arg]
        except ImportError as e:
            raise PlanFrameBackendError(
                "Excel support requires installing planframe-pandas[excel]"
            ) from e

    def write_database(
        self,
        df: pd.DataFrame,
        *,
        table_name: str,
        connection: object,
        if_table_exists: str = "fail",
        engine: str | None = None,
    ) -> None:
        if if_table_exists not in {"fail", "replace", "append"}:
            raise ValueError("if_table_exists must be 'fail', 'replace', or 'append'")
        df.to_sql(
            table_name,
            con=connection,  # type: ignore[arg-type]
            if_exists=cast(Literal["fail", "replace", "append"], if_table_exists),
            index=False,
        )

    def write_delta(
        self,
        df: pd.DataFrame,
        target: str,
        *,
        mode: str = "error",
        storage_options: StorageOptions | None = None,
    ) -> None:
        raise PlanFrameBackendError("Delta writing is not implemented for pandas adapter")

    def write_avro(
        self,
        df: pd.DataFrame,
        path: str,
        *,
        compression: str = "uncompressed",
        name: str = "",
    ) -> None:
        raise PlanFrameBackendError("Avro writing is not implemented for pandas adapter")
