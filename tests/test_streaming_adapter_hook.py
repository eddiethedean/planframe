from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import Any

from test_core_lazy_and_schema import SpyAdapter

from planframe.backend.adapter import BaseAdapter
from planframe.backend.io import AdapterRowStreamer
from planframe.execution_options import ExecutionOptions
from planframe.frame import Frame


class _S:
    __planframe_model__ = True
    x: int


class _StreamingSpyAdapter(
    BaseAdapter[list[dict[str, Any]], object], AdapterRowStreamer[list[dict[str, Any]]]
):
    name = "streaming-spy"

    def __init__(self) -> None:
        self.seen: list[tuple[str, Any]] = []

    # ---- streaming hook ----
    def stream_dicts(
        self, df: list[dict[str, Any]], *, options: ExecutionOptions | None = None
    ) -> Iterator[dict[str, object]]:
        self.seen.append(("stream_dicts", options))
        yield from df

    def astream_dicts(
        self, df: list[dict[str, Any]], *, options: ExecutionOptions | None = None
    ) -> AsyncIterator[dict[str, object]]:
        self.seen.append(("astream_dicts", options))

        async def gen() -> AsyncIterator[dict[str, object]]:
            for r in df:
                yield r

        return gen()

    # ---- minimal BaseAdapter impls to satisfy ABC (unused) ----
    def select(self, df: list[dict[str, Any]], columns: tuple[str, ...]) -> list[dict[str, Any]]:
        return df

    def project(self, df: list[dict[str, Any]], items: tuple[Any, ...]) -> list[dict[str, Any]]:
        return df

    def drop(
        self, df: list[dict[str, Any]], columns: tuple[str, ...], *, strict: bool = True
    ) -> list[dict[str, Any]]:
        return df

    def rename(
        self, df: list[dict[str, Any]], mapping: dict[str, str], *, strict: bool = True
    ) -> list[dict[str, Any]]:
        return df

    def with_column(
        self, df: list[dict[str, Any]], name: str, expr: object
    ) -> list[dict[str, Any]]:
        return df

    def cast(self, df: list[dict[str, Any]], name: str, dtype: object) -> list[dict[str, Any]]:
        return df

    def with_row_count(
        self, df: list[dict[str, Any]], *, name: str = "row_nr", offset: int = 0
    ) -> list[dict[str, Any]]:
        return df

    def filter(self, df: list[dict[str, Any]], predicate: object) -> list[dict[str, Any]]:
        return df

    def sort(
        self,
        df: list[dict[str, Any]],
        keys: tuple[Any, ...],
        *,
        descending: tuple[bool, ...],
        nulls_last: tuple[bool, ...],
    ) -> list[dict[str, Any]]:
        return df

    def unique(
        self,
        df: list[dict[str, Any]],
        subset: tuple[str, ...] | None,
        *,
        keep: str = "first",
        maintain_order: bool = False,
    ) -> list[dict[str, Any]]:
        return df

    def duplicated(
        self,
        df: list[dict[str, Any]],
        subset: tuple[str, ...] | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> list[dict[str, Any]]:
        return df

    def group_by_agg(
        self, df: list[dict[str, Any]], *, keys: tuple[Any, ...], named_aggs: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return df

    def group_by_dynamic_agg(self, df: list[dict[str, Any]], **_: Any) -> list[dict[str, Any]]:
        return df

    def rolling_agg(self, df: list[dict[str, Any]], **_: Any) -> list[dict[str, Any]]:
        return df

    def drop_nulls(
        self,
        df: list[dict[str, Any]],
        subset: tuple[str, ...] | None,
        *,
        how: str = "any",
        threshold: int | None = None,
    ) -> list[dict[str, Any]]:
        return df

    def fill_null(
        self,
        df: list[dict[str, Any]],
        value: Any,
        subset: tuple[str, ...] | None,
        *,
        strategy: str | None = None,
    ) -> list[dict[str, Any]]:
        return df

    def melt(self, df: list[dict[str, Any]], **_: Any) -> list[dict[str, Any]]:
        return df

    def join(
        self, left: list[dict[str, Any]], right: list[dict[str, Any]], **_: Any
    ) -> list[dict[str, Any]]:
        return left

    def slice(
        self, df: list[dict[str, Any]], *, offset: int, length: int | None
    ) -> list[dict[str, Any]]:
        return df

    def head(self, df: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
        return df

    def tail(self, df: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
        return df

    def concat_vertical(
        self, left: list[dict[str, Any]], right: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        return left + right

    def concat_horizontal(
        self, left: list[dict[str, Any]], right: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        return left

    def pivot(self, df: list[dict[str, Any]], **_: Any) -> list[dict[str, Any]]:
        return df

    def write_parquet(self, df: list[dict[str, Any]], path: str, **_: Any) -> None:
        _ = df, path

    def write_csv(self, df: list[dict[str, Any]], path: str, **_: Any) -> None:
        _ = df, path

    def write_ndjson(self, df: list[dict[str, Any]], path: str, **_: Any) -> None:
        _ = df, path

    def write_ipc(self, df: list[dict[str, Any]], path: str, **_: Any) -> None:
        _ = df, path

    def write_database(self, df: list[dict[str, Any]], **_: Any) -> None:
        _ = df

    def write_excel(self, df: list[dict[str, Any]], path: str, **_: Any) -> None:
        _ = df, path

    def write_delta(self, df: list[dict[str, Any]], target: str, **_: Any) -> None:
        _ = df, target

    def write_avro(self, df: list[dict[str, Any]], path: str, **_: Any) -> None:
        _ = df, path

    def explode(
        self, df: list[dict[str, Any]], columns: tuple[str, ...], *, outer: bool = False
    ) -> list[dict[str, Any]]:
        return df

    def unnest(self, df: list[dict[str, Any]], items: tuple[Any, ...]) -> list[dict[str, Any]]:
        return df

    def posexplode(self, df: list[dict[str, Any]], column: str, **_: Any) -> list[dict[str, Any]]:
        return df

    def drop_nulls_all(
        self, df: list[dict[str, Any]], subset: tuple[str, ...] | None
    ) -> list[dict[str, Any]]:
        return df

    def sample(self, df: list[dict[str, Any]], **_: Any) -> list[dict[str, Any]]:
        return df

    def compile_expr(self, expr: object, *, schema: object | None = None, ctx: object | None = None) -> object:
        _ = expr, schema, ctx
        return expr

    def collect(
        self, df: list[dict[str, Any]], *, options: ExecutionOptions | None = None
    ) -> list[dict[str, Any]]:
        self.seen.append(("collect", options))
        return df

    def to_dicts(
        self, df: list[dict[str, Any]], *, options: ExecutionOptions | None = None
    ) -> list[dict[str, object]]:
        self.seen.append(("to_dicts", options))
        return df  # type: ignore[return-value]

    def to_dict(
        self, df: list[dict[str, Any]], *, options: ExecutionOptions | None = None
    ) -> dict[str, list[object]]:
        self.seen.append(("to_dict", options))
        return {}


def test_stream_dicts_prefers_adapter_streamer_hook() -> None:
    adapter = _StreamingSpyAdapter()
    pf = Frame.source([{"x": 1}, {"x": 2}], adapter=adapter, schema=_S)

    rows = list(pf.stream_dicts(options=ExecutionOptions(streaming=True)))
    assert rows == [{"x": 1}, {"x": 2}]
    assert adapter.seen[0][0] == "stream_dicts"


class _SyncOnlyStreamAdapter(SpyAdapter):
    """Defines ``stream_dicts`` but not ``astream_dicts`` — must not qualify as ``AdapterRowStreamer``."""

    def stream_dicts(
        self, df: list[dict[str, Any]], *, options: ExecutionOptions | None = None
    ) -> Iterator[dict[str, object]]:
        raise AssertionError("sync-only stream_dicts must not be used when protocol incomplete")


def test_sync_only_streaming_falls_back_to_to_dicts() -> None:
    """Adapters with only sync ``stream_dicts`` are not ``AdapterRowStreamer``; use materialized path."""
    adapter = _SyncOnlyStreamAdapter()
    assert not isinstance(adapter, AdapterRowStreamer)

    pf = Frame.source([{"x": 1}], adapter=adapter, schema=_S)
    rows = list(pf.stream_dicts(options=ExecutionOptions(streaming=True)))
    assert rows == [{"x": 1}]
    names = [c[0] for c in adapter.calls]
    assert "to_dicts" in names


def test_astream_dicts_prefers_adapter_streamer_hook() -> None:
    adapter = _StreamingSpyAdapter()
    pf = Frame.source([{"x": 1}], adapter=adapter, schema=_S)

    async def run() -> list[dict[str, object]]:
        return [r async for r in pf.astream_dicts(options=ExecutionOptions(streaming=True))]

    rows = asyncio.run(run())
    assert rows == [{"x": 1}]
    assert adapter.seen[0][0] == "astream_dicts"
