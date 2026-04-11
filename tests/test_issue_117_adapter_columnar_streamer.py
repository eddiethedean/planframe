"""Issue #117: AdapterColumnarStreamer protocol is structural + runtime_checkable."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator

from planframe.backend.io import AdapterColumnarStreamer
from planframe.execution_options import ExecutionOptions


class _ChunkAdapter:
    name = "chunky"

    def iter_columnar_chunks(
        self, df: object, *, options: ExecutionOptions | None = None
    ) -> Iterator[dict[str, list[object]]]:
        _ = df, options
        yield {"a": [1, 2]}

    async def aiter_columnar_chunks(
        self, df: object, *, options: ExecutionOptions | None = None
    ) -> AsyncIterator[dict[str, list[object]]]:
        _ = df, options
        yield {"a": [3, 4]}


def test_adapter_columnar_streamer_isinstance() -> None:
    adapter = _ChunkAdapter()
    assert isinstance(adapter, AdapterColumnarStreamer)


def test_aiter_columnar_chunks_runs() -> None:
    adapter = _ChunkAdapter()

    async def _run() -> None:
        chunks = [c async for c in adapter.aiter_columnar_chunks(None)]
        assert chunks == [{"a": [3, 4]}]

    asyncio.run(_run())
