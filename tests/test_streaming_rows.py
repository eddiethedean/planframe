from __future__ import annotations

import asyncio

from test_core_lazy_and_schema import SpyAdapter, UserDC

from planframe.execution_options import ExecutionOptions
from planframe.frame import Frame


def test_stream_dicts_matches_to_dicts() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": 2}, {"id": 2, "age": 3}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)
    out = pf.select("id").sort("id")

    assert list(out.stream_dicts()) == out.to_dicts()


def test_execution_options_forwarded_to_stream_fallback_to_to_dicts() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": 2}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)
    out = pf.select("id")

    opts = ExecutionOptions(streaming=True, engine_streaming=False)
    _ = list(out.stream_dicts(options=opts))
    assert ("to_dicts", opts) in adapter.calls


def test_stream_models_matches_collect() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": 2}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)
    out = pf.select("id", "age")

    rows = out.collect(name="Row")
    streamed = list(out.stream(name="Row"))
    assert len(rows) == len(streamed) == 1
    assert rows[0].id == streamed[0].id == 1
    assert rows[0].age == streamed[0].age == 2


def test_astream_dicts_matches_ato_dicts() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": 2}, {"id": 2, "age": 3}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)
    out = pf.select("id").sort("id")

    async def run() -> None:
        a = [r async for r in out.astream_dicts()]
        b = await out.ato_dicts()
        assert a == b

    asyncio.run(run())
