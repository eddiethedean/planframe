"""Async materialization on Frame and BaseAdapter (issue #15)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from test_core_lazy_and_schema import SpyAdapter, UserDC

from planframe.expr import col, eq, lit
from planframe.frame import Frame


class AsyncSpyAdapter(SpyAdapter):
    """Spy that records ``acollect`` and yields before delegating to sync collect."""

    async def acollect(
        self, df: list[dict[str, Any]], *, options: object | None = None
    ) -> list[dict[str, Any]]:
        self.calls.append(("acollect", options))
        await asyncio.sleep(0)
        return self.collect(df, options=options)


def test_acollect_default_adapter_matches_collect() -> None:
    async def run() -> None:
        adapter = SpyAdapter()
        data = [{"id": 1, "age": 2}]
        pf = Frame.source(data, adapter=adapter, schema=UserDC)
        out = await pf.select("id").acollect()
        sync_out = pf.select("id").collect()
        assert len(out) == 1 and len(sync_out) == 1
        assert out[0].id == 1 and sync_out[0].id == 1
        assert "acollect" not in [c[0] for c in adapter.calls]

    asyncio.run(run())


def test_acollect_async_adapter_path() -> None:
    adapter = AsyncSpyAdapter()
    data = [{"id": 1, "age": 2}, {"id": 2, "age": 3}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    async def run() -> None:
        out = await pf.select("id", "age").filter(eq(col("id"), lit(1))).acollect()
        assert len(out) == 1
        assert out[0].id == 1 and out[0].age == 2
        names = [c[0] for c in adapter.calls]
        assert "acollect" in names
        assert names[-1] == "to_dicts"

    asyncio.run(run())


def test_ato_dicts_matches_to_dicts() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": 2}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    async def run() -> None:
        a = await pf.ato_dicts()
        b = pf.to_dicts()
        assert a == b

    asyncio.run(run())


def test_ato_dict_matches_to_dict() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": 2}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    async def run() -> None:
        a = await pf.ato_dict()
        b = pf.to_dict()
        assert a == b

    asyncio.run(run())


def test_acollect_builds_pydantic_models() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": 2}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    async def run() -> None:
        rows = await pf.acollect(name="UserRow")
        assert len(rows) == 1
        row = rows[0]
        assert type(row).__name__ == "UserRow"
        assert row.id == 1 and row.age == 2

    asyncio.run(run())


def test_acollect_native_async_skips_to_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async adapter override runs on the event loop; it must not call ``asyncio.to_thread``."""

    async def boom(*_a: object, **_k: object) -> object:
        raise AssertionError("AsyncSpyAdapter.acollect must not use asyncio.to_thread")

    monkeypatch.setattr(asyncio, "to_thread", boom)
    adapter = AsyncSpyAdapter()
    data = [{"id": 1, "age": 2}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    async def run() -> None:
        out = await pf.acollect(name="UserRow")
        assert len(out) == 1
        assert out[0].id == 1 and out[0].age == 2

    asyncio.run(run())
