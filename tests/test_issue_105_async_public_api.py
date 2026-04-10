"""Issue #105: discoverable async materialization aliases and execute_plan_async."""

from __future__ import annotations

import asyncio

import pytest
from test_core_lazy_and_schema import SpyAdapter, UserDC

from planframe.execution import execute_plan, execute_plan_async
from planframe.expr import col, eq, lit
from planframe.frame import Frame


def test_execute_plan_async_matches_execute_plan() -> None:
    async def run() -> None:
        adapter = SpyAdapter()
        data = [{"id": 1, "age": 2}]
        pf = Frame.source(data, adapter=adapter, schema=UserDC)
        plan = pf.select("id").plan()
        sync_out = execute_plan(
            adapter=adapter, plan=plan, root_data=pf._data, schema=pf.schema()
        )
        async_out = await execute_plan_async(
            adapter=adapter, plan=plan, root_data=pf._data, schema=pf.schema()
        )
        assert sync_out == async_out

    asyncio.run(run())


def test_execute_plan_async_propagates_errors() -> None:
    class BoomAdapter(SpyAdapter):
        def filter(self, df: list[dict[str, object]], predicate: object) -> list[dict[str, object]]:
            raise RuntimeError("boom")

    async def run() -> None:
        adapter = BoomAdapter()
        data = [{"id": 1, "age": 2}]
        pf = Frame.source(data, adapter=adapter, schema=UserDC)
        plan = pf.filter(eq(col("id"), lit(1))).plan()
        with pytest.raises(RuntimeError, match="boom"):
            await execute_plan_async(
                adapter=adapter, plan=plan, root_data=pf._data, schema=pf.schema()
            )

    asyncio.run(run())


def test_collect_async_to_dict_aliases_match_acollect_ato() -> None:
    async def run() -> None:
        adapter = SpyAdapter()
        data = [{"id": 1, "age": 2}]
        pf = Frame.source(data, adapter=adapter, schema=UserDC)
        f = pf.select("id", "age")

        a = await f.collect_async()
        b = await f.acollect()
        assert len(a) == len(b) == 1
        assert a[0].id == b[0].id

        td = await f.to_dict_async()
        atd = await f.ato_dict()
        assert td == atd

        tds = await f.to_dicts_async()
        atds = await f.ato_dicts()
        assert tds == atds

        backend = await f.collect_backend_async()
        abackend = await f.acollect_backend()
        assert backend == abackend

    asyncio.run(run())
