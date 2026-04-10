from __future__ import annotations

import asyncio

import pytest
from test_core_lazy_and_schema import SpyAdapter, UserDC

from planframe import (
    amaterialize_columns,
    amaterialize_into,
    materialize_columns,
    materialize_into,
)
from planframe.backend.errors import PlanFrameExecutionError
from planframe.execution_options import ExecutionOptions
from planframe.expr import col, eq, lit
from planframe.frame import Frame


def _last_call(adapter: SpyAdapter, name: str) -> object | None:
    for n, arg in reversed(adapter.calls):
        if n == name:
            return arg
    return None


def test_materialize_columns_matches_to_dict() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": 10}, {"id": 2, "age": 20}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    direct = pf.to_dict()
    via = materialize_columns(pf)
    assert via == direct == {"id": [1, 2], "age": [10, 20]}


def test_materialize_columns_forwards_execution_options() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1}], adapter=adapter, schema=UserDC)
    opts = ExecutionOptions(streaming=True, engine_streaming=False)

    materialize_columns(pf, options=opts)

    assert _last_call(adapter, "to_dict") == opts


def test_materialize_into_invokes_factory_with_columns() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)
    opts = ExecutionOptions(streaming=None, engine_streaming=True)

    def factory(cols: dict[str, list[object]]) -> tuple[str, ...]:
        return tuple(cols.keys())

    out = materialize_into(pf, factory, options=opts)
    assert out == ("id", "age")
    assert _last_call(adapter, "to_dict") == opts


def test_materialize_into_propagates_factory_errors() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1}], adapter=adapter, schema=UserDC)

    def boom(_: dict[str, list[object]]) -> None:
        raise ValueError("factory failed")

    with pytest.raises(ValueError, match="factory failed"):
        materialize_into(pf, boom)


def test_materialize_into_propagates_backend_errors() -> None:
    class BadAdapter(SpyAdapter):
        def to_dict(
            self, df: list[dict[str, object]], *, options: object | None = None
        ) -> dict[str, list[object]]:
            raise RuntimeError("backend exploded")

    adapter = BadAdapter()
    pf = Frame.source([{"id": 1}], adapter=adapter, schema=UserDC)

    with pytest.raises(PlanFrameExecutionError) as ei:
        materialize_into(pf, lambda c: c)
    assert isinstance(ei.value.__cause__, RuntimeError)


def test_amaterialize_forwards_options() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1}], adapter=adapter, schema=UserDC).filter(eq(col("id"), lit(1)))
    opts = ExecutionOptions(streaming=True)

    async def run() -> None:
        cols = await amaterialize_columns(pf, options=opts)
        assert cols == {"id": [1]}
        assert _last_call(adapter, "to_dict") == opts

    asyncio.run(run())


def test_amaterialize_into() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1}], adapter=adapter, schema=UserDC)

    def factory(cols: dict[str, list[object]]) -> int:
        return len(cols["id"])

    async def run() -> None:
        assert await amaterialize_into(pf, factory) == 1

    asyncio.run(run())
