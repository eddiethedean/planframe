from __future__ import annotations

from typing import Any

from test_core_lazy_and_schema import SpyAdapter, UserDC

from planframe.execution import execute_plan
from planframe.frame import Frame


def test_execute_plan_matches_frame_eval_for_common_nodes() -> None:
    adapter = SpyAdapter()
    data = [{"id": 2, "age": None}, {"id": 1, "age": 3}, {"id": 1, "age": 3}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    out = (
        pf.fill_null(0, "age")
        .drop_nulls(subset=("age",))
        .sort("id", nulls_last=True)
        .unique("id")
    )

    assert adapter.calls == []

    planned1 = out._eval(out.plan())
    planned2 = execute_plan(adapter=adapter, plan=out.plan(), root_data=data, schema=out.schema())

    assert planned1 == planned2
    # execute_plan is an interpreter only; should not call adapter.collect by itself.
    assert "collect" not in [c[0] for c in adapter.calls]


def test_execute_plan_can_be_collected_explicitly() -> None:
    adapter = SpyAdapter()
    data: list[dict[str, Any]] = [{"id": 1, "age": None}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)

    planned = execute_plan(
        adapter=adapter, plan=pf.fill_null(0, "age").plan(), root_data=data, schema=pf.schema()
    )
    collected = adapter.collect(planned)

    assert collected == [{"id": 1, "age": 0}]
