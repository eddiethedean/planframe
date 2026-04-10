"""Issue #104: adapter hook for dtype resolution during expression compilation."""

from __future__ import annotations

from dataclasses import dataclass

from test_core_lazy_and_schema import SpyAdapter

from planframe.backend.adapter import CompileExprContext
from planframe.execution import execute_plan
from planframe.expr import agg_sum, col, gt, lit
from planframe.frame import Frame


@dataclass(frozen=True)
class Row:
    id: int
    b: int


def test_resolve_dtype_hook_invoked_for_filter_before_select() -> None:
    class LogAdapter(SpyAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.resolve_dtype_calls: list[str] = []

        def resolve_dtype(self, name: str, *, ctx: CompileExprContext) -> object | None:
            self.resolve_dtype_calls.append(name)
            return super().resolve_dtype(name, ctx=ctx)

    adapter = LogAdapter()
    pf = Frame.source([{"id": 1, "b": 2}], adapter=adapter, schema=Row)
    out = pf.filter(gt(col("b"), lit(0))).select("id")
    _ = execute_plan(adapter=adapter, plan=out.plan(), root_data=pf._data, schema=out.schema())
    assert "b" in adapter.resolve_dtype_calls


def test_resolve_dtype_hook_invoked_for_agg_on_non_key_column() -> None:
    class LogAdapter(SpyAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.resolve_dtype_calls: list[str] = []

        def resolve_dtype(self, name: str, *, ctx: CompileExprContext) -> object | None:
            self.resolve_dtype_calls.append(name)
            return super().resolve_dtype(name, ctx=ctx)

    adapter = LogAdapter()
    pf = Frame.source(
        [{"id": 1, "b": 10}, {"id": 1, "b": 20}],
        adapter=adapter,
        schema=Row,
    )
    out = pf.group_by("id").agg(total=agg_sum(col("b")))
    _ = execute_plan(adapter=adapter, plan=out.plan(), root_data=pf._data, schema=out.schema())
    assert "b" in adapter.resolve_dtype_calls
