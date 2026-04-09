from __future__ import annotations

from test_core_lazy_and_schema import SpyAdapter, UserDC

from planframe.frame import Frame
from planframe.plan.nodes import Drop, Source
from planframe.plan.walk import iter_plan_nodes


def test_optimize_fuses_select_chain() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)
    out = pf.select("id", "age").select("id")

    names_before = [type(n).__name__ for n in iter_plan_nodes(root=out.plan())]
    assert names_before.count("Select") == 2

    opt = out.optimize(level=1)
    names_after = [type(n).__name__ for n in iter_plan_nodes(root=opt.plan())]
    assert names_after.count("Select") == 1

    assert out.collect_backend() == opt.collect_backend()


def test_optimize_prunes_drop_empty_columns() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)

    # Explicitly create a Drop with empty columns (Frame.drop() with no args).
    out = pf.drop()
    assert isinstance(out.plan(), Drop)

    opt = out.optimize(level=1)
    assert isinstance(opt.plan(), Source)

    adapter.calls.clear()
    out_res = out.collect_backend()
    calls_unopt = [c[0] for c in adapter.calls]

    adapter.calls.clear()
    opt_res = opt.collect_backend()
    calls_opt = [c[0] for c in adapter.calls]

    assert out_res == opt_res
    assert "drop" in calls_unopt
    assert "drop" not in calls_opt
