from __future__ import annotations

from test_core_lazy_and_schema import SpyAdapter, UserDC

from planframe.frame import Frame
from planframe.plan.nodes import Source
from planframe.plan.walk import iter_plan_nodes


def test_source_plan_has_default_ir_version() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)
    p = pf.plan()
    assert isinstance(p, Source)
    assert p.ir_version == 1


def test_ir_version_is_preserved_through_transforms() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC)
    out = pf.select("id").rename(id="user_id").head(1)

    sources = [n for n in iter_plan_nodes(root=out.plan()) if isinstance(n, Source)]
    assert len(sources) == 1
    assert sources[0].ir_version == 1
