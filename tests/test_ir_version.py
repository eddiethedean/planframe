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


def test_ir_version_for_side_frames_in_join_and_concat() -> None:
    adapter = SpyAdapter()
    left = Frame.source([{"id": 1, "age": 2}], adapter=adapter, schema=UserDC).select("id")
    right = Frame.source([{"id": 1, "age": 3}], adapter=adapter, schema=UserDC).select("id").head(1)

    joined = left.join(right, on=("id",))
    sources_j = [
        n
        for n in iter_plan_nodes(root=joined.plan(), include_side_frames=True)
        if isinstance(n, Source)
    ]
    assert len(sources_j) == 2
    assert {s.ir_version for s in sources_j} == {1}

    cv = left.concat_vertical(right)
    sources_cv = [
        n
        for n in iter_plan_nodes(root=cv.plan(), include_side_frames=True)
        if isinstance(n, Source)
    ]
    assert len(sources_cv) == 2
    assert {s.ir_version for s in sources_cv} == {1}
