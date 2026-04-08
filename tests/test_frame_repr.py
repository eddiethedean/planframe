from __future__ import annotations

from planframe.frame import Frame
from planframe.plan.nodes import Source
from planframe.schema.ir import Field, Schema


class DummyAdapter:
    name = "dummy"


def test_frame_repr_is_bounded_and_side_effect_free() -> None:
    adapter = DummyAdapter()
    data = object()

    # Wide schema to force truncation.
    wide = Schema(fields=tuple(Field(name=f"c{i}", dtype=int) for i in range(30)))
    pf = Frame(
        _data=data, _adapter=adapter, _plan=Source(schema_type=object, ir_version=1), _schema=wide
    )

    s = repr(pf)

    assert s.startswith("Frame")
    assert "cols=30" in s
    assert "plan=Source" in s
    assert "c0" in s
    assert "…(+24)" in s
