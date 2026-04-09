from __future__ import annotations

from test_core_lazy_and_schema import SpyAdapter, UserDC

from planframe.frame import Frame


def test_drop_nulls_how_any_default() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": None}, {"id": 2, "age": 3}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)
    out = pf.drop_nulls(subset=("age",)).collect_backend()
    assert out == [{"id": 2, "age": 3}]


def test_drop_nulls_how_all() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": None}, {"id": None, "age": None}, {"id": 2, "age": 3}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)
    out = pf.drop_nulls(subset=("id", "age"), how="all").collect_backend()
    assert out == [{"id": 1, "age": None}, {"id": 2, "age": 3}]


def test_drop_nulls_threshold() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "age": None}, {"id": None, "age": None}, {"id": 2, "age": 3}]
    pf = Frame.source(data, adapter=adapter, schema=UserDC)
    out = pf.drop_nulls(subset=("id", "age"), threshold=2).collect_backend()
    assert out == [{"id": 2, "age": 3}]
