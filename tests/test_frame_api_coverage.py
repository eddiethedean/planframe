from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest
from test_core_lazy_and_schema import SpyAdapter

from planframe.backend.errors import (
    PlanFrameBackendError,
    PlanFrameExecutionError,
    PlanFrameSchemaError,
)
from planframe.expr import add, col, lit
from planframe.frame import Frame


@dataclass(frozen=True)
class S:
    id: int
    name: str
    age: int | None


def test_frame_select_variants_and_column_order_ops() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "name": "a", "age": None}], adapter=adapter, schema=S)

    assert pf.select_prefix("na").schema().names() == ("name",)
    assert pf.select_suffix("e").schema().names() == ("name", "age")
    assert pf.select_regex("^a").schema().names() == ("age",)

    assert pf.select_exclude("name").schema().names() == ("id", "age")
    assert pf.reorder_columns("name", "id", "age").schema().names() == ("name", "id", "age")
    assert pf.select_first("age").schema().names() == ("age", "id", "name")
    assert pf.select_last("id").schema().names() == ("name", "age", "id")

    assert pf.move("name", before="id").schema().names() == ("name", "id", "age")
    assert pf.move("name", after="age").schema().names() == ("id", "age", "name")

    # select() with no args is allowed (empty frame)
    assert pf.select().schema().names() == ()

    # Invalid select arg shape
    with pytest.raises(TypeError, match="select arguments must be"):
        pf.select(("x", "not_expr", 1))  # type: ignore[arg-type]


def test_frame_rename_helpers_and_strict_modes() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "name": "a", "age": None}], adapter=adapter, schema=S)

    assert pf.rename(name="full_name").schema().names() == ("id", "full_name", "age")

    # strict=False ignores unknown mapping keys
    assert pf.rename(name="full_name", missing="x", strict=False).schema().names() == (
        "id",
        "full_name",
        "age",
    )

    assert pf.rename_prefix("x_", "id").schema().names() == ("x_id", "name", "age")
    assert pf.rename_suffix("_x", "id").schema().names() == ("id_x", "name", "age")
    assert pf.rename_replace("n", "N", "name").schema().names() == ("id", "Name", "age")

    with pytest.raises(PlanFrameSchemaError, match="Cannot rename missing columns"):
        pf.rename(missing="x")

    with pytest.raises(PlanFrameSchemaError, match="Cannot drop missing columns"):
        pf.drop("missing")


def test_frame_drop_nulls_and_fill_null_validation() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "name": "a", "age": None}], adapter=adapter, schema=S)

    with pytest.raises(ValueError, match="drop_nulls how"):
        pf.drop_nulls("age", how="nope")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="threshold must be non-negative"):
        pf.drop_nulls("age", threshold=-1)

    # fill_null must specify exactly one of value or strategy
    with pytest.raises(ValueError, match="exactly one of value= or strategy="):
        pf.fill_null()
    with pytest.raises(ValueError, match="exactly one of value= or strategy="):
        pf.fill_null(0, "age", strategy="forward")

    # subset validation
    with pytest.raises(PlanFrameSchemaError, match="Cannot select missing column"):
        pf.drop_nulls("missing")

    # subset validation
    with pytest.raises(PlanFrameSchemaError, match="Cannot select missing column"):
        pf.drop_nulls("missing")


def test_frame_sort_flag_coercion_errors() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "name": "a", "age": None}], adapter=adapter, schema=S)

    with pytest.raises(ValueError, match="sequence of length"):
        pf.sort("id", "name", descending=(True,))  # wrong length
    with pytest.raises(TypeError, match="must contain only bool"):
        pf.sort("id", "name", descending=(True, "nope"))  # type: ignore[arg-type]


def test_frame_join_concat_backend_mismatch_errors() -> None:
    adapter = SpyAdapter()
    left = Frame.source([{"id": 1, "name": "a", "age": None}], adapter=adapter, schema=S)

    class SpyAdapter2(SpyAdapter):
        name = "spy2"

    right = Frame.source([{"id": 1, "name": "b", "age": 1}], adapter=SpyAdapter2(), schema=S)

    with pytest.raises(PlanFrameExecutionError, match="Backend collect failed"):
        left.join(right, on=("id",)).collect()

    with pytest.raises(PlanFrameBackendError, match="different backends"):
        left.concat_vertical(right)


def test_frame_write_methods_call_adapter() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "name": "a", "age": None}], adapter=adapter, schema=S)

    adapter.calls.clear()
    pf.write_parquet("x.parquet")
    pf.write_csv("x.csv")
    pf.write_ndjson("x.ndjson")
    pf.write_ipc("x.ipc")
    pf.write_excel("x.xlsx")
    pf.write_delta("x.delta")
    pf.write_avro("x.avro")
    pf.write_database(table_name="t", connection=object())

    called = {c[0] for c in adapter.calls}
    assert {
        "write_parquet",
        "write_csv",
        "write_ndjson",
        "write_ipc",
        "write_excel",
        "write_delta",
        "write_avro",
        "write_database",
    }.issubset(called)


def test_frame_select_with_expr_lowers_to_project() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "name": "a", "age": 2}], adapter=adapter, schema=S)

    out = pf.select("id", ("age_plus_one", add(col("age"), lit(1))))
    adapter.calls.clear()
    _ = out._eval(out.plan())
    calls = [c[0] for c in adapter.calls]
    assert "project" in calls


def test_frame_collect_kind_builds_models() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "name": "a", "age": 2}], adapter=adapter, schema=S)

    rows_dc = pf.select("id", "name").collect(kind="dataclass", name="Out")
    assert isinstance(rows_dc, list)
    assert rows_dc[0].__class__.__name__ == "Out"
    assert rows_dc[0].id == 1

    rows_pd = pf.select("id").collect(kind="pydantic", name="OutPD")
    assert isinstance(rows_pd, list)
    assert rows_pd[0].__class__.__name__ == "OutPD"
    assert rows_pd[0].id == 1


def test_frame_error_wrapping_for_collect_to_dicts_and_write() -> None:
    class BoomAdapter(SpyAdapter):
        def collect(self, planned: object) -> object:  # type: ignore[override]
            raise RuntimeError("boom")

    pf = Frame.source([{"id": 1, "name": "a", "age": 2}], adapter=BoomAdapter(), schema=S)
    with pytest.raises(PlanFrameExecutionError, match="Backend collect failed"):
        pf.collect()

    class BoomDictsAdapter(SpyAdapter):
        def to_dicts(self, df: object) -> list[dict[str, object]]:  # type: ignore[override]
            raise RuntimeError("boom")

    pf2 = Frame.source([{"id": 1, "name": "a", "age": 2}], adapter=BoomDictsAdapter(), schema=S)
    with pytest.raises(PlanFrameExecutionError, match="Backend to_dicts failed"):
        pf2.to_dicts()

    class BoomWriteAdapter(SpyAdapter):
        def write_csv(self, df: object, path: str, **kwargs: object) -> None:  # type: ignore[override]
            raise RuntimeError("boom")

    pf3 = Frame.source([{"id": 1, "name": "a", "age": 2}], adapter=BoomWriteAdapter(), schema=S)
    with pytest.raises(PlanFrameExecutionError, match="Backend write_csv failed"):
        pf3.write_csv("x.csv")

    class BoomCompileAdapter(SpyAdapter):
        def compile_expr(self, expr: object, *, schema: object = None) -> object:  # type: ignore[override]
            raise RuntimeError("boom")

    pf4 = Frame.source([{"id": 1, "name": "a", "age": 2}], adapter=BoomCompileAdapter(), schema=S)
    with pytest.raises(PlanFrameExecutionError, match="Backend collect failed"):
        pf4.with_column("x", add(col("id"), lit(1))).collect()

    class BoomKindAdapter(SpyAdapter):
        def to_dicts(self, df: object) -> list[dict[str, object]]:  # type: ignore[override]
            raise RuntimeError("boom")

    pf5 = Frame.source([{"id": 1, "name": "a", "age": 2}], adapter=BoomKindAdapter(), schema=S)
    with pytest.raises(PlanFrameExecutionError, match="collect\\(kind="):
        pf5.collect(kind="dataclass")


def test_frame_async_paths_and_dict_helpers() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "name": "a", "age": 2}], adapter=adapter, schema=S)

    assert isinstance(pf.to_dicts(), list)
    assert isinstance(pf.to_dict(), dict)

    assert isinstance(asyncio.run(pf.ato_dicts()), list)
    assert isinstance(asyncio.run(pf.ato_dict()), dict)

    assert isinstance(asyncio.run(pf.acollect()), list)

    class BoomAcollectAdapter(SpyAdapter):
        async def acollect(self, planned: object) -> object:  # type: ignore[override]
            raise RuntimeError("boom")

    pf2 = Frame.source([{"id": 1, "name": "a", "age": 2}], adapter=BoomAcollectAdapter(), schema=S)
    with pytest.raises(PlanFrameExecutionError, match="Backend acollect failed"):
        asyncio.run(pf2.acollect())


def test_frame_group_by_and_pivot_validation_branches() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "name": "a", "age": 2}], adapter=adapter, schema=S)

    with pytest.raises(PlanFrameBackendError, match="group_by requires at least one"):
        pf.group_by()

    with pytest.raises(PlanFrameSchemaError, match="references unknown columns"):
        pf.group_by(add(col("missing"), lit(1)))

    with pytest.raises(PlanFrameSchemaError, match="requires non-empty index"):
        pf.pivot(index=(), on="name", values="age")


def test_frame_repr_verbose_and_truncation(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "name": "a", "age": 2}], adapter=adapter, schema=S)

    # non-verbose
    monkeypatch.delenv("PLANFRAME_REPR_VERBOSE", raising=False)
    s1 = repr(pf)
    assert "adapter=" not in s1

    # verbose includes adapter name
    monkeypatch.setenv("PLANFRAME_REPR_VERBOSE", "1")
    s2 = repr(pf)
    assert "adapter=" in s2


def test_frame_optimize_noop_paths() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "name": "a", "age": 2}], adapter=adapter, schema=S)

    assert pf.optimize(level=0) is pf
    assert pf.optimize(level=1) is pf
