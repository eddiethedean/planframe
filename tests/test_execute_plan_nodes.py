from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict

import pytest
from test_core_lazy_and_schema import SpyAdapter

from planframe.execution import execute_plan
from planframe.expr import add, col, gt, lit
from planframe.frame import Frame
from planframe.plan.join_options import JoinOptions
from planframe.schema.ir import Schema


@dataclass(frozen=True)
class S:
    id: int
    a: int | None
    b: int


@dataclass(frozen=True)
class ExplodeSchema:
    id: int
    xs: list[int]


@dataclass(frozen=True)
class UnnestSchema:
    id: int
    s: UnnestStruct


class UnnestStruct(TypedDict):
    k: int


@dataclass(frozen=True)
class LeftOnly:
    id: int


@dataclass(frozen=True)
class RightOnly:
    x: int


def _run(frame: Frame[Any, list[dict[str, Any]], object]) -> tuple[list[dict[str, Any]], list[str]]:
    adapter = frame._adapter
    assert isinstance(adapter, SpyAdapter)
    adapter.calls.clear()
    out = execute_plan(
        adapter=adapter, plan=frame.plan(), root_data=frame._data, schema=frame.schema()
    )
    return out, [c[0] for c in adapter.calls]


def test_execute_plan_collect_true_passes_options() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "a": None, "b": 2}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    from planframe.execution_options import ExecutionOptions

    adapter.calls.clear()
    _ = execute_plan(
        adapter=adapter,
        plan=pf.plan(),
        root_data=pf._data,
        schema=pf.schema(),
        collect=True,
        options=ExecutionOptions(streaming=True),
    )
    assert ("collect", ExecutionOptions(streaming=True, engine_streaming=None)) in adapter.calls


def test_execute_plan_join_passes_join_execution_hints() -> None:
    adapter = SpyAdapter()
    left = Frame.source([{"id": 1, "a": 1, "b": 0}], adapter=adapter, schema=S)
    right = Frame.source([{"id": 1, "a": 2, "b": 0}], adapter=adapter, schema=S)

    joined = left.join(
        right,
        on=("id",),
        options=JoinOptions(allow_parallel=True, engine_streaming=False),
    )

    adapter.calls.clear()
    _ = execute_plan(
        adapter=adapter,
        plan=joined.plan(),
        root_data=joined._data,
        schema=joined.schema(),
    )
    assert adapter.calls
    name, args = adapter.calls[0]
    assert name == "join"
    options = args[-1]
    assert isinstance(options, JoinOptions)
    assert options.allow_parallel is True
    assert options.engine_streaming is False


def test_execute_plan_select_project_with_column_cast_filter_sort_slice_head_tail() -> None:
    adapter = SpyAdapter()
    data = [{"id": 2, "a": None, "b": 5}, {"id": 1, "a": 3, "b": 4}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    out = (
        pf.select(("id2", add(col("id"), lit(1))), ("b2", col("b")))
        .with_column("x", add(col("b2"), lit(10)))
        .cast("x", int)
        .filter(gt(col("id2"), lit(1)))
        .sort("id2", descending=True, nulls_last=True)
        .slice(0, 1)
        .head(1)
        .tail(1)
    )

    res, calls = _run(out)
    assert calls == [
        "compile_expr",
        "compile_expr",
        "project",
        "compile_expr",
        "with_column",
        "cast",
        "compile_expr",
        "filter",
        "sort",
        "slice",
        "head",
        "tail",
    ]
    assert res == [{"id2": "computed", "b2": "computed", "x": "computed"}]


def test_execute_plan_unique_and_duplicated() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "a": 1, "b": 9}, {"id": 1, "a": 2, "b": 9}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    res_u, calls_u = _run(pf.unique("id"))
    assert calls_u == ["unique"]
    assert res_u == [{"id": 1, "a": 1, "b": 9}]

    res_d, calls_d = _run(pf.duplicated("id", out_name="is_dup"))
    assert calls_d == ["duplicated"]
    assert res_d == [{"is_dup": False}, {"is_dup": True}]


def test_execute_plan_drop_nulls_and_drop_nulls_all() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "a": None, "b": 0}, {"id": 2, "a": 1, "b": 0}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    res1, calls1 = _run(pf.drop_nulls("a"))
    assert calls1 == ["drop_nulls"]
    assert res1 == [{"id": 2, "a": 1, "b": 0}]

    res2, calls2 = _run(pf.drop_nulls_all("a"))
    assert calls2 == ["drop_nulls_all"]
    assert res2 == [{"id": 2, "a": 1, "b": 0}]


def test_execute_plan_fill_null_literal_and_strategy() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "a": None, "b": 0}, {"id": 2, "a": 2, "b": 0}, {"id": 3, "a": None, "b": 0}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    res_lit, calls_lit = _run(pf.fill_null(0, "a"))
    assert calls_lit == ["fill_null"]
    assert [r["a"] for r in res_lit] == [0, 2, 0]

    res_ff, calls_ff = _run(pf.fill_null(None, "a", strategy="forward"))
    assert calls_ff == ["fill_null"]
    assert [r["a"] for r in res_ff] == [None, 2, 2]


def test_execute_plan_fill_null_expr_value_compiles() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "a": None, "b": 2}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    out = pf.fill_null(add(col("b"), lit(1)), "a")
    res, calls = _run(out)
    assert calls == ["compile_expr", "fill_null"]
    # SpyAdapter uses the compiled expression object directly as a literal fill value.
    assert res[0]["id"] == 1
    assert res[0]["b"] == 2
    assert "a" in res[0]


def test_execute_plan_groupby_node_evaluates_prev() -> None:
    from planframe.plan.nodes import GroupBy, JoinKeyColumn

    adapter = SpyAdapter()
    data = [{"id": 1, "a": 1, "b": 0}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    gb = GroupBy(prev=pf.plan(), keys=(JoinKeyColumn(name="id"),))
    adapter.calls.clear()
    res = execute_plan(adapter=adapter, plan=gb, root_data=pf._data, schema=pf.schema())
    calls = [c[0] for c in adapter.calls]
    assert calls == []
    assert res == data


def test_execute_plan_agg_requires_groupby() -> None:
    from planframe.plan.nodes import Agg

    adapter = SpyAdapter()
    data = [{"id": 1, "a": 1, "b": 0}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    bad = Frame(
        _data=data,
        _adapter=adapter,
        _plan=Agg(prev=pf.plan(), named_aggs={"x": ("count", "id")}),
        _schema=pf.schema(),
    )
    with pytest.raises(Exception, match="Agg must follow GroupBy"):
        _run(bad)


def test_execute_plan_invalid_side_frames_and_unsupported_node() -> None:
    from dataclasses import dataclass

    from planframe.plan.nodes import ConcatHorizontal, ConcatVertical, Join, PlanNode, Source

    adapter = SpyAdapter()
    src = Source(schema_type=object, ir_version=1)

    class BadFrameNoAdapter:
        _plan = src

    j = Join(
        prev=src,
        right=BadFrameNoAdapter(),  # type: ignore[arg-type]
        left_keys=(),
        right_keys=(),
        how="inner",
        suffix="_r",
        options=None,
    )
    bad_join = Frame(_data=[], _adapter=adapter, _plan=j, _schema=Schema(fields=()))
    with pytest.raises(Exception, match="Join node right frame is invalid"):
        _run(bad_join)

    class BadFrameWrongAdapter:
        _adapter = type("A", (), {"name": "nope"})()
        _plan = src

        def _eval(self, node: object) -> object:
            return []

    j2 = Join(
        prev=src,
        right=BadFrameWrongAdapter(),  # type: ignore[arg-type]
        left_keys=(),
        right_keys=(),
        how="inner",
        suffix="_r",
        options=None,
    )
    bad_join2 = Frame(_data=[], _adapter=adapter, _plan=j2, _schema=Schema(fields=()))
    with pytest.raises(Exception, match="different backends"):
        _run(bad_join2)

    class BadFrameWrongAdapter2(BadFrameWrongAdapter):
        _adapter = type("A", (), {"name": "spy2"})()

    cv2 = ConcatVertical(prev=src, other=BadFrameWrongAdapter2())  # type: ignore[arg-type]
    bad_cv2 = Frame(_data=[], _adapter=adapter, _plan=cv2, _schema=Schema(fields=()))
    with pytest.raises(Exception, match="different backends"):
        _run(bad_cv2)

    ch2 = ConcatHorizontal(prev=src, other=BadFrameWrongAdapter2())  # type: ignore[arg-type]
    bad_ch2 = Frame(_data=[], _adapter=adapter, _plan=ch2, _schema=Schema(fields=()))
    with pytest.raises(Exception, match="different backends"):
        _run(bad_ch2)

    cv = ConcatVertical(prev=src, other=BadFrameNoAdapter())  # type: ignore[arg-type]
    bad_cv = Frame(_data=[], _adapter=adapter, _plan=cv, _schema=Schema(fields=()))
    with pytest.raises(Exception, match="ConcatVertical node other frame is invalid"):
        _run(bad_cv)

    ch = ConcatHorizontal(prev=src, other=BadFrameNoAdapter())  # type: ignore[arg-type]
    bad_ch = Frame(_data=[], _adapter=adapter, _plan=ch, _schema=Schema(fields=()))
    with pytest.raises(Exception, match="ConcatHorizontal node other frame is invalid"):
        _run(bad_ch)

    @dataclass(frozen=True, slots=True)
    class Unsupported(PlanNode):
        prev: PlanNode

    unsup = Unsupported(prev=src)
    bad_u = Frame(_data=[], _adapter=adapter, _plan=unsup, _schema=Schema(fields=()))
    with pytest.raises(Exception, match="Unsupported plan node"):
        _run(bad_u)

    with pytest.raises(Exception, match="Unsupported plan node"):
        execute_plan(adapter=adapter, plan=object(), root_data=[], schema=Schema(fields=()))  # type: ignore[arg-type]


def test_execute_plan_group_by_agg_smoke() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "a": 1, "b": 10}, {"id": 1, "a": 2, "b": 20}, {"id": 2, "a": 3, "b": 30}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    out = pf.group_by("id").agg(total=("sum", "b"))
    res, calls = _run(out)
    assert calls == ["group_by_agg"]
    # SpyAdapter groups without ordering guarantees; compare as sets.
    assert {r["id"] for r in res} == {1, 2}


def test_execute_plan_melt_pivot_explode_unnest_sample() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "a": 1, "b": 2}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    melted = pf.melt(id_vars=("id",), value_vars=("a", "b"))
    res_m, calls_m = _run(melted)
    assert calls_m == ["melt"]
    assert len(res_m) == 2

    piv = melted.pivot(index=("id",), on="variable", values="value", on_columns=("a", "b"))
    res_p, calls_p = _run(piv)
    assert calls_p == ["melt", "pivot"]
    assert res_p == [{"id": 1, "a": 1, "b": 2}]

    ex = Frame.source([{"id": 1, "xs": [1, 2]}], adapter=adapter, schema=ExplodeSchema).explode(
        "xs"
    )
    res_e, calls_e = _run(ex)
    assert calls_e == ["explode"]
    assert [r["xs"] for r in res_e] == [1, 2]

    un = Frame.source([{"id": 1, "s": {"k": 1}}], adapter=adapter, schema=UnnestSchema).unnest("s")
    res_u, calls_u = _run(un)
    assert calls_u == ["unnest"]
    assert res_u == [{"id": 1, "k": 1}]

    samp = pf.sample(n=1)
    res_s, calls_s = _run(samp)
    assert calls_s == ["sample"]
    assert len(res_s) == 1


def test_execute_plan_with_row_count() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    out = pf.select("id").with_row_count(name="rn", offset=7)
    res, calls = _run(out)
    assert calls == ["select", "with_row_count"]
    assert res == [{"id": 1, "rn": 7}, {"id": 2, "rn": 8}, {"id": 3, "rn": 9}]


def test_execute_plan_pivot_longer_and_wider() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "a": 10, "b": 20}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    out = pf.pivot_longer(id_vars=("id",), value_vars=("a", "b"), names_to="k", values_to="v")
    res1, calls1 = _run(out)
    assert calls1 == ["melt"]
    assert len(res1) == 2

    out2 = out.pivot_wider(
        index=("id",),
        names_from="k",
        values_from="v",
        on_columns=("a", "b"),
        aggregate_function="first",
    )
    res2, calls2 = _run(out2)
    assert calls2 == ["melt", "pivot"]
    assert res2 == [{"id": 1, "a": 10, "b": 20}]


def test_execute_plan_cast_many_and_subset_lower_to_cast() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "a": None, "b": 2}], adapter=adapter, schema=S)

    out = pf.cast_many({"b": float}).cast_subset("b", dtype=int)
    res, calls = _run(out)
    assert calls == ["cast", "cast"]
    assert res == [{"id": 1, "a": None, "b": 2}]


def test_execute_plan_select_schema_lowers_to_select() -> None:
    adapter = SpyAdapter()
    data = [{"id": 1, "a": 10, "b": 20}]
    pf = Frame.source(data, adapter=adapter, schema=S)

    from planframe.selector import by_name

    out = pf.select_schema(by_name("id", "b"))
    res, calls = _run(out)
    assert calls == ["select"]
    assert res == [{"id": 1, "b": 20}]


def test_execute_plan_fill_null_subset_and_many_lower_to_fill_null() -> None:
    adapter = SpyAdapter()
    pf = Frame.source([{"id": 1, "a": None, "b": 2}], adapter=adapter, schema=S)

    out = pf.fill_null_subset(0, "a").fill_null_many({"a": 1})
    res, calls = _run(out)
    assert calls == ["fill_null", "fill_null"]
    # SpyAdapter.fill_null applies eagerly; second fill_null doesn't change non-null values.
    assert res == [{"id": 1, "a": 0, "b": 2}]


def test_execute_plan_rename_case_helpers_lower_to_rename() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class S2:
        id: int
        foo: int

    pf = Frame.source([{"id": 1, "foo": 2}], adapter=adapter, schema=S2)
    out = pf.rename_lower()
    res, calls = _run(out)
    assert calls == ["rename"]
    assert res == [{"id": 1, "foo": 2}]


def test_execute_plan_posexplode() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class S2:
        id: int
        xs: list[int]

    pf = Frame.source([{"id": 1, "xs": [5, 6]}], adapter=adapter, schema=S2)
    out = pf.posexplode("xs")
    res, calls = _run(out)
    assert calls == ["posexplode"]
    assert res == [{"id": 1, "pos": 0, "xs": 5}, {"id": 1, "pos": 1, "xs": 6}]


def test_execute_plan_group_by_dynamic_and_rolling_agg() -> None:
    adapter = SpyAdapter()

    @dataclass(frozen=True)
    class S2:
        ts: int
        g: str
        x: int

    pf = Frame.source(
        [{"ts": 1, "g": "a", "x": 10}, {"ts": 1, "g": "a", "x": 20}],
        adapter=adapter,
        schema=S2,
    )

    dyn = pf.group_by_dynamic("ts", every="1h", by=("g",)).agg(n=("count", "x"))
    res_dyn, calls_dyn = _run(dyn)
    assert calls_dyn == ["group_by_dynamic_agg"]
    assert len(res_dyn) == 1

    roll = pf.rolling_agg(on="ts", column="x", window_size=2, op="mean", out_name="x_roll")
    res_roll, calls_roll = _run(roll)
    assert calls_roll == ["rolling_agg"]
    assert "x_roll" in res_roll[0]


def test_execute_plan_join_and_concat_and_backend_mismatch_errors() -> None:
    adapter = SpyAdapter()
    left = Frame.source([{"id": 1, "a": 1, "b": 0}], adapter=adapter, schema=S)
    right = Frame.source([{"id": 1, "a": 2, "b": 0}], adapter=adapter, schema=S)

    joined = left.join(right, on=("id",))
    res_j, calls_j = _run(joined)
    assert calls_j == ["join"]
    assert res_j == [{"id": 1, "a": 1, "b": 0, "a_right": 2, "b_right": 0}]

    cv = left.concat_vertical(right)
    res_cv, calls_cv = _run(cv)
    assert calls_cv == ["concat_vertical"]
    assert len(res_cv) == 2

    ch = Frame.source([{"id": 1}], adapter=adapter, schema=LeftOnly).concat_horizontal(
        Frame.source([{"x": 2}], adapter=adapter, schema=RightOnly)
    )
    res_ch, calls_ch = _run(ch)
    assert calls_ch == ["concat_horizontal"]
    assert res_ch == [{"id": 1, "x": 2}]

    # Backend mismatch
    class SpyAdapter2(SpyAdapter):
        name = "spy2"

    right2 = Frame.source([{"id": 1, "a": 2, "b": 0}], adapter=SpyAdapter2(), schema=S)
    with pytest.raises(Exception, match="different backends"):
        _run(left.join(right2, on=("id",)))
