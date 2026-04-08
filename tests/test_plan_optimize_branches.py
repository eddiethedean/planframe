from __future__ import annotations

import pytest

from planframe.plan.nodes import Drop, Rename, Select, Source
from planframe.plan.optimize import _replace_prev, optimize_plan


def test_optimize_level_zero_is_noop() -> None:
    p = Source(schema_type=object, ir_version=1)
    assert optimize_plan(p, level=0) is p


def test_optimize_prunes_rename_empty_mapping() -> None:
    p = Source(schema_type=object, ir_version=1)
    plan = Rename(prev=p, mapping={}, strict=True)
    opt = optimize_plan(plan, level=1)
    assert opt is p


def test_optimize_is_idempotent() -> None:
    p = Source(schema_type=object, ir_version=1)
    plan = Select(prev=Select(prev=p, columns=("a",)), columns=("b",))
    opt1 = optimize_plan(plan, level=1)
    opt2 = optimize_plan(opt1, level=1)
    assert opt2 is opt1


def test_replace_prev_rebuilds_dataclass_node() -> None:
    p1 = Source(schema_type=object, ir_version=1)
    p2 = Source(schema_type=int, ir_version=1)
    sel = Select(prev=p1, columns=("a",))
    sel2 = _replace_prev(sel, p2)
    assert isinstance(sel2, Select)
    assert sel2.prev is p2
    assert sel2.columns == ("a",)


def test_optimize_replaces_prev_when_child_optimizes() -> None:
    src = Source(schema_type=object, ir_version=1)
    drop_noop = Drop(prev=src, columns=(), strict=True)
    plan = Select(prev=drop_noop, columns=("a",))
    opt = optimize_plan(plan, level=1)
    assert isinstance(opt, Select)
    assert opt.prev is src


def test_replace_prev_requires_dataclass() -> None:
    class NotDataclass:
        prev: object

    with pytest.raises(TypeError, match="Expected dataclass"):
        _replace_prev(NotDataclass(), Source(schema_type=object, ir_version=1))  # type: ignore[arg-type]


def test_optimize_prunes_drop_empty_columns_node() -> None:
    p = Source(schema_type=object, ir_version=1)
    plan = Drop(prev=p, columns=(), strict=True)
    assert optimize_plan(plan, level=1) is p
