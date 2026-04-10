from __future__ import annotations

import pytest

from planframe.adapter_conformance import run_minimal_adapter_conformance
from planframe_polars import PolarsFrame

pytestmark = pytest.mark.conformance


class Users(PolarsFrame):
    id: int
    name: str
    age: int


class JoinLeft(PolarsFrame):
    id: int
    name: str


class JoinRight(PolarsFrame):
    id: int
    city: str


def test_run_minimal_adapter_conformance_polars() -> None:
    run_minimal_adapter_conformance(users=Users, join_left=JoinLeft, join_right=JoinRight)


def test_run_minimal_adapter_conformance_polars_result() -> None:
    r = run_minimal_adapter_conformance(
        users=Users,
        join_left=JoinLeft,
        join_right=JoinRight,
        raise_on_failure=False,
    )
    assert r.passed
    names = {c.name for c in r.cases}
    assert names == {"select_filter", "project_expr", "sort", "group_by_agg", "join_inner"}
    assert all(c.ok for c in r.cases)


def test_join_optional_skip_reported() -> None:
    r = run_minimal_adapter_conformance(users=Users, raise_on_failure=False)
    join_cases = [c for c in r.cases if c.name == "join_inner"]
    assert len(join_cases) == 1
    assert join_cases[0].ok is True
    assert "skipped" in join_cases[0].detail
