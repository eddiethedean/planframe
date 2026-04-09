"""Structured parametrization with ``pytest-cases`` (SpyAdapter join / sort scenarios)."""

from __future__ import annotations

from dataclasses import dataclass

from pytest_cases import case, parametrize_with_cases
from test_core_lazy_and_schema import SpyAdapter

from planframe.frame import Frame


@dataclass
class _IX:
    id: int
    x: int


@dataclass
class _IY:
    id: int
    y: int


@case(id="single_match")
def case_join_single() -> tuple[list[dict[str, int]], list[dict[str, int]], list[dict[str, int]]]:
    left = [{"id": 1, "x": 10}, {"id": 2, "x": 20}]
    right = [{"id": 2, "y": 99}]
    expected = [{"id": 2, "x": 20, "y": 99}]
    return left, right, expected


@case(id="no_match")
def case_join_empty() -> tuple[list[dict[str, int]], list[dict[str, int]], list[dict[str, int]]]:
    left = [{"id": 1, "x": 1}]
    right = [{"id": 9, "y": 1}]
    return left, right, []


@case(id="duplicate_keys_cartesian")
def case_join_dup_keys() -> tuple[list[dict[str, int]], list[dict[str, int]], list[dict[str, int]]]:
    left = [{"id": 1, "x": 1}, {"id": 1, "x": 2}]
    right = [{"id": 1, "y": 10}, {"id": 1, "y": 20}]
    expected = [
        {"id": 1, "x": 1, "y": 10},
        {"id": 1, "x": 1, "y": 20},
        {"id": 1, "x": 2, "y": 10},
        {"id": 1, "x": 2, "y": 20},
    ]
    return left, right, expected


@parametrize_with_cases("left,right,expected", cases=".", prefix="case_join_")
def test_inner_join_cases(
    left: list[dict[str, int]], right: list[dict[str, int]], expected: list[dict[str, int]]
) -> None:
    adapter = SpyAdapter()
    lf = Frame.source(left, adapter=adapter, schema=_IX)
    rf = Frame.source(right, adapter=adapter, schema=_IY)
    got = lf.join(rf, on=("id",), how="inner").to_dicts()
    assert got == expected
