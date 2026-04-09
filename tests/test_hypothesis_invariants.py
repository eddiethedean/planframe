"""Property-based tests for core invariants (sort nulls, join keys, schema projection)."""

from __future__ import annotations

from dataclasses import dataclass

import hypothesis.strategies as st
import pytest
from hypothesis import given
from hypothesis.strategies import composite
from test_core_lazy_and_schema import SpyAdapter

from planframe.frame import Frame


@dataclass
class _K:
    k: int | None


@dataclass
class _IdX:
    id: int
    x: int


@dataclass
class _IdY:
    id: int
    y: int


def _ref_sort_nulls_last(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Reference ascending sort on ``k`` with nulls last (SpyAdapter semantics)."""

    def key(r: dict[str, object]) -> tuple[int, int]:
        v = r["k"]
        if v is None:
            return (1, 0)
        assert isinstance(v, int)
        return (0, v)

    return sorted(rows, key=key)


def _ref_sort_nulls_first(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    def key(r: dict[str, object]) -> tuple[int, int]:
        v = r["k"]
        if v is None:
            return (0, 0)
        assert isinstance(v, int)
        return (1, v)

    return sorted(rows, key=key)


row_with_k = st.builds(
    dict,
    k=st.one_of(st.none(), st.integers(min_value=-50, max_value=50)),
)


@composite
def table_k(draw: object) -> list[dict[str, object]]:
    n = draw(st.integers(min_value=0, max_value=24))
    return [draw(row_with_k) for _ in range(n)]


@pytest.mark.property
@given(table_k())
def test_spy_sort_null_ordering_matches_reference(rows: list[dict[str, object]]) -> None:
    adapter = SpyAdapter()
    pf = Frame.source(rows, adapter=adapter, schema=_K)
    out_nl = pf.sort("k", nulls_last=True).to_dicts()
    out_nf = pf.sort("k", nulls_last=False).to_dicts()
    assert out_nl == _ref_sort_nulls_last(list(rows))
    assert out_nf == _ref_sort_nulls_first(list(rows))


id_x_row = st.builds(
    dict, id=st.integers(min_value=0, max_value=8), x=st.integers(min_value=0, max_value=100)
)
id_y_row = st.builds(
    dict, id=st.integers(min_value=0, max_value=8), y=st.integers(min_value=0, max_value=100)
)


@composite
def join_tables(draw: object) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    ln = draw(st.integers(min_value=0, max_value=12))
    rn = draw(st.integers(min_value=0, max_value=12))
    left = [draw(id_x_row) for _ in range(ln)]
    right = [draw(id_y_row) for _ in range(rn)]
    return left, right


def _naive_inner_join(
    left: list[dict[str, object]], right: list[dict[str, object]]
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for lr in left:
        for rr in right:
            if lr["id"] == rr["id"]:
                row = dict(lr)
                if "y" in rr:
                    row["y"] = rr["y"]
                out.append(row)
    return out


@pytest.mark.property
@given(join_tables())
def test_spy_inner_join_ids_match_naive(
    tables: tuple[list[dict[str, object]], list[dict[str, object]]],
) -> None:
    left, right = tables
    adapter = SpyAdapter()
    lf = Frame.source(left, adapter=adapter, schema=_IdX)
    rf = Frame.source(right, adapter=adapter, schema=_IdY)
    got = lf.join(rf, on=("id",), how="inner").to_dicts()
    assert got == _naive_inner_join(left, right)


@dataclass
class _ABC:
    a: int
    b: int
    c: int


@pytest.mark.property
@given(
    st.lists(
        st.sampled_from(("a", "b", "c")),
        min_size=1,
        max_size=3,
        unique=True,
    )
)
def test_select_schema_names_are_projected(cols: list[str]) -> None:
    adapter = SpyAdapter()
    base = [{"a": 1, "b": 2, "c": 3}]
    pf = Frame.source(base, adapter=adapter, schema=_ABC).select(*cols)
    assert pf.schema().names() == tuple(cols)
