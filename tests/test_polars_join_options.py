from __future__ import annotations

import polars as pl
import pytest

from planframe.backend.adapter import CompiledJoinKey
from planframe.plan.join_options import JoinOptions
from planframe_polars.adapter import PolarsAdapter


@pytest.fixture
def captured_join_kwargs(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, object]]:
    calls: list[dict[str, object]] = []
    orig = pl.LazyFrame.join

    def tracking_join(self: pl.LazyFrame, other: pl.LazyFrame, **kwargs: object) -> pl.LazyFrame:
        calls.append(dict(kwargs))
        return orig(self, other, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "join", tracking_join)
    return calls


def _inner_join(adapter: PolarsAdapter, options: JoinOptions | None) -> None:
    left = pl.LazyFrame({"a": [1, 2]})
    right = pl.LazyFrame({"a": [1, 3], "c": [10, 20]})
    keys = (CompiledJoinKey(column="a", expr=None),)
    adapter.join(left, right, left_on=keys, right_on=keys, options=options)


def test_polars_join_streaming_sets_allow_parallel_inverted(captured_join_kwargs: list) -> None:
    adapter = PolarsAdapter()
    _inner_join(adapter, JoinOptions(streaming=True))
    assert captured_join_kwargs[-1]["allow_parallel"] is False


def test_polars_join_allow_parallel_overrides_streaming(captured_join_kwargs: list) -> None:
    adapter = PolarsAdapter()
    _inner_join(adapter, JoinOptions(streaming=True, allow_parallel=True))
    assert captured_join_kwargs[-1]["allow_parallel"] is True


def test_polars_join_force_parallel_forwards_to_polars(captured_join_kwargs: list) -> None:
    adapter = PolarsAdapter()
    _inner_join(adapter, JoinOptions(force_parallel=True))
    assert captured_join_kwargs[-1]["force_parallel"] is True


def test_polars_join_streaming_false_then_allow_parallel_false(captured_join_kwargs: list) -> None:
    adapter = PolarsAdapter()
    _inner_join(adapter, JoinOptions(streaming=False, allow_parallel=False))
    assert captured_join_kwargs[-1]["allow_parallel"] is False
