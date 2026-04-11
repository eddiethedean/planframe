"""Issue #113: backend-aware dtype recovery via CompileExprContext.resolve_backend_dtype."""

from __future__ import annotations

import pytest
from test_core_lazy_and_schema import SpyAdapter

from planframe.backend.adapter import CompileExprContext
from planframe.schema.ir import Field, Schema


def test_resolve_dtype_uses_backend_callback_when_schema_omits_column() -> None:
    adapter = SpyAdapter()
    partial = Schema(fields=(Field(name="id", dtype=int),))
    ctx = CompileExprContext(
        schema=partial,
        resolve_backend_dtype=lambda n: float if n == "extra" else None,
    )
    assert adapter.resolve_dtype("extra", ctx=ctx) is float
    assert adapter.resolve_dtype("missing", ctx=ctx) is None


def test_resolve_dtype_schema_wins_over_backend_callback() -> None:
    adapter = SpyAdapter()
    partial = Schema(fields=(Field(name="id", dtype=int),))
    ctx = CompileExprContext(
        schema=partial,
        resolve_backend_dtype=lambda n: str,
    )
    assert adapter.resolve_dtype("id", ctx=ctx) is int


def test_resolve_dtype_chains_backend_frame_without_float_fallback() -> None:
    """When the step schema omits a column, resolve_dtype consults the execution callback."""

    pytest.importorskip("polars")
    pytest.importorskip("planframe_polars")

    import polars as pl

    from planframe_polars import PolarsAdapter

    adapter = PolarsAdapter()
    df = pl.DataFrame({"a": [1]})
    partial = Schema(fields=(Field(name="x", dtype=int),))
    ctx = CompileExprContext(
        schema=partial,
        resolve_backend_dtype=lambda n: adapter.resolve_backend_dtype_from_frame(df, n),
    )
    assert adapter.resolve_dtype("a", ctx=ctx) == pl.Int64
    assert adapter.resolve_dtype("nope", ctx=ctx) is None
