"""Issue #114: documented policy — permissive compile_expr for unknown column names (shipped adapters)."""

from __future__ import annotations

import pytest

from planframe.backend.adapter import CompileExprContext
from planframe.expr import col
from planframe.schema.ir import Field, Schema


def test_resolve_dtype_none_is_not_fatal_for_unknown_name() -> None:
    """None means no dtype hint; BaseAdapter default does not imply an error."""
    from test_core_lazy_and_schema import SpyAdapter

    schema = Schema(fields=(Field(name="a", dtype=int),))
    ctx = CompileExprContext(schema=schema)
    adapter = SpyAdapter()
    assert adapter.resolve_dtype("not_in_schema", ctx=ctx) is None


@pytest.mark.parametrize(
    "import_mod,adapter_cls",
    [
        pytest.param("planframe_polars", "PolarsAdapter", id="polars"),
        pytest.param("planframe_pandas", "PandasAdapter", id="pandas"),
    ],
)
def test_shipped_adapter_compile_expr_permissive_unknown_column(
    import_mod: str, adapter_cls: str
) -> None:
    """Shipped adapters lower Col even when the name is absent from ctx.schema (execution may fail)."""
    pytest.importorskip(import_mod)
    mod = __import__(import_mod, fromlist=[adapter_cls])
    adapter = getattr(mod, adapter_cls)()

    schema = Schema(fields=(Field(name="a", dtype=int),))
    ctx = CompileExprContext(schema=schema)
    assert adapter.resolve_dtype("ghost", ctx=ctx) is None

    out = adapter.compile_expr(col("ghost"), schema=schema, ctx=ctx)
    assert out is not None


def test_polars_unknown_column_fails_at_execution_not_compile() -> None:
    """Representative engine: invalid column surfaces when the lazy plan runs."""
    pytest.importorskip("polars")
    pytest.importorskip("planframe_polars")

    import polars as pl
    import polars.exceptions as plx

    from planframe_polars import PolarsAdapter

    adapter = PolarsAdapter()
    df = pl.DataFrame({"a": [1]})
    schema = Schema(fields=(Field(name="a", dtype=int),))
    ctx = CompileExprContext(schema=schema)
    compiled = adapter.compile_expr(col("ghost"), schema=schema, ctx=ctx)
    lazy = df.lazy().filter(compiled)
    with pytest.raises(plx.ColumnNotFoundError):
        lazy.collect()
