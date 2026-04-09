## PlanFrame

[![Docs](https://readthedocs.org/projects/planframe/badge/?version=latest)](https://planframe.readthedocs.io/en/latest/)
[![PyPI - planframe](https://img.shields.io/pypi/v/planframe)](https://pypi.org/project/planframe/)
[![PyPI - planframe-polars](https://img.shields.io/pypi/v/planframe-polars)](https://pypi.org/project/planframe-polars/)
[![License: MIT](https://img.shields.io/badge/License-MIT-informational)](LICENSE)

PlanFrame is a typed relational planning layer for Python DataFrames.

**Principle**: PlanFrame is **always lazy**. Every transformation builds a typed plan and evolves schema metadata. **No backend work runs** until you call `collect()`.

This repository is a mono-repo that currently ships:
- **`planframe`**: core package (import as `planframe`; optional skins `planframe.spark` and `planframe.pandas` for PySpark-like and pandas-like ergonomics)
- **`planframe-polars`**: Polars adapter (import as `planframe_polars`)
- **`planframe-pandas`**: Pandas adapter (import as `planframe_pandas`; `PandasFrame` is built on the pandas-like skin)

### Documentation (ReadTheDocs)

The docs are organized into two clear tracks:

- **PlanFrame (core)** (adapter authors): `https://planframe.readthedocs.io/en/latest/planframe/`
- **planframe-polars** (end users): `https://planframe.readthedocs.io/en/latest/planframe_polars/`
- **planframe-pandas** (end users): `https://planframe.readthedocs.io/en/latest/planframe_pandas/`

Key pages:

- **Creating an adapter**: `https://planframe.readthedocs.io/en/latest/planframe/guides/creating-an-adapter/`
- **Streaming rows**: `https://planframe.readthedocs.io/en/latest/planframe/guides/streaming-rows/`
- **PySpark-like API (`planframe.spark`)**: `https://planframe.readthedocs.io/en/latest/planframe/guides/pyspark-like-api/`
- **Pandas-like API (`planframe.pandas`)**: `https://planframe.readthedocs.io/en/latest/planframe/guides/pandas-like-api/`
- **Using planframe-polars**: `https://planframe.readthedocs.io/en/latest/planframe_polars/guides/using-planframe-polars/`
- **Using planframe-pandas**: `https://planframe.readthedocs.io/en/latest/planframe_pandas/guides/using-planframe-pandas/`

### Install (development)

This repo uses a local virtualenv at `.venv` (Python 3.10+ recommended).

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e packages/planframe
pip install -e packages/planframe-polars
```

### Quickstart (Polars)

```python
import polars as pl

from planframe.expr import add, col, lit
from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    name: str
    age: int

pf = User({"id": [1], "name": ["a"], "age": [10]})

out = (
    pf.select("id", "name", "age")
    .with_columns(age_plus_one=add(col("age"), lit(1)))
    .rename(name="full_name")
    .with_row_index(name="row_nr")
)

Output = out.materialize_model("Output", kind="dataclass")
df = out.collect()
```

### What’s new in v0.8.0

- **`planframe.pandas`**: pandas-like `PandasLikeFrame` and `Series` (typed boolean indexing, column `filter`, `astype`, `eval`, `drop_duplicates`, and compatibility with core `Frame` methods).
- **`planframe.spark`**: PySpark-style ergonomics—`df["col"]` / `df.col`, `withColumns`, `groupBy().agg(**...)`, and `hint()`—plus a **`Hint`** plan node and optional `BaseAdapter.hint()` for backends.
- **planframe-pandas**: `PandasFrame` is layered on `PandasLikeFrame`, so the pandas adapter ships the pandas-flavored API by default.

### What shipped in v0.7.1

- **pandas**: `fill_null(..., strategy=...)` fills only the requested subset columns (parity with Polars).
- **pandas**: `drop_nulls(..., threshold=...)` no longer triggers a pandas `how`/`thresh` conflict; behavior matches Polars for threshold-based dropping.
- **schema**: unnest field names inferred from Pydantic v2 `BaseModel` types (`model_fields`).
- **planframe-polars**: `JoinOptions.force_parallel` maps to Polars `force_parallel`; join hint precedence documented on `JoinOptions`.
- **`planframe.spark`**: PySpark-like `SparkFrame`, `Column`, and `functions` on the core package (no Spark dependency).

### What shipped in v0.7.0

- **Async materialization**: `Frame.acollect()`, `Frame.ato_dicts()`, `Frame.ato_dict()` with `ExecutionOptions`; `BaseAdapter` provides async hooks (defaults use `asyncio.to_thread` around sync methods).
- **`drop_nulls`**: `how="any"` / `how="all"` and optional `threshold` (row-wise null logic), aligned with common dataframe semantics.
- **Join hints**: `JoinOptions.engine_streaming` (alongside `streaming`) for backends that distinguish user- vs engine-level streaming.
- **Selectors**: `ColumnSelector` is `@runtime_checkable`, so `isinstance(obj, ColumnSelector)` works for built-in selector types and structural matches.
- **Docs**: the [Creating an adapter](https://planframe.readthedocs.io/en/latest/planframe/guides/creating-an-adapter/) guide now documents `ExecutionOptions` at materialization boundaries and the full `JoinOptions` field list (omit-`None` forwarding).

### Highlights from v0.6.x

- `Frame.with_row_index(name="row_nr", offset=0)` to add a monotonically increasing row number column (lazy).
- `Frame.clip(lower=..., upper=..., subset=...)` to clamp numeric columns (lazy; `subset=None` clamps all numeric schema fields).
- `Frame.select_schema(selector, strict=True)`: schema-only selectors (no backend dependency).
- Multi-column helpers: `cast_many`, `cast_subset`, `fill_null_subset`, `fill_null_many`.
- Rename helpers: `rename_upper`, `rename_lower`, `rename_title`, `rename_strip`.
- Reshape helpers: `pivot_longer`, `pivot_wider`.
- Execution hints: `ExecutionOptions(...)` at materialization time and join hints via `JoinOptions` (e.g. `allow_parallel`, `force_parallel`).

### Learn more

- **Core concepts & design**: `https://planframe.readthedocs.io/en/latest/planframe/design/`
- **Light API reference (core)**: `https://planframe.readthedocs.io/en/latest/planframe/reference/api/`
- **Light API reference (polars)**: `https://planframe.readthedocs.io/en/latest/planframe_polars/reference/api/`
- **Light API reference (pandas)**: `https://planframe.readthedocs.io/en/latest/planframe_pandas/reference/api/`

### Public API (stable imports)

Preferred imports:

- `from planframe import Frame, Schema, JoinOptions, execute_plan`
- `from planframe import expr` (then `expr.col`, `expr.lit`, `expr.add`, ...)
- `from planframe import spark` / `from planframe import pandas` (lazy submodules: PySpark-like and pandas-like skins)

Backend-specific frames (example):

- `from planframe_polars import PolarsFrame`
- `from planframe_pandas import PandasFrame`

### Execution model

- **Always lazy**: chaining operations does not touch backend data.
- **Backend-independent**: even if a backend is naturally eager (e.g. pandas), PlanFrame still builds a plan and defers execution to `collect()`.

### Notes / constraints

- **Typing**: PlanFrame ships heavy `.pyi` stubs (notably `packages/planframe/planframe/frame/__init__.pyi` for `Frame`) to encourage *literal* column names and provide better Pyright feedback. Re-generate with `python scripts/generate_typing_stubs.py` and validate drift with `python scripts/generate_typing_stubs.py --check`.
- **Pivot (Polars)**: lazy pivot requires `on_columns` to be provided up-front (Polars needs output schema known before `collect()`).
- **Not supported**: arbitrary Python UDFs / `.apply(...)` and schema-dependent compile-time column-name unions (that would require per-schema codegen).

### Development

Run tests:

```bash
source .venv/bin/activate
pytest
```

Run tests in parallel (optional):

```bash
pytest -n 10
```

Run subsets:

```bash
# skip typing-only tests (pyright + stub generation)
pytest -m "not typing"

# only typing-only tests
pytest -m typing

# only backend conformance (Polars) tests
pytest -m conformance
```
 
