## PlanFrame

PlanFrame is a typed relational planning layer for Python DataFrames.

**Principle**: PlanFrame is **always lazy**. Every transformation builds a typed plan and evolves schema metadata. **No backend work runs** until you call `collect()`.

This repository is a mono-repo that currently ships:
- **`planframe`**: core package (import as `planframe`)
- **`planframe-polars`**: Polars adapter (import as `planframe_polars`)

### Guides

- **PlanFrame (core) – creating an adapter**: `https://github.com/eddiethedean/planframe/blob/main/docs/guides/planframe/creating-an-adapter.md`
- **PlanFrame-Polars – using planframe-polars**: `https://github.com/eddiethedean/planframe/blob/main/docs/guides/planframe-polars/using-planframe-polars.md`

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
    .with_column("age_plus_one", add(col("age"), lit(1)))
    .rename(name="full_name")
)

Output = out.materialize_model("Output", kind="dataclass")
df = out.collect()
```

### API surface

- **Frames**: `Frame.source(...)` (or backend frame constructors like `class User(PolarsFrame): ...; User(data)`), immutable chaining (lazy)
- **Transforms**:
  - **projection**: `select` (column names and/or `("out_name", expr)` tuples in one step; lowers to `Project` when expressions are present), `drop`, `select_exclude`
  - **column order**: `reorder_columns`, `select_first`, `select_last`, `move`
  - **rename helpers**: `rename` (optional `strict=False` ignores unknown source names, like `drop`), `rename_prefix`, `rename_suffix`, `rename_replace`
  - **row ops**: `head`, `tail`, `limit`, `slice`
  - **null helpers**: `drop_nulls`, `fill_null`
  - **reshape**: `melt`, `pivot` (see note below)
  - **set-like**: `concat_vertical`
  - **dedupe**: `unique`, `duplicated`
  - **joins**: `join` (`on` / `left_on` / `right_on` may mix column names and `Expr` keys)
  - **grouping**: `group_by(...).agg(...)` (keys may be column names or expressions; expression keys appear as `__pf_g0`, `__pf_g1`, … in the result schema; aggregations may be `(op, column)` or `agg_sum(expr)` / `agg_mean(expr)` / … over any supported expression)
  - **core**: `with_column`, `cast`, `filter`, `sort` (keys may be column names and/or `Expr`; schema unchanged)
- **Boundaries**:
  - `collect()` executes the accumulated plan using the adapter/backend
  - `materialize_model(kind="dataclass" | "pydantic")` materializes a Python model from the derived schema (no execution)

### Public API (stable imports)

Preferred imports:

- `from planframe import Frame, Schema, JoinOptions, execute_plan`
- `from planframe import expr` (then `expr.col`, `expr.lit`, `expr.add`, ...)

Backend-specific frames (example):

- `from planframe_polars import PolarsFrame`

### Execution model

- **Always lazy**: chaining operations does not touch backend data.
- **Backend-independent**: even if a backend is naturally eager (e.g. pandas), PlanFrame still builds a plan and defers execution to `collect()`.

### Notes / constraints

- **Typing**: PlanFrame ships heavy `.pyi` stubs to encourage *literal* column names and provide better Pyright feedback. Re-generate with `python scripts/generate_typing_stubs.py` and validate drift with `python scripts/generate_typing_stubs.py --check`.
- **Pivot (Polars)**: lazy pivot requires `on_columns` to be provided up-front (Polars needs output schema known before `collect()`).
- **Not supported**: arbitrary Python UDFs / `.apply(...)` and schema-dependent compile-time column-name unions (that would require per-schema codegen).

### Development

Run tests:

```bash
source .venv/bin/activate
pytest
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
 
