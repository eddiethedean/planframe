## PlanFrame

PlanFrame is a typed relational planning layer for Python DataFrames.

**Principle**: PlanFrame is **always lazy**. Every transformation builds a typed plan and evolves schema metadata. **No backend work runs** until you call `collect()`.

This repository is a mono-repo that currently ships:
- **`planframe`**: core package (import as `planframe`)
- **`planframe-polars`**: Polars adapter (import as `planframe_polars`)

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
from dataclasses import dataclass

from planframe.expr import add, col, lit
from planframe_polars import from_polars


@dataclass(frozen=True)
class UserSchema:
    id: int
    name: str
    age: int


lf = pl.DataFrame({"id": [1], "name": ["a"], "age": [10]}).lazy()
pf = from_polars(lf, schema=UserSchema)

out = (
    pf.select("id", "name", "age")
    .with_column("age_plus_one", add(col("age"), lit(1)))
    .rename(name="full_name")
)

Output = out.materialize_model("Output", kind="dataclass")
df = out.collect()
```

### API surface (MVP)

- **Frames**: `Frame.source(...)` (via adapter helper like `from_polars(...)`), immutable chaining (lazy)
- **Transforms**: `select`, `drop`, `rename`, `with_column`, `cast`, `filter`
- **Boundaries**:
  - `collect()` executes the accumulated plan using the adapter/backend
  - `materialize_model(kind="dataclass" | "pydantic")` materializes a Python model from the derived schema (no execution)

### Execution model

- **Always lazy**: chaining operations does not touch backend data.
- **Backend-independent**: even if a backend is naturally eager (e.g. pandas), PlanFrame still builds a plan and defers execution to `collect()`.

### Safe subset (MVP)

- **Supported**: `select`, `drop`, `rename`, `with_column`, `cast`, `filter`, `collect`, `materialize_model`
- **Not supported (yet)**: joins, groupby/agg, arbitrary Python functions / `.apply(...)`

### Development

Run tests:

```bash
source .venv/bin/activate
pytest
```
 
