## PlanFrame

[![CI](https://github.com/eddiethedean/planframe/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/eddiethedean/planframe/actions/workflows/ci.yml?query=branch%3Amain)
[![Docs](https://readthedocs.org/projects/planframe/badge/?version=latest)](https://planframe.readthedocs.io/en/latest/)
[![PyPI - planframe](https://img.shields.io/pypi/v/planframe)](https://pypi.org/project/planframe/)
[![PyPI - planframe-polars](https://img.shields.io/pypi/v/planframe-polars)](https://pypi.org/project/planframe-polars/)
[![License: MIT](https://img.shields.io/badge/License-MIT-informational)](LICENSE)

PlanFrame is a typed relational planning layer for Python DataFrames.

**Principle**: PlanFrame is **always lazy**. Every transformation builds a typed plan and evolves schema metadata. **No backend work runs** until you hit an execution boundary like `collect()` / `to_dicts()` / `to_dict()` (or async equivalents).

**Highlights (through v1.3.0)**: [Migrating since v1.1.0](https://planframe.readthedocs.io/en/latest/planframe/guides/migrating-since-1-1/) (v1.2.0–v1.3.0); [API reference](https://planframe.readthedocs.io/en/latest/planframe/reference/api/) (`execute_plan_async`, `planframe.materialize`, …); [Typing design](https://planframe.readthedocs.io/en/latest/planframe/design/typing-design/) (Expr operator overload semantics); [CHANGELOG](https://github.com/eddiethedean/planframe/blob/main/CHANGELOG.md) (per-release notes).

This repository is a mono-repo that ships:
- **`planframe`**: core package (import as `planframe`; optional skins `planframe.spark` and `planframe.pandas` for PySpark-like and pandas-like ergonomics)
- **`planframe-polars`**: Polars adapter (import as `planframe_polars`)
- **`planframe-pandas`**: Pandas adapter (import as `planframe_pandas`; `PandasFrame` is built on the pandas-like skin)
- **`planframe-sparkless`**: sparkless adapter (import as `planframe_sparkless`; executes the Spark UI on the `sparkless` engine)

### Documentation (ReadTheDocs)

Documentation is organized **by package**:

- **[PlanFrame (core)](https://planframe.readthedocs.io/en/latest/planframe/)** — planning layer, adapter contracts, and shared guides (primary audience: adapter authors and contributors)
- **[planframe-polars](https://planframe.readthedocs.io/en/latest/planframe_polars/)** — Polars adapter
- **[planframe-pandas](https://planframe.readthedocs.io/en/latest/planframe_pandas/)** — pandas adapter
- **[planframe-sparkless](https://planframe.readthedocs.io/en/latest/planframe_sparkless/)** — sparkless adapter

Key pages:

- **Migrating since v1.1.0** (v1.2.0+ through **v1.3.0**): `https://planframe.readthedocs.io/en/latest/planframe/guides/migrating-since-1-1/`
- **Creating an adapter**: `https://planframe.readthedocs.io/en/latest/planframe/guides/creating-an-adapter/`
- **Third-party adapter checklist** (BaseAdapter surface, `compile_expr` / dtypes, columnar `materialize` vs `to_dict`, sync/async terminals): `https://planframe.readthedocs.io/en/latest/planframe/guides/creating-an-adapter/#third-party-adapter-checklist`
- **Adapter conformance kit** (third-party `BaseAdapter` CI): `https://planframe.readthedocs.io/en/latest/planframe/guides/adapter-conformance/`
- **Streaming rows**: `https://planframe.readthedocs.io/en/latest/planframe/guides/streaming-rows/`
- **PySpark-like API (`planframe.spark`)**: `https://planframe.readthedocs.io/en/latest/planframe/guides/pyspark-like-api/`
- **Pandas-like API (`planframe.pandas`)**: `https://planframe.readthedocs.io/en/latest/planframe/guides/pandas-like-api/`
- **Using planframe-polars**: `https://planframe.readthedocs.io/en/latest/planframe_polars/guides/using-planframe-polars/`
- **Using planframe-pandas**: `https://planframe.readthedocs.io/en/latest/planframe_pandas/guides/using-planframe-pandas/`
- **Using planframe-sparkless**: `https://planframe.readthedocs.io/en/latest/planframe_sparkless/`

### Install (end users)

Pick an adapter package:

```bash
pip install planframe-polars
# or
pip install planframe-pandas
# or
pip install planframe-sparkless
```

The adapter packages pull in `planframe` automatically.

### Install (development / monorepo)

This repo uses a local virtualenv at `.venv` (Python 3.10+ recommended).

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
pip install -e ".[dev]"

# editable workspace packages
pip install -e packages/planframe
pip install -e packages/planframe-polars
pip install -e packages/planframe-pandas
pip install -e packages/planframe-sparkless
```

### Quickstart (Polars)

```python
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

### What’s new

See `CHANGELOG.md` (workspace packages are released together). For upgrades from **v1.1.0**, read [Migrating since v1.1.0](https://planframe.readthedocs.io/en/latest/planframe/guides/migrating-since-1-1/) on ReadTheDocs.

### Learn more

- **Core concepts & design**: `https://planframe.readthedocs.io/en/latest/planframe/design/`
- **Light API reference (core)**: `https://planframe.readthedocs.io/en/latest/planframe/reference/api/`
- **Light API reference (polars)**: `https://planframe.readthedocs.io/en/latest/planframe_polars/reference/api/`
- **Light API reference (pandas)**: `https://planframe.readthedocs.io/en/latest/planframe_pandas/reference/api/`

### Public API (stable imports)

Preferred imports:

- `from planframe import Frame, Schema, JoinOptions, execute_plan, execute_plan_async`
- `from planframe import CompileExprContext, ExecutionOptions` (expression compile context; materialization hints)
- `from planframe import materialize_columns, materialize_into` (and `amaterialize_*` for async columnar export)
- `from planframe import expr` (then `expr.col`, `expr.lit`, `expr.add`, …; `==`, `&`, `|` on `Expr` build IR)
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

Contributor guide: **[CONTRIBUTING.md](CONTRIBUTING.md)** (Pyright fixtures, generated stub parity, `ty`).

Run tests:

```bash
source .venv/bin/activate
pytest
```

Third-party adapter authors can run PlanFrame’s **published** minimal conformance helper (`planframe.adapter_conformance`) in their own CI; see the docs: [Adapter conformance kit](https://planframe.readthedocs.io/en/latest/planframe/guides/adapter-conformance/).

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

# property-based (Hypothesis) tests
pytest -m property
```

Dependency vulnerability scan (uses `uv.lock` via `uv export` + `pip-audit`):

```bash
bash scripts/audit-deps.sh
```

Install tooling (included in `pip install -e ".[dev]"`): `hypothesis`, `pytest-cases`, `pip-audit`.

### Security

See `SECURITY.md` for how to report vulnerabilities. Dependency scanning runs in CI (`.github/workflows/security.yml`) and locally via `bash scripts/audit-deps.sh`.

### Continuous integration

On each push and pull request to `main`, CI runs Ruff, `ty`, the full pytest suite (including Pyright stub checks), a strict MkDocs build, and smoke wheel builds for all published packages (`.github/workflows/ci.yml`).

### Releasing to PyPI

Build wheels and sdists locally:

```bash
bash scripts/build-dist.sh
```

Upload (requires a [PyPI API token](https://pypi.org/manage/account/token/) with scope for the relevant projects):

```bash
UV_PUBLISH_TOKEN=pypi-... uv publish "dist/*"
```

Or use GitHub Actions **Publish to PyPI** (`.github/workflows/publish-pypi.yml`): run the workflow manually, or trigger it by [publishing a GitHub Release](https://github.com/eddiethedean/planframe/releases). Configure [trusted publishing](https://docs.pypi.org/trusted-publishers/) on PyPI for each package (`planframe`, `planframe-polars`, `planframe-pandas`, `planframe-sparkless`) so OIDC can upload without a long-lived token.

