# Contributing to PlanFrame

## Typing, stubs, and CI contract

PlanFrame ships `.pyi` stubs (notably `packages/planframe/planframe/frame/__init__.pyi`) and strict Pyright fixtures so downstream packages get consistent editor feedback. The CI job enforces two layers:

### 1. Pyright pass/fail fixtures (`tests/pyright/`)

- **`tests/pyright/pass/*.py`** must type-check clean under **strict** Pyright (`tests/pyright/pyrightconfig.json`).
- **`tests/pyright/fail/*.py`** must **fail** Pyright (negative tests for intentional errors).
- The harness is `tests/test_pyright_typing.py` (marker **`typing`**).

Add **`tests/pyright/pass/*.py`** files when you need a **core-only** contract (no Polars `Frame`) for `planframe.expr` IR — for example `expr_ir_public_contract.py` covers common `col` / `lit` / helper patterns so stub/runtime drift shows up in CI.

### 2. Generated stub parity (`scripts/generate_typing_stubs.py`)

Frame-related stubs are **generated** from Jinja templates. CI runs:

```bash
python scripts/generate_typing_stubs.py --check
```

If this fails, regenerate and commit:

```bash
python scripts/generate_typing_stubs.py
```

The same check is duplicated in **`tests/test_stub_generation_parity.py`** (marker **`typing`**) so `pytest -m typing` runs Pyright fixtures **and** the stub diff in one go.

### 3. Astral `ty`

First-party packages are checked with **`ty`** (see `pyproject.toml` `[tool.ty]`). It complements Pyright; keep both green on PRs.

## Running tests locally

```bash
source .venv/bin/activate
pytest
pytest -m typing          # Pyright fixtures + stub generation check
pytest -m "not typing"    # skip typing-only tests
```

## Documentation

Docs build with MkDocs (`mkdocs build --strict` in CI). Anchor links in Markdown should resolve (use stable heading ids where needed).
