# Changelog

All notable changes to this repository are documented here.

- `planframe`, `planframe-polars`, and `planframe-pandas` are released together at the same version.
- `planframe-sparkless` is versioned independently.

## Unreleased

### Added

- **`planframe.materialize`**: `materialize_columns` / `materialize_into` (and async `amaterialize_*`) as thin, options-preserving boundaries for columnar export and downstream factories—documented in [Creating an adapter](docs/planframe/guides/creating-an-adapter.md) ([issue #107](https://github.com/eddiethedean/planframe/issues/107)).

- **`Expr` operator overloads** aligned with IR construction (`==`, `!=`, `&`, `|`, `~`, plus existing ordered comparisons): typing regression coverage in `tests/pyright/pass/expr_comparisons.py` and documented semantics in [typing-design](docs/planframe/design/typing-design.md) ([issue #106](https://github.com/eddiethedean/planframe/issues/106)).

## 1.2.0

### Fixed

- **`execute_plan`**: compile expressions (filter predicates, projections, sort keys, join keys, etc.) using each step’s **input** schema from the plan, not the final frame schema—fixes `filter(...).select(...)` when the predicate references columns dropped by the downstream projection ([issue #103](https://github.com/eddiethedean/planframe/issues/103)).

### Added

- **Async public API** ([issue #105](https://github.com/eddiethedean/planframe/issues/105)): **`execute_plan_async`** (runs :func:`execute_plan` via :func:`asyncio.to_thread`) and Frame aliases **`collect_async`**, **`collect_backend_async`**, **`to_dict_async`**, **`to_dicts_async`** (wrappers around existing ``a*`` materializers).

- **`BaseAdapter.resolve_dtype`**: optional hook for dtype-aware `Col(...)` lowering during `compile_expr`, with **`CompileExprContext`** (exported from `planframe`) carrying the active schema. Polars, pandas, and sparkless adapters invoke the hook for every column reference ([issue #104](https://github.com/eddiethedean/planframe/issues/104)).

## 1.1.0

### Added

- **Adapter conformance kit**: `planframe.adapter_conformance.run_minimal_adapter_conformance` for third-party `BaseAdapter` implementations, plus optional extra **`planframe[adapter-dev]`** (pytest). See the [Adapter conformance kit](https://planframe.readthedocs.io/en/latest/planframe/guides/adapter-conformance/) guide.

### Changed

- **Adapters**: expanded `BaseAdapter.capabilities` (`AdapterCapabilities`) to cover optional IO surfaces (read/sink hooks) and used it to fail fast on unsupported sinks.

### Typing

- Added regression coverage for `group_by(...)` with expression keys; improved robustness of the Pyright typing test runner.

## 1.0.0

### Added

- Stable 1.0 packaging for `planframe`, `planframe-polars`, and `planframe-pandas`.
- GitHub Actions **CI** workflow: Ruff, Astral `ty`, full pytest (including Pyright typing tests), strict MkDocs build, and smoke wheel builds for all packages.
- **`scripts/build-dist.sh`** and **Publish to PyPI** workflow (`.github/workflows/publish-pypi.yml`): build all packages into `dist/` and upload via trusted publishing or `uv publish`.

### Changed

- Documentation and READMEs were refreshed to reflect 1.0+ install paths and current contracts (async boundaries, streaming hooks, adapter notes).

### Typing

- Typing behavior (generated `.pyi` stubs and public generics like `Frame[...]`) is treated as part of PlanFrame’s public API. When releases change user-visible typing behavior, we document it here (see docs “Stability & compatibility” for policy).

## 0.8.0

### Added

- **`planframe.pandas`**: pandas-like `PandasLikeFrame` and `Series` on the core package (boolean indexing, column `filter`, `astype`, `eval`/`assign`, `drop_duplicates`, and overloads that stay compatible with core `Frame` APIs).
- **Spark skin**: `SparkFrame` column access (`df["x"]`, `df.x`), `withColumns`, `GroupedData.agg(**named_aggs)`, and `hint()`; **`Hint`** plan node and optional `BaseAdapter.hint()` hook (no-op unless implemented).

### Changed

- **planframe-pandas**: `PandasFrame` is built on `PandasLikeFrame`, so the pandas backend exposes the pandas-flavored skin by default.

### Documentation

- Guides: [PySpark-like API](https://planframe.readthedocs.io/en/latest/planframe/guides/pyspark-like-api/) and [pandas-like API](https://planframe.readthedocs.io/en/latest/planframe/guides/pandas-like-api/).

## planframe-sparkless 0.1.0

### Added

- Initial `planframe-sparkless` adapter package (Spark-like UI on top of the `sparkless` engine).

## 0.7.1

### Added

- **`planframe.spark`**: PySpark-like `SparkFrame`, `Column`, and `functions` submodule on the core `planframe` package (no Apache Spark dependency).

### Changed

- The PySpark-like API is provided by core `planframe` via `planframe.spark` (no Spark dependency).

### Documentation

- Workspace meta-package: optional `[docs]` extra in the repo root `pyproject.toml` (MkDocs, Material, mkdocstrings, section-index, PyMdown) for local documentation builds (`pip install -e ".[docs]"`).
- [Core layout](https://planframe.readthedocs.io/en/latest/planframe/design/core-layout/) design note: how `Frame`, compilation, and `execute_plan` are organized.

### Fixed

- **planframe-polars** / **planframe-pandas**: subclass constructors like `User(data)` now return `User` instances (`cls.source` / `cls._adapter_singleton`), enabling mixins such as `SparkFrame`.
- **pandas**: `fill_null(..., strategy=...)` applies forward/backward fill only to the selected subset columns (matches Polars).
- **pandas**: `drop_nulls(..., threshold=...)` no longer passes both `how` and `thresh` to pandas (avoids `TypeError`; threshold semantics align with Polars).
- **schema**: `Schema` unnest field inference supports Pydantic v2 models via `model_fields`.
- **planframe-polars**: `JoinOptions.force_parallel` forwards to Polars `force_parallel` (not mapped onto `allow_parallel`); `JoinOptions` documents Polars join hint precedence.

### Tests

- Parity tests for pandas vs Polars (`fill_null` strategy subset, `drop_nulls` with threshold); Polars join option kwargs coverage.

## 0.7.0

### Added

- Async materialization: `Frame.acollect`, `Frame.ato_dicts`, `Frame.ato_dict` with `ExecutionOptions`; `BaseAdapter` async hooks (defaults use `asyncio.to_thread`).
- `Frame.drop_nulls(..., how="any"|"all", threshold=...)` for row-wise null dropping.
- `JoinOptions.engine_streaming` (with `streaming`) for join execution hints.
- `ColumnSelector` documented and tested as `@runtime_checkable` for `isinstance` checks.

### Documentation

- Adapter guide: `ExecutionOptions` at materialization boundaries, full `JoinOptions` table and omit-`None` forwarding.
- Root and docs index updated for v0.7.0; FAQ and glossary expanded.

### Fixed

- Pandas adapter: `write_parquet` typing aligned with pandas stubs (`compression`).

### Tests

- Parquet “missing pyarrow” test is skipped when `pyarrow` is installed (e.g. full dev environment).
