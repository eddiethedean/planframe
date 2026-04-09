# Changelog

All notable changes to this project are documented here. Versions follow the workspace packages (`planframe`, `planframe-polars`, `planframe-pandas`), which are released together at the same version.

## 0.7.1

### Documentation

- Workspace meta-package: optional `[docs]` extra in the repo root `pyproject.toml` (MkDocs, Material, mkdocstrings, section-index, PyMdown) for local documentation builds (`pip install -e ".[docs]"`).
- [Core layout](https://planframe.readthedocs.io/en/latest/planframe/design/core-layout/) design note: how `Frame`, compilation, and `execute_plan` are organized.

### Fixed

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
