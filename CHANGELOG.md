# Changelog

All notable changes to this project are documented here. Versions follow the workspace packages (`planframe`, `planframe-polars`, `planframe-pandas`), which are released together at the same version.

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
