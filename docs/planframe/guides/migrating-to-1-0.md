# Migrating to v1.0.0

This guide summarizes the main changes when upgrading to **PlanFrame v1.0.0** (workspace release for `planframe`, `planframe-polars`, and `planframe-pandas`).

## Quick upgrade checklist

- Upgrade the workspace packages together:
  - `planframe==1.0.0`
  - `planframe-polars==1.0.0` (if you use Polars)
  - `planframe-pandas==1.0.0` (if you use pandas)
- If you use the pandas backend, note that `PandasFrame` includes the pandas-like skin by default.
- If you use the Spark UI (`planframe.spark`), note it’s an in-core typing/interface surface (no Spark engine).

## What changed in v1.0.0 (high level)

- **Deprecated APIs were removed**. If you were relying on legacy method names, update to the canonical v1 method names (see below).
- **Spark remains UI-only in core** (`planframe.spark`). There is no `planframe-spark` adapter package as part of v1.0.0.

## Breaking API changes (common fixes)

### Materialization contract (documented divergence from parent libraries)

In v1.0.0, PlanFrame’s three typed interfaces intentionally share the same row-export surface:

- **`collect()` / `acollect()`**: returns **`list[pydantic.BaseModel]`** (row models derived from the current schema)
- **`to_dict()` / `ato_dict()`**: returns **`dict[str, list[object]]`**
- **`to_dicts()` / `ato_dicts()`**: returns **`list[dict[str, object]]`**

If you need the backend-native object (e.g. `polars.DataFrame` / `pandas.DataFrame`) use **`collect_backend()` / `acollect_backend()`**.

### Column creation

- **Removed**: `Frame.with_column(name, expr)`
- **Use**: `Frame.with_columns(...)`

Example:

```python
out = df.with_columns(new_col=expr)
```

### Row index

- **Removed**: `Frame.with_row_count(...)`
- **Use**: `Frame.with_row_index(...)`

### Reshaping

- **Removed**: `Frame.melt(...)`
- **Use**: `Frame.unpivot(...)`

### Concatenation

- **Removed**: `Frame.concat_vertical(...)` / `Frame.concat_horizontal(...)`
- **Use**: `Frame.vstack(...)` / `Frame.hstack(...)`

### Null dropping

- **Removed**: `drop_nulls(*subset, ...)` positional subset
- **Use**: `drop_nulls(subset=..., ...)`

### IO naming

PlanFrame is lazy-first, so IO methods are exposed as sinks. For Polars familiarity, PlanFrame also exposes `write_*` convenience entrypoints with the same typed surface as the corresponding `sink_*` methods:

- **Available**: `sink_*` methods (e.g. `sink_parquet`, `sink_csv`, ...)
- **Also available**: `write_*` methods (e.g. `write_parquet`, `write_csv`, ...)

## Notes for adapter authors

### Capabilities

Adapters now expose capability flags via `BaseAdapter.capabilities` to help frontends decide whether a feature is supported (or should fail loudly with a clear message).

## Common issues

### “I expected eager pandas semantics”

PlanFrame remains **always lazy**. Even for naturally eager backends (like pandas), PlanFrame builds a plan and defers execution to materialization boundaries like `collect()` / `collect_backend()` / `to_dicts()` / writes.

## Links

- Changelog: `CHANGELOG.md`
- pandas-like API guide: `planframe/guides/pandas-like-api.md`
- PySpark-like API guide: `planframe/guides/pyspark-like-api.md`
- Creating an adapter: `planframe/guides/creating-an-adapter.md`

