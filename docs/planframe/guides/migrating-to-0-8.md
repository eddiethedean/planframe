# Migrating to v0.8.0

This guide summarizes the main changes when upgrading to **PlanFrame v0.8.0** (workspace release for `planframe`, `planframe-polars`, and `planframe-pandas`).

## Quick upgrade checklist

- Upgrade the workspace packages together:
  - `planframe==0.8.0`
  - `planframe-polars==0.8.0` (if you use Polars)
  - `planframe-pandas==0.8.0` (if you use pandas)
- If you use the pandas backend, note that `PandasFrame` now includes the pandas-like skin by default (see below).
- If you previously used `planframe-spark`, switch to the **core Spark skin** (`planframe.spark`) (see below).

## What’s new in v0.8.0 (high level)

From the changelog (`CHANGELOG.md`):

- **`planframe.pandas` skin**: adds a pandas-like API on top of lazy `Frame` via `PandasLikeFrame` + `Series`.
- **Spark skin**: adds a PySpark-like API via `SparkFrame`, `Column`, and `functions`, plus a `hint()` plan node and optional adapter hook.
- **planframe-pandas** now ships `PandasFrame` built on `PandasLikeFrame`.

## Core change: pandas-like skin (`planframe.pandas`)

### What you get

`planframe.pandas.PandasLikeFrame` is a **typed mixin** that provides pandas-flavored naming and conveniences, while still building a PlanFrame plan (still lazy).

Examples you can now use on frames that include the mixin:

- `df.assign(...)`
- `df.sort_values(...)`
- typed boolean indexing and `df.query(expr)` (typed; not a string expression)
- pandas-like `drop(...)` / `rename(...)` overloads that remain compatible with PlanFrame core behavior

### Migration note for pandas backend users

In v0.8.0, **`planframe_pandas.PandasFrame` subclasses `PandasLikeFrame`**, so you get the pandas-like API by default.

If you already had helper methods with the same names (e.g. your own `assign`, `query`, `sort_values`), you may need to rename them to avoid method resolution conflicts.

## Core change: Spark skin (`planframe.spark`)

### If you used `planframe-spark` previously

The PySpark-like API is now provided by the core package:

- `from planframe.spark import SparkFrame`
- `from planframe.spark import Column`
- `from planframe.spark import functions as F`

There is **no Spark dependency**; this is an API skin that still executes via an adapter at `collect()`.

### New capabilities

- Column access sugar: `df["x"]` / `df.x`
- `withColumns({...})`
- `groupBy().agg(**named_aggs)`
- `hint()` support via a core `Hint` plan node; adapters may optionally implement `BaseAdapter.hint()`

## Notes for adapter authors

### `hint()` support

If you’re writing a backend adapter and want to support `hint()`, implement the optional adapter hook (see the adapter guide and backend-adapter design notes in the docs).

If you do not implement it, `hint()` should be treated as a no-op at execution time (it remains in the plan for possible inspection/optimization).

## Common issues

### “I expected pandas semantics / eager evaluation”

PlanFrame remains **always lazy**. Even for naturally eager backends (like pandas), PlanFrame builds a plan and defers execution to `collect()`.

### Method name conflicts with the new skins

If you created your own `Frame` subclasses with methods like `assign`, `query`, `sort_values`, `withColumns`, `groupBy`, etc., you may need to rename your methods or adjust mixin ordering.

## Links

- Changelog: `CHANGELOG.md` (v0.8.0 section)
- pandas-like API guide: `planframe/guides/pandas-like-api.md`
- PySpark-like API guide: `planframe/guides/pyspark-like-api.md`
- Creating an adapter: `planframe/guides/creating-an-adapter.md`

