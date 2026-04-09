# PlanFrame documentation

PlanFrame is a typed, backend-agnostic planning layer for dataframe engines.

Choose a path:

- **PlanFrame (core)**: build a statically-typed adapter for a backend engine.
- **planframe-polars**: use PlanFrame on top of Polars.
- **planframe-pandas**: use PlanFrame on top of Pandas.

## Start here

- [PlanFrame (core)](planframe/index.md)
- [planframe-polars](planframe_polars/index.md)
- [planframe-pandas](planframe_pandas/index.md)
- [PySpark-like API (`planframe.spark`)](planframe/guides/pyspark-like-api.md)
- [Pandas-like API (`planframe.pandas`)](planframe/guides/pandas-like-api.md)

## What’s new (high-level)

**v0.8.0**

- **`planframe.pandas`**: pandas-like `PandasLikeFrame` / `Series` skin on the core package; **planframe-pandas**’s `PandasFrame` uses that skin.
- **Spark skin** (`planframe.spark`): column sugar, `withColumns`, typed `groupBy().agg`, and plan-level `hint()` with a core `Hint` node and optional adapter hook.

**v0.7.1**

- Bug fixes: pandas `fill_null` strategy respects subset; `drop_nulls` + `threshold` matches Polars and avoids pandas `how`/`thresh` conflict; Pydantic v2 unnest schema inference; Polars `JoinOptions.force_parallel` forwarding and documented join hint precedence.

**v0.7.0**

- Async boundaries: `acollect` / `ato_dicts` / `ato_dict` and `ExecutionOptions` on materialization.
- `drop_nulls(..., how=..., threshold=...)` for row-wise null handling.
- `JoinOptions.engine_streaming`; expanded adapter guide for execution vs join hints.
- `ColumnSelector` is runtime-checkable (`isinstance` supported).

**Earlier releases**

- `Frame.with_row_count(...)`: add a monotonically increasing row-number column (lazy).
- `Frame.clip(...)`: clamp numeric columns (lazy).
- `Frame.select_schema(...)`: schema-only column selectors (backend-independent).
- Multi-column helpers: `cast_many` / `cast_subset` and `fill_null_many` / `fill_null_subset`.
- Rename helpers: `rename_upper` / `rename_lower` / `rename_title` / `rename_strip`.
- Reshape helpers: `pivot_longer` / `pivot_wider`.

## Adding a new adapter

If you’re creating a new adapter, start with:

- [Creating an adapter](planframe/guides/creating-an-adapter.md)
- [Core layout](planframe/design/core-layout.md) (how `Frame`, compilation, and `execute_plan` are organized in the codebase)
- [Adapter docs template](adapters/template/README.md)

