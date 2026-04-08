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

## What’s new (high-level)

- `Frame.with_row_count(...)`: add a monotonically increasing row-number column (lazy).
- `Frame.clip(...)`: clamp numeric columns (lazy).
- `Frame.select_schema(...)`: schema-only column selectors (backend-independent).
- Multi-column helpers: `cast_many` / `cast_subset` and `fill_null_many` / `fill_null_subset`.
- Rename helpers: `rename_upper` / `rename_lower` / `rename_title` / `rename_strip`.
- Reshape helpers: `pivot_longer` / `pivot_wider`.

## Adding a new adapter

If you’re creating a new adapter, start with:

- [Creating an adapter](planframe/guides/creating-an-adapter.md)
- [Adapter docs template](adapters/template/README.md)

