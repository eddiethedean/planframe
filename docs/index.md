# PlanFrame documentation

PlanFrame is a typed, backend-agnostic planning layer for dataframe engines.

Choose a path:

- **PlanFrame (core)**: build a statically-typed adapter for a backend engine.
- **planframe-polars**: use PlanFrame on top of Polars.
- **planframe-pandas**: use PlanFrame on top of Pandas.
- **planframe-sparkless**: use PlanFrame’s Spark-like interface on top of `sparkless`.

## Install

Install an adapter package (recommended):

```bash
pip install planframe-polars
# or
pip install planframe-pandas
# or
pip install planframe-sparkless
```

Core-only (adapter authors / libraries):

```bash
pip install planframe
```

## Start here

- [PlanFrame (core)](planframe/index.md)
- [planframe-polars](planframe_polars/index.md)
- [planframe-pandas](planframe_pandas/index.md)
- [planframe-sparkless](planframe_sparkless/index.md)
- [PySpark-like API (`planframe.spark`)](planframe/guides/pyspark-like-api.md)
- [Pandas-like API (`planframe.pandas`)](planframe/guides/pandas-like-api.md)
- [Adapter capability matrix](adapters/capability-matrix.md)
- [Stability & compatibility](shared/stability-and-compatibility.md)

## What’s new (high-level)

See the project changelog: `https://github.com/eddiethedean/planframe/blob/main/CHANGELOG.md`

## Adding a new adapter

If you’re creating a new adapter, start with:

- [Creating an adapter](planframe/guides/creating-an-adapter.md)
- [Core layout](planframe/design/core-layout.md) (how `Frame`, compilation, and `execute_plan` are organized in the codebase)
- [Adapter docs template](adapters/template/README.md)

