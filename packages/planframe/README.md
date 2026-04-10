## planframe

[![Docs](https://readthedocs.org/projects/planframe/badge/?version=latest)](https://planframe.readthedocs.io/en/latest/planframe/)
[![PyPI](https://img.shields.io/pypi/v/planframe)](https://pypi.org/project/planframe/)
[![License: MIT](https://img.shields.io/badge/License-MIT-informational)](../../LICENSE)

Core package for PlanFrame (typed planning layer). Import as `planframe`.

Documentation (ReadTheDocs):

- Core (adapter authors): `https://planframe.readthedocs.io/en/latest/planframe/`
- **Migrating since v1.1.0** (v1.2.0+): `https://planframe.readthedocs.io/en/latest/planframe/guides/migrating-since-1-1/`
- Design docs: `https://planframe.readthedocs.io/en/latest/planframe/design/`
- Light API reference: `https://planframe.readthedocs.io/en/latest/planframe/reference/api/`
- Streaming rows: `https://planframe.readthedocs.io/en/latest/planframe/guides/streaming-rows/`
- Adapter conformance kit (third-party `BaseAdapter` CI): `https://planframe.readthedocs.io/en/latest/planframe/guides/adapter-conformance/`
- Optional API skins: [PySpark-like (`planframe.spark`)](https://planframe.readthedocs.io/en/latest/planframe/guides/pyspark-like-api/), [pandas-like (`planframe.pandas`)](https://planframe.readthedocs.io/en/latest/planframe/guides/pandas-like-api/)

### Install

`planframe` is backend-agnostic; you typically install an adapter package like `planframe-polars` or `planframe-pandas`.

If you only want the core planning layer:

```bash
pip install planframe
```

### What you get
- `planframe.Frame`: immutable, schema-aware transformation plan (**always lazy**)
- `planframe.expr`: typed expression IR (`col`, `lit`, arithmetic/compare/boolean ops, `coalesce`, `if_else`, etc.); **operator overloads** on `Expr` (`==`, `!=`, `&`, `|`, `~`, …) build IR nodes—see [Typing design](https://planframe.readthedocs.io/en/latest/planframe/design/typing-design/). **Aggregation wrappers** for `group_by(...).agg(...)`: `agg_sum`, `agg_mean`, `agg_min`, `agg_max`, `agg_count`, `agg_n_unique` (these build `AggExpr` nodes)
- `planframe.groupby.GroupedFrame`: produced by `Frame.group_by`; **`group_by`** accepts column names and/or expressions (expression keys show up as `__pf_g0`, `__pf_g1`, … in the result schema). **`agg`** accepts `(op, column)` tuples and/or `AggExpr` values—not arbitrary bare expressions
- `planframe.schema`: schema reflection (dataclass + Pydantic) and materialization
- `planframe.spark`: optional PySpark-like `SparkFrame` / `Column` / `functions` (import `from planframe.spark import SparkFrame`, or `from planframe import spark`)
- `planframe.pandas`: optional pandas-like `PandasLikeFrame` / `Series` (import `from planframe.pandas import PandasLikeFrame`, or `from planframe import pandas`); mix with any `Frame` subclass for familiar naming without new backend dependencies
- `planframe.adapter_conformance`: minimal **`run_minimal_adapter_conformance`** helper for adapter authors; optional extra **`planframe[adapter-dev]`** includes pytest for local runs

### Common transforms

Some commonly used Frame transforms:

- `with_row_index(name="row_nr", offset=0)`: add a monotonically increasing row number column.
- `clip(lower=..., upper=..., subset=...)`: clamp numeric columns (if `subset=None`, clamps all numeric schema fields).
- `drop_nulls(subset=..., how="any"|"all", threshold=...)`: drop rows by null pattern over a column subset.
- `select_schema(selector, strict=True)`: schema-only selectors (backend-independent); `ColumnSelector` is runtime-checkable.
- `cast_many(mapping, strict=True)` / `cast_subset(*columns, dtype, strict=True)`: multi-column cast helpers.
- `fill_null_subset(value|strategy, *columns)` / `fill_null_many(mapping, strict=True)`: multi-column fill-null helpers.
- `rename_upper/lower/title/strip(...)`: schema-driven rename helpers.
- `pivot_longer(...)` / `pivot_wider(...)`: reshape convenience wrappers around `unpivot` / `pivot`.

Materialization accepts optional **`ExecutionOptions`** on `collect` / `to_dicts` / `to_dict` (and async counterparts). **`JoinOptions`** on `Frame.join` carries execution hints (including `engine_streaming` where the backend supports it).

**`planframe.materialize`**: `materialize_columns` / `materialize_into` (and `amaterialize_*`) forward the same options as `Frame.to_dict` / `ato_dict`—useful for adapter and host-library boundaries ([Creating an adapter](https://planframe.readthedocs.io/en/latest/planframe/guides/creating-an-adapter/) — columnar helpers).

**`execute_plan` / `execute_plan_async`**: the supported plan interpreters; `execute_plan_async` runs the sync interpreter in `asyncio.to_thread` so you can `await` without blocking the event loop ([Core layout](https://planframe.readthedocs.io/en/latest/planframe/design/core-layout/)).

### Note on backends
`planframe` is backend-agnostic. It does not execute anything until `collect()` (even for eager backends). To execute plans you need an adapter package (e.g. `planframe-polars`).

For async stacks, use `Frame.acollect()` / `ato_dicts()` / `ato_dict()` or the discoverable aliases **`collect_async`**, **`to_dicts_async`**, **`to_dict_async`** (same behavior). These await adapter hooks (`BaseAdapter.acollect` and friends); defaults run sync methods in a thread pool. See [Backend adapter design](https://planframe.readthedocs.io/en/latest/planframe/design/backend-adapter-design/) and [Creating an adapter — Async execution](https://planframe.readthedocs.io/en/latest/planframe/guides/creating-an-adapter/#async-execution-contract-third-party-adapters).

### Typing
PlanFrame includes `py.typed` plus generated stubs (notably `planframe/frame/__init__.pyi`) to improve static typing in editors and Pyright.

If you modify the `Frame` API, regenerate stubs from the repo root:

```bash
python scripts/generate_typing_stubs.py
python scripts/generate_typing_stubs.py --check
```
 
