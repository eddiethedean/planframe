# Using `planframe-pandas`

This guide covers the intended public usage pattern:

- define a schema as a **`PandasFrame` subclass**
- construct frames from **Python-native data** (PlanFrame constructs pandas internally)
- chain transforms (always lazy)
- execute via boundaries (`collect`, `to_dicts`, `to_dict`, `collect(kind=...)`)

## Quickstart

Run:

```bash
./.venv/bin/python docs/planframe_pandas/guides/examples/basic_usage.py
```

Expected output:

```text
columns=['id', 'age', 'age_plus_one']
to_dict={'id': [1], 'age': [10], 'age_plus_one': [11]}
rows=[{'id': 1, 'age': 10, 'age_plus_one': 11}]
```

## Construction rules

- **Do**: `User({"id": [1], "age": [10]})`
- **Do**: `User([{"id": 1, "age": 10}])`
- **Don’t**: pass `pandas.DataFrame` directly into `User(...)`

If you need advanced construction, use `Frame.source(...)` with a backend frame (this is intentionally “escape hatch” territory).

## Execution model

PlanFrame is always lazy:

- Chaining methods (like `.select(...)`) does **not** run pandas operations.
- `collect()` evaluates the full plan by calling adapter methods on demand.

## Grouping and aggregation

`group_by` takes one or more **keys**, each either a column name (`str`) or a **`planframe.expr`** expression (same general idea as `sort` / `join` keys). Keys that are expressions are not named after a single input column; in the aggregated result they appear as **`__pf_g0`**, **`__pf_g1`**, … by position in the key list.

`agg` takes keyword arguments **`name=value`** where each value is either:

- **Tuple form**: `("op", "column")` with `op` one of `count`, `sum`, `mean`, `min`, `max`, `n_unique`.
- **Aggregation expression**: wrap any supported inner expression with **`agg_sum`**, **`agg_mean`**, **`agg_min`**, **`agg_max`**, **`agg_count`**, or **`agg_n_unique`** from `planframe.expr` (these produce `AggExpr` IR).

Example:

```python
from planframe.expr import agg_sum, col
from planframe_pandas import PandasFrame


class S(PandasFrame):
    g: int
    x: int


pf = S({"g": [1, 1, 2], "x": [10, 20, 7]})
out = pf.group_by("g").agg(n=("count", "x"), sx=agg_sum(col("x")))
df = out.collect()
```

## Reshape and nested data

- **melt**: implemented via `pandas.melt(...)`
- **pivot**: implemented via `DataFrame.pivot_table(...)`; if you pass `on_columns`, PlanFrame will ensure those output columns exist (filling missing with `NA`) and will reorder to match.
- **explode**: implemented via `DataFrame.explode(...)`
- **unnest**: expands dict-like values into columns (via `pandas.json_normalize`); name collisions raise an error.

## I/O and optional dependencies

- **CSV**: `write_csv(...)` works with the built-in pandas writer.
- **Parquet**: `write_parquet(...)` requires an extra engine. Install `planframe-pandas[parquet]` (uses `pyarrow`).

