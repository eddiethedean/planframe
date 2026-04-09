# Using `planframe-polars`

This guide covers the intended public usage pattern:

- define a schema as a **`PolarsFrame` subclass**
- construct frames from **Python-native data** (PlanFrame constructs Polars internally)
- chain transforms (always lazy)
- execute via boundaries (`collect`, `to_dicts`, `to_dict`, `collect(kind=...)`, or async: `acollect`, `ato_dicts`, `ato_dict`)

Optional **`ExecutionOptions`** (`streaming`, `engine_streaming`) can be passed at materialization time on `collect` / `to_dicts` / `to_dict` and the async variants.

## Optional PySpark- or pandas-style APIs

The core package provides typed mixins—**`planframe.spark.SparkFrame`** and **`planframe.pandas.PandasLikeFrame`**—that you can combine with `PolarsFrame` if you want column sugar (`df["x"]`, `withColumns`, `groupBy().agg`, `hint`, …) or pandas naming on the same plan. See [PySpark-like API (`planframe.spark`)](../../planframe/guides/pyspark-like-api.md) and [Pandas-like API (`planframe.pandas`)](../../planframe/guides/pandas-like-api.md).

## Quickstart

Run:

```bash
./.venv/bin/python docs/planframe_polars/guides/examples/basic_usage.py
```

Expected output:

```text
columns=['id', 'full_name', 'age_plus_one']
to_dict={'id': [1], 'full_name': ['a'], 'age_plus_one': [11]}
rows=[{'id': 1, 'full_name': 'a', 'age_plus_one': 11}]
row_models=[('Row', 1, 'a', 11)]
```

## Construction rules

- **Do**: `User({"id": [1], "name": ["a"], "age": [10]})`
- **Do**: `User([{"id": 1, "name": "a", "age": 10}])`
- **Don’t**: pass `polars.DataFrame` / `polars.LazyFrame` directly into `User(...)`

If you need advanced construction, use `Frame.source(...)` with a backend frame (this is intentionally “escape hatch” territory).

## Row numbering and clamping

Two common primitives:

- `with_row_count(name="row_nr", offset=0)` adds a monotonically increasing row number column.
- `clip(lower=..., upper=..., subset=...)` clamps numeric columns (if `subset=None`, PlanFrame clamps all numeric schema fields).

## Schema-only selectors and multi-column helpers

- `select_schema(selector, strict=True)` evaluates a selector object against the current PlanFrame `Schema` (backend-independent) and lowers to an explicit selection.
- Multi-column helpers: `cast_many`, `cast_subset`, `fill_null_many`, `fill_null_subset`.
- Rename helpers: `rename_upper/lower/title/strip(...)`.

## Reshape ergonomics

- `pivot_longer(...)` and `pivot_wider(...)` are convenience wrappers around `melt(...)` / `pivot(...)`.
- For deterministic output columns (especially on lazy sources), pass `on_columns` to `pivot_wider(...)`.

## Defaults for missing columns

If your schema defines defaults, PlanFrame will fill **missing input keys/columns** on construction.

Example:

```python
class User(PolarsFrame):
    id: int
    name: str
    age: int
    active: bool = True
```

If the input data omits `active`, it will be filled with `True` for all rows.

## Grouping and aggregation

`group_by` takes one or more **keys**, each either a column name (`str`) or a **`planframe.expr`** expression (same general idea as `sort` / `join` keys). Keys that are expressions are not named after a single input column; in the aggregated result they appear as **`__pf_g0`**, **`__pf_g1`**, … by position in the key list.

`agg` takes keyword arguments **`name=value`** where each value is either:

- **Tuple form**: `("op", "column")` with `op` one of `count`, `sum`, `mean`, `min`, `max`, `n_unique`.
- **Aggregation expression**: wrap any supported inner expression with **`agg_sum`**, **`agg_mean`**, **`agg_min`**, **`agg_max`**, **`agg_count`**, or **`agg_n_unique`** from `planframe.expr` (these produce `AggExpr` IR). You cannot pass a bare `col(...)` here; use e.g. `agg_sum(col("x"))` or the tuple form.

Example (assuming `pf` has columns `name`, `id`, `revenue`, and `clicks`):

```python
from planframe.expr import agg_sum, col, lower, truediv

out = (
    pf.group_by(lower(col("name")))
    .agg(
        n=("count", "id"),
        revenue_per_click=agg_sum(truediv(col("revenue"), col("clicks"))),
    )
)
```

Runnable script (from repo root):

```bash
./.venv/bin/python docs/planframe_polars/guides/examples/group_by_usage.py
```

Expected output:

```text
columns=['__pf_g0', 'n', 'rpc']
{'__pf_g0': ['a', 'b'], 'n': [2, 1], 'rpc': [20.0, 20.0]}
```

