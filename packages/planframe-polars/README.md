## planframe-polars

[![Docs](https://readthedocs.org/projects/planframe/badge/?version=latest)](https://planframe.readthedocs.io/en/latest/planframe_polars/)
[![PyPI](https://img.shields.io/pypi/v/planframe-polars)](https://pypi.org/project/planframe-polars/)
[![License: MIT](https://img.shields.io/badge/License-MIT-informational)](../../LICENSE)

Polars adapter package for PlanFrame. Import as `planframe_polars`.

Documentation (ReadTheDocs):

- Polars track (end users): `https://planframe.readthedocs.io/en/latest/planframe_polars/`
- Light API reference: `https://planframe.readthedocs.io/en/latest/planframe_polars/reference/api/`

### Install

```bash
pip install planframe-polars
```

### Usage

```python
from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    age: int

# Construct from python data:
pf = User({"id": [1], "age": [2]})
df = pf.select("id").collect()

# Common transforms (PlanFrame is always lazy; these build a plan until `collect()`).
pf3 = pf.with_row_index(name="row_nr").clip(lower=0, subset=("age",))
pf4 = pf.rename_upper().cast_many({"age": float})

# If you already have a Polars DataFrame/LazyFrame, use `Frame.source(...)`:
import polars as pl

pf2 = User.source(pl.DataFrame({"id": [1], "age": [2]}).lazy(), adapter=User._adapter_singleton, schema=User)
```

### Execution model

Core **v1.2.0+** (current minor **v1.3.x**) includes `execute_plan_async`, `planframe.materialize`, discoverable Frame async aliases (`collect_async`, `to_dict_async`, …), and **v1.3.0** adapter/typing additions—see the [migration guide](https://planframe.readthedocs.io/en/latest/planframe/guides/migrating-since-1-1/) and [API reference](https://planframe.readthedocs.io/en/latest/planframe/reference/api/).

PlanFrame is always lazy:
- Chaining methods (like `.select(...)`) does **not** run Polars operations.
- `collect()` evaluates the full plan (and returns `list[pydantic.BaseModel]`).
- If you need a backend-native `polars.DataFrame` / `polars.LazyFrame`, use `collect_backend()`.
- If you want to iterate rows, use `stream_dicts()` / `stream()` (see the [Streaming rows](https://planframe.readthedocs.io/en/latest/planframe/guides/streaming-rows/) guide).

### Optional API skins (core)

The core package includes typed **mixins** you can combine with `PolarsFrame` if you want a different surface API (same lazy plan underneath):

- **`planframe.spark.SparkFrame`**: PySpark-like column access, `withColumns`, `groupBy().agg(...)`, `hint()`, … — see [PySpark-like API](https://planframe.readthedocs.io/en/latest/planframe/guides/pyspark-like-api/).
- **`planframe.pandas.PandasLikeFrame`**: pandas-like naming — see [pandas-like API](https://planframe.readthedocs.io/en/latest/planframe/guides/pandas-like-api/) (the pandas adapter’s `PandasFrame` uses this mixin by default).

### Notes (Polars-specific)

- **Pivot**: `LazyFrame.pivot(...)` requires `on_columns` to be provided up-front (Polars must know the output schema prior to `collect()`). PlanFrame enforces this at execution time.
- **pivot_wider**: wrapper around `pivot(...)`; for deterministic output columns on lazy sources, pass `on_columns`.
- **vstack**: implemented via `polars.concat(..., how="vertical")`.
- **Join**: implemented via `LazyFrame.join(...)` / `DataFrame.join(...)` with symmetric `on` or asymmetric `left_on` / `right_on`, plus optional `JoinOptions` mapped to Polars (`nulls_equal`, `validate`, `coalesce`, `maintain_order`, `allow_parallel` / `force_parallel`, `streaming`, `engine_streaming` when supported by the installed Polars).
- **Group by / agg**: `group_by` compiles to Polars `group_by` with column or expression keys (expression keys are aliased `__pf_g{i}`). `agg` compiles tuple reductions to `pl.col(...).sum()`-style calls and `AggExpr` to aggregated expressions on compiled inners (e.g. `agg_sum(truediv(col("a"), col("b")))`).

