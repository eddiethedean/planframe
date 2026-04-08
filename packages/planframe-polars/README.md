## planframe-polars

[![Docs](https://readthedocs.org/projects/planframe/badge/?version=latest)](https://planframe.readthedocs.io/en/latest/planframe_polars/)
[![PyPI](https://img.shields.io/pypi/v/planframe-polars)](https://pypi.org/project/planframe-polars/)
[![License: MIT](https://img.shields.io/badge/License-MIT-informational)](../../LICENSE)

Polars adapter package for PlanFrame. Import as `planframe_polars`.

Documentation (ReadTheDocs):

- Polars track (end users): `https://planframe.readthedocs.io/en/latest/planframe_polars/`
- Light API reference: `https://planframe.readthedocs.io/en/latest/planframe_polars/reference/api/`

### Usage

```python
import polars as pl

from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    age: int

pf = User(pl.DataFrame({"id": [1], "age": [2]}))
df = pf.select("id").collect()

# Or construct from python data:
pf2 = User({"id": [1], "age": [2]})
```

### Execution model

PlanFrame is always lazy:
- Chaining methods (like `.select(...)`) does **not** run Polars operations.
- `collect()` evaluates the full plan. If the source is a `polars.LazyFrame`, this naturally compiles into a single lazy query before collecting.

### Notes (Polars-specific)

- **Pivot**: `LazyFrame.pivot(...)` requires `on_columns` to be provided up-front (Polars must know the output schema prior to `collect()`). PlanFrame enforces this at execution time.
- **concat_vertical**: implemented via `polars.concat(..., how="vertical")`.
- **Join**: implemented via `LazyFrame.join(...)` / `DataFrame.join(...)` with symmetric `on` or asymmetric `left_on` / `right_on`, plus optional `JoinOptions` mapped to Polars (`nulls_equal`, `validate`, `coalesce`, `maintain_order`, `allow_parallel` / streaming).
- **Group by / agg**: `group_by` compiles to Polars `group_by` with column or expression keys (expression keys are aliased `__pf_g{i}`). `agg` compiles tuple reductions to `pl.col(...).sum()`-style calls and `AggExpr` to aggregated expressions on compiled inners (e.g. `agg_sum(truediv(col("a"), col("b")))`).

