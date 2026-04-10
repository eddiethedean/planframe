# Using `planframe-sparkless`

This track provides a **real execution backend** for the **Spark UI** (`planframe.spark`) by running plans on the [`sparkless`](https://pypi.org/project/sparkless/) engine (no JVM).

For **core** API changes (async materialization, `planframe.materialize`, Expr operators, …), see [Migrating since v1.1.0](../planframe/guides/migrating-since-1-1.md).

## Quickstart

```python
from planframe.expr import add, col, lit
from planframe_sparkless import SparklessFrame


class User(SparklessFrame):
    id: int
    x: int


pf = User([{"id": 1, "x": 2}, {"id": 2, "x": 3}])

out = (
    pf.select("id", "x")
    .withColumn("x2", add(col("x"), lit(1)))
    .where(pf["x"] > lit(2))
    .select("id", "x2")
)

print(out.to_dicts())
```

## Notes

- The **UI** is PlanFrame’s `SparkFrame` mixin (PySpark-like naming).
- The **engine** is `sparkless` (a PySpark-compatible Python DataFrame library).
- PlanFrame’s materialization contract still applies:
  - `collect()` returns `list[pydantic.BaseModel]`
  - `collect_backend()` returns the sparkless backend DataFrame object

