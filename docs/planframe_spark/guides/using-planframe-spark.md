# Using `planframe-spark`

The `planframe_spark` package provides a **PySpark-shaped** API frontend (`SparkFrame`, `Column`, and `functions`).

## Imports

```python
from planframe_spark import SparkFrame, functions as F
```

## Combine with a backend

You combine the frontend with a concrete backend frame (e.g. Polars):

```python
from planframe_polars import PolarsFrame


class Users(PolarsFrame, SparkFrame):
    id: int
    age: int


df = Users({"id": [1], "age": [2]})
out = df.withColumn("age_plus_one", F.col("age") + 1).where(F.col("age") > F.lit(0))
rows = out.to_dicts()
```

## Notes

- PlanFrame is **lazy**; `.withColumn(...)` builds a plan.
- Unsupported Spark APIs raise `NotImplementedError` with an actionable message.

