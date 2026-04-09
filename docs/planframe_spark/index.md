# planframe-spark

`planframe-spark` provides a **PySpark-like API frontend** for PlanFrame, without a Spark dependency.

## What it is

- A typed `SparkFrame` mixin with Spark-shaped method names (`select`, `withColumn`, `where`, `groupBy`, ...).
- A typed `Column` wrapper and a small `functions` subset (`col`, `lit`, `sum`, ...).

## What it is not

- It does **not** execute on Spark unless you provide a Spark adapter.
- It does **not** aim to reproduce all Spark semantics (partitioning, caching, SQL-string APIs).

## Quickstart

```python
from planframe_spark import SparkFrame, functions as F
from planframe_polars import PolarsFrame


class User(PolarsFrame, SparkFrame):
    id: int
    name: str


df = User({"id": [1], "name": ["a"]})
out = df.withColumn("id2", F.col("id") + 1).where(F.col("id") > F.lit(0))
rows = out.to_dicts()
```

