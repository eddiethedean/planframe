# PySpark-like API (`planframe.spark`)

PlanFrame includes an optional **PySpark-style surface** (no Apache Spark dependency) under the `planframe.spark` submodule.

## Imports

```python
from planframe.spark import SparkFrame, functions as F
from planframe_polars import PolarsFrame


class User(PolarsFrame, SparkFrame):
    id: int
    name: str


df = User({"id": [1], "name": ["a"]})
out = df.withColumn("id2", F.col("id")).where(F.col("id") > F.lit(0))
```

## Extras

- **Column sugar**: `df["col"]` and (when it doesn’t conflict with real attributes) `df.col`
- **Plural**: `withColumns({...})`
- **Typed aggregations**: `groupBy(...).agg(total=F.sum("x"))` (accepts only Spark `functions` aggregations)
- **Hints**: `df.hint("broadcast", table="...")` is a plan-level hint node; backends may ignore it.

Or load the submodule lazily:

```python
import planframe

SparkFrame = planframe.spark.SparkFrame
```

## Limits

- Semantics remain PlanFrame (lazy plans, adapters, `collect()`).
- Unsupported PySpark calls raise `NotImplementedError` with a short note (e.g. `selectExpr`, Spark partitions).

See the core package design docs for how `Frame` and adapters fit together.
