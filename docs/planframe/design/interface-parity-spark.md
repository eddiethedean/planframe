# Interface parity: PlanFrame spark skin vs PySpark `DataFrame`

This page compares PlanFrame’s **PySpark-like interface skin** (`planframe.spark.SparkFrame`) to `pyspark.sql.DataFrame`.

PlanFrame is **always lazy** and executes through adapters; there is no Spark execution engine in core.

## Legend

- **Exact parity**: same name + same argument shape + same behavior (modulo “always lazy”).
- **Typed-parity**: same shape, but restricted to typed-safe subsets.
- **Divergence**: present but intentionally differs (documented).
- **Unsupported**: not implemented; should fail loudly.

## High-value mapping

| PySpark | PlanFrame spark skin | Status | Notes |
| --- | --- | --- | --- |
| `df["x"]` | `__getitem__` | Typed-parity | Returns a typed `Column` wrapper over PlanFrame `Expr`. |
| `df.x` | `__getattr__` | Typed-parity | Only when `x` is a schema column. |
| `select(...)` | `select(...)` | Typed-parity | Accepts `str`, Spark `Column`, or `Expr` (with restrictions). |
| `where(...)` | `where(...)` | Typed-parity | Predicate must be `Column[bool]` or `Expr[bool]`. |
| `filter(...)` | `filter(...)` | Divergence | Delegates to `where` (typed). |
| `withColumn(name, col)` | `withColumn(...)` | Typed-parity | Lowered to core `with_columns(exprs={...})`. |
| `withColumns(map)` | `withColumns(...)` | Typed-parity | Lowered to repeated `with_columns`. |
| `withColumnRenamed(a,b)` | `withColumnRenamed(...)` | Typed-parity | Lowered to core `rename`. |
| `orderBy(...)` | `orderBy(...)` | Typed-parity | Lowered to core `sort`; supports per-key ascending. |
| `distinct()` | `distinct()` | Typed-parity | Lowered to `unique()`. |
| `dropDuplicates(subset=...)` | `dropDuplicates(...)` | Typed-parity | Lowered to `drop_duplicates`. |
| `groupBy(...).agg(...)` | `groupBy(...).agg(...)` | Typed-parity | Typed subset of aggregations (`functions.sum/mean/min/max/count/n_unique`). |
| `union` / `unionAll` | `union` / `unionAll` | Typed-parity | Lowered to `vstack` (UNION ALL semantics). |
| `unionByName(...)` | `unionByName(...)` | Divergence | `allowMissingColumns=True` is unsupported. |
| `join(...)` | `join(...)` | Typed-parity | Normalizes `on/left_on/right_on`; supports Spark-ish `how` aliases. |
| `hint(...)` | `hint(...)` | Divergence | Plan-level hint node; adapters may ignore. |

## Unsupported (expected)

- Partition and caching semantics: `repartition`, `coalesce`, `cache`, `persist`, `unpersist`
- SQL-string API: `selectExpr`
- Partition-level sorting: `sortWithinPartitions`
- Set ops not implemented in core: `intersect`, `subtract`

