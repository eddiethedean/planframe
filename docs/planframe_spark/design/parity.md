# planframe-spark parity matrix (PySpark `pyspark.sql.DataFrame`)

This page tracks how closely the **PlanFrame Spark frontend** aims to match the parent PySpark interface (`pyspark.sql.DataFrame`).

This frontend is **API-parity oriented**, but PlanFrame remains:

- **lazy-first** (plans execute at `collect()`),
- **adapter-driven** (no Spark runtime unless an adapter exists),
- **typed** (high-value typed subset; unsupported surface fails loudly).

## Legend

- **Exact parity**: same name + same argument shape + same behavior (modulo “always lazy”).
- **Typed-parity**: same name/args, but restricted to a typed-safe subset.
- **Divergence**: exists but intentionally differs (documented).
- **Not supported**: explicitly unsupported; should raise a clear error.

## High-value surface (current focus)

| PySpark `DataFrame` | PlanFrame Spark frontend | Status | Notes |
| --- | --- | --- | --- |
| `select(...)` | `SparkFrame.select(...)` | Typed-parity | Typed subset (column names + typed `Column`). |
| `where(...)` / `filter(...)` | `SparkFrame.where(...)` / `filter(...)` | Typed-parity | Predicate is typed `Column[bool]` / `Expr[bool]`. |
| `withColumn(name, col)` | `SparkFrame.withColumn(...)` | Typed-parity | Lowered to core `with_columns`. |
| `withColumns(map)` | `SparkFrame.withColumns(...)` | Typed-parity | Lowered to repeated `with_columns`. |
| `drop(...)` | `SparkFrame.drop(...)` | Typed-parity | Column drop only (no Spark partition semantics). |
| `join(...)` | `SparkFrame.join(...)` | Typed-parity | Lowered to core `join`. |
| `union(...)` | `SparkFrame.union(...)` | Typed-parity | Lowered to `vstack`. |
| `unionByName(...)` | `SparkFrame.unionByName(...)` | Typed-parity | Lowered to `vstack` with schema alignment rules. |
| `groupBy(...).agg(...)` | `GroupedData.agg(...)` | Typed-parity | Accepts typed Spark-function aggregations subset. |
| `hint(...)` | `SparkFrame.hint(...)` | Divergence | Plan-level hint node; adapters may ignore. |

## Not supported (expected)

- Partition and storage semantics (`repartition`, `coalesce`, `sortWithinPartitions`, …)
- Caching/persistence (`cache`, `persist`, `unpersist`)
- SQL-string surfaces (`selectExpr`, `expr("...")`, …)
- Execution-graph introspection and Spark session integration

## Where this is implemented

- Current in-core skin: `packages/planframe/planframe/spark/*`
- Planned dedicated package: `packages/planframe-spark/planframe_spark/*` (frontend contract)
