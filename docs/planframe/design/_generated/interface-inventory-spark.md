# Generated interface inventory (Spark parity)

This file is generated from source. It inventories the spark-like skin surface.

## `SparkFrame` vs `pyspark.sql.DataFrame`

| Method | Our signature | Parent | Parent signature | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| `cache` | `def cache(self)` | `pyspark.sql.DataFrame` | — | **unsupported** | Spark engine/partition semantics not part of PlanFrame core |
| `coalesce` | `def coalesce(self, _numPartitions)` | `pyspark.sql.DataFrame` | — | **unsupported** | Spark engine/partition semantics not part of PlanFrame core |
| `columns` | `def columns(self)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `count` | `def count(self)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `crossJoin` | `def crossJoin(self, other)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `distinct` | `def distinct(self)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `drop` | `def drop(self, *cols, strict)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `dropDuplicates` | `def dropDuplicates(self, subset)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `dropna` | `def dropna(self, how, thresh, subset)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `fillna` | `def fillna(self, value, subset)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `filter` | `def filter(self, *predicates)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `groupBy` | `def groupBy(self, *cols)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `hint` | `def hint(self, *hints, **kv)` | `pyspark.sql.DataFrame` | — | **divergence** | Plan-level hint / restricted unionByName shape |
| `intersect` | `def intersect(self, other)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `join` | `def join(self, other, on, how, left_on, right_on, suffix, options)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `limit` | `def limit(self, n)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `na` | `def na(self)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `orderBy` | `def orderBy(self, *cols, ascending)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `persist` | `def persist(self, *args, **kwargs)` | `pyspark.sql.DataFrame` | — | **unsupported** | Spark engine/partition semantics not part of PlanFrame core |
| `repartition` | `def repartition(self, *args, **kwargs)` | `pyspark.sql.DataFrame` | — | **unsupported** | Spark engine/partition semantics not part of PlanFrame core |
| `sample` | `def sample(self, n, frac, with_replacement, shuffle, seed, **kwargs)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `select` | `def select(self, *columns, **named_exprs)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `selectExpr` | `def selectExpr(self, *expr)` | `pyspark.sql.DataFrame` | — | **unsupported** | Spark engine/partition semantics not part of PlanFrame core |
| `show` | `def show(self, n, truncate, vertical)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `sortWithinPartitions` | `def sortWithinPartitions(self, *cols, ascending)` | `pyspark.sql.DataFrame` | — | **unsupported** | Spark engine/partition semantics not part of PlanFrame core |
| `subtract` | `def subtract(self, other)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `take` | `def take(self, num)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `toDF` | `def toDF(self, *names)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `union` | `def union(self, other)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `unionAll` | `def unionAll(self, other)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `unionByName` | `def unionByName(self, other, allowMissingColumns)` | `pyspark.sql.DataFrame` | — | **divergence** | Plan-level hint / restricted unionByName shape |
| `unpersist` | `def unpersist(self, *args, **kwargs)` | `pyspark.sql.DataFrame` | — | **unsupported** | Spark engine/partition semantics not part of PlanFrame core |
| `where` | `def where(self, condition)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `withColumn` | `def withColumn(self, colName, col)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `withColumnRenamed` | `def withColumnRenamed(self, existing, new)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
| `withColumns` | `def withColumns(self, colsMap)` | `pyspark.sql.DataFrame` | — | **typed-parity** | — |
