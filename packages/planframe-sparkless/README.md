## planframe-sparkless

[![Docs](https://readthedocs.org/projects/planframe/badge/?version=latest)](https://planframe.readthedocs.io/en/latest/planframe_sparkless/)
[![License: MIT](https://img.shields.io/badge/License-MIT-informational)](../../LICENSE)

Sparkless adapter package for PlanFrame. Import as `planframe_sparkless`.

This package:

- uses the **PySpark-like UI** from `planframe.spark` (`SparkFrame`)
- executes plans using the **`sparkless`** engine (no JVM)

Documentation (ReadTheDocs):

- Sparkless track (end users): `https://planframe.readthedocs.io/en/latest/planframe_sparkless/`

### Install

```bash
pip install planframe-sparkless
```

### Quickstart

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

### Execution model (PlanFrame)

- PlanFrame is **always lazy**: chaining does not execute backend work.
- Materialization boundaries:
  - `collect()` returns `list[pydantic.BaseModel]`
  - `collect_backend()` returns the sparkless backend dataframe object
  - `to_dicts()` / `to_dict()` export rows/columns
  - Async: `acollect()` / `ato_dicts()` / `ato_dict()` (aliases: `collect_async`, `to_dicts_async`, `to_dict_async`)
- Core **v1.2+**: `execute_plan_async`, `planframe.materialize`, Expr `==` / `&` / … as IR—see [Migrating since v1.1.0](https://planframe.readthedocs.io/en/latest/planframe/guides/migrating-since-1-1/).

### Notes / limitations

- This adapter aims to support a practical subset of Spark-like operations using `sparkless`.
- Row streaming: `stream_dicts()` currently materializes via `to_dicts()` (sparkless does not expose an efficient local iterator API yet).
- For backend-agnostic semantics and supported transforms, see the core docs: `https://planframe.readthedocs.io/en/latest/planframe/`

