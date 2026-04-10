# FAQ

## Is PlanFrame a dataframe library?

No. PlanFrame is a typed planning layer that delegates execution to a backend via an adapter.

## Does chaining execute backend work?

No. PlanFrame is always lazy; execution happens at explicit boundaries like `collect()`.

## Is there an async API?

Yes. `Frame` exposes `acollect`, `ato_dicts`, and `ato_dict`, which await the adapter’s async hooks (`BaseAdapter.acollect`, etc.). Discoverable aliases **`collect_async`**, **`to_dicts_async`**, and **`to_dict_async`** are the same operations. For awaiting **plan evaluation** without blocking the event loop, core also provides **`execute_plan_async`** (it runs the sync `execute_plan` in `asyncio.to_thread`). Defaults run synchronous adapter methods in a worker thread so existing backends keep working without code changes. See [Migrating since v1.1.0](../planframe/guides/migrating-since-1-1.md).

## Can I detect PlanFrame column selectors at runtime?

Yes. `planframe.selector.ColumnSelector` is a `@runtime_checkable` protocol, so `isinstance(obj, ColumnSelector)` works for built-in selector types and for structural matches (objects with a compatible `select(self, schema)` method).

## Is there a PySpark-like or pandas-like API?

Yes. The core package ships optional, typed **skins**—`planframe.spark` (PySpark-style `SparkFrame`, `Column`, `functions`) and `planframe.pandas` (pandas-style `PandasLikeFrame`, `Series`). They are mixins on `Frame` and do not add Spark or extra runtime deps. The **planframe-pandas** adapter’s `PandasFrame` uses `PandasLikeFrame` by default. See the [PySpark-like API](../planframe/guides/pyspark-like-api.md) and [pandas-like API](../planframe/guides/pandas-like-api.md) guides.

