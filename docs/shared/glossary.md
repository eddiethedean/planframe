# Glossary

- **Adapter**: A backend implementation of `BaseAdapter` that executes PlanFrame plans.
- **Plan / PlanNode**: The lazy IR PlanFrame builds when you chain `Frame` operations.
- **Expression / Expr**: The backend-agnostic expression IR compiled by adapters.
- **ExecutionOptions**: Optional hints (`streaming`, `engine_streaming`, …) passed at materialization boundaries (`collect`, `to_dicts`, `to_dict`, and async equivalents).
- **JoinOptions**: Optional execution hints for `Frame.join` (not relational semantics). Fields unset (`None`) should be omitted when calling the backend.
- **ColumnSelector**: Schema-only selection protocol (`planframe.selector`); `@runtime_checkable` so `isinstance` works for built-ins and structural matches.
- **PlanCompileContext**: Internal helper (`planframe.compile_context`) that holds `(adapter, schema)` and compiles expression IR, join keys, aggregations, and related structures—shared by `Frame` and `execute_plan` so compilation stays consistent.
- **execute_plan**: The supported interpreter that evaluates a `PlanNode` tree by dispatching to `BaseAdapter` methods; `Frame` uses it at materialization boundaries.
- **execute_plan_async**: Async wrapper around `execute_plan` that runs the same synchronous interpreter in a worker thread (`asyncio.to_thread`) so callers can `await` without blocking the event loop; lazy plan *building* remains sync. Async engine I/O still uses `BaseAdapter` async materializers (`acollect`, `ato_dict`, …). See [Migrating since v1.1.0](../planframe/guides/migrating-since-1-1.md) and [Creating an adapter — Async execution](../planframe/guides/creating-an-adapter.md#async-execution-contract-third-party-adapters).
- **CompileExprContext**: Holds the active `Schema` for an expression compile step; passed to `BaseAdapter.compile_expr` and optional `BaseAdapter.resolve_dtype` (re-exported from `planframe`). Different from **PlanCompileContext** (`planframe.compile_context`), which pairs an adapter with a schema for plan-level compilation helpers used by `Frame` and `execute_plan`.
- **materialize_columns / materialize_into** (`planframe.materialize`): Thin, adapter-agnostic helpers for `Frame → dict[str, list[object]]` and an optional post-processing callable; forward `ExecutionOptions` like `Frame.to_dict`. See [Creating an adapter — Columnar boundary helpers](../planframe/guides/creating-an-adapter.md#columnar-boundary-helpers-planframematerialize).
- **API skin**: An optional mixin on `Frame` (e.g. `planframe.spark.SparkFrame`, `planframe.pandas.PandasLikeFrame`) that adds familiar method names while preserving PlanFrame’s lazy plan and typing story.
- **Hint (plan node)**: A `PlanNode` carrying opaque execution hints (`SparkFrame.hint(...)`). `execute_plan` calls `BaseAdapter.hint` (default no-op) so backends can optionally honor hint strings or metadata.

