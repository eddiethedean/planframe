# Glossary

- **Adapter**: A backend implementation of `BaseAdapter` that executes PlanFrame plans.
- **Plan / PlanNode**: The lazy IR PlanFrame builds when you chain `Frame` operations.
- **Expression / Expr**: The backend-agnostic expression IR compiled by adapters.
- **ExecutionOptions**: Optional hints (`streaming`, `engine_streaming`, …) passed at materialization boundaries (`collect`, `to_dicts`, `to_dict`, and async equivalents).
- **JoinOptions**: Optional execution hints for `Frame.join` (not relational semantics). Fields unset (`None`) should be omitted when calling the backend.
- **ColumnSelector**: Schema-only selection protocol (`planframe.selector`); `@runtime_checkable` so `isinstance` works for built-ins and structural matches.
- **PlanCompileContext**: Internal helper (`planframe.compile_context`) that holds `(adapter, schema)` and compiles expression IR, join keys, aggregations, and related structures—shared by `Frame` and `execute_plan` so compilation stays consistent.
- **execute_plan**: The supported interpreter that evaluates a `PlanNode` tree by dispatching to `BaseAdapter` methods; `Frame` uses it at materialization boundaries.
- **API skin**: An optional mixin on `Frame` (e.g. `planframe.spark.SparkFrame`, `planframe.pandas.PandasLikeFrame`) that adds familiar method names while preserving PlanFrame’s lazy plan and typing story.
- **Hint (plan node)**: A `PlanNode` carrying opaque execution hints (`SparkFrame.hint(...)`). `execute_plan` calls `BaseAdapter.hint` (default no-op) so backends can optionally honor hint strings or metadata.

