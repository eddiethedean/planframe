# Glossary

- **Adapter**: A backend implementation of `BaseAdapter` that executes PlanFrame plans.
- **Plan / PlanNode**: The lazy IR PlanFrame builds when you chain `Frame` operations.
- **Expression / Expr**: The backend-agnostic expression IR compiled by adapters.
- **ExecutionOptions**: Optional hints (`streaming`, `engine_streaming`, …) passed at materialization boundaries (`collect`, `to_dicts`, `to_dict`, and async equivalents).
- **JoinOptions**: Optional execution hints for `Frame.join` (not relational semantics). Fields unset (`None`) should be omitted when calling the backend.
- **ColumnSelector**: Schema-only selection protocol (`planframe.selector`); `@runtime_checkable` so `isinstance` works for built-ins and structural matches.

