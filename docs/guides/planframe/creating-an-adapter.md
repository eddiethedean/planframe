# Creating an adapter (PlanFrame core)

This guide shows how to implement a **PlanFrame adapter** for an existing “dataframe-like” engine by implementing `BaseAdapter`.

## What an adapter does

PlanFrame’s core (`planframe`) is backend-agnostic. It builds a typed plan, then calls an adapter to:

- **compile expressions** (`compile_expr`)
- **execute plan nodes** (`select`, `filter`, `join`, …)
- **materialize outputs** (`collect`, `to_dicts`, `to_dict`, `write_*`)

The adapter API is the abstract base class:

- `packages/planframe/planframe/backend/adapter.py` (`BaseAdapter`)

## A minimal runnable adapter

Below we implement an adapter for a tiny engine that represents a “DataFrame” as `list[dict[str, object]]`.

It’s not fast, but it’s a good template: each adapter method is a pure transformation returning a new “frame”.

### Example script

Run:

```bash
./.venv/bin/python docs/guides/planframe/examples/rows_adapter_minimal.py
```

Expected output:

```text
schema=('id', 'age')
collect=[{'id': 1, 'age': 10}, {'id': 2, 'age': 20}]
dicts=[{'id': 1, 'age': 10}, {'id': 2, 'age': 20}]
dict={'id': [1, 2], 'age': [10, 20]}
```

## Checklist for a real engine adapter

- **Frame type**: pick the backend’s “frame” type (e.g. a lazy plan type if it has one)
- **Expression type**: pick the backend’s expression type (or use a small wrapper object)
- **Always-lazy**: return lazy objects from transforms; only execute inside `collect`/`to_dicts`/writes
- **I/O**: implement `write_*` (or raise a clear error if not supported)

## `join`

`BaseAdapter.join` receives **`left_on`** and **`right_on`** tuples of equal length (for symmetric joins they are identical). For a **`how="cross"`** join from `Frame.join`, both tuples are empty—there are no key columns.

Optional **`JoinOptions`** (`planframe.plan.join_options`) carries backend-specific hints (`coalesce`, `validate`, `join_nulls`, `maintain_order`, `streaming`). Adapters may ignore any field they do not support; omit kwargs when the option is `None` so the engine keeps its defaults.

## `group_by_agg`

`Frame.group_by(...).agg(...)` lowers to a **`GroupBy`** node followed by **`Agg`**. Evaluation calls **`BaseAdapter.group_by_agg`** on the frame *before* grouping (the `GroupBy` predecessor), not on a backend-specific “grouped” handle.

### Arguments

- **`keys`**: `tuple[CompiledJoinKey[BackendExprT], ...]` — same structural type as join / sort keys (`CompiledJoinKey` is an alias of `CompiledSortKey`). Each element is either a column name (`column=` set, `expr=None`) or a compiled backend expression (`column=None`, `expr=` set). PlanFrame compiles `JoinKeyExpr` IR into the latter. Synthetic key column names in the result schema are `__pf_g0`, `__pf_g1`, … matching the index in this tuple.

- **`named_aggs`**: `dict[str, tuple[str, str] | BackendExprT]` mapping output column names to either:
  - **Tuple form**: `(op, column_name)` with `op` one of `count`, `sum`, `mean`, `min`, `max`, `n_unique` (legacy reductions over a single input column).
  - **Compiled expression form**: a backend-native expression that is already a **per-group aggregation** suitable for the engine’s `group_by(...).agg(...)`. PlanFrame produces this by compiling **`AggExpr`** IR (`agg_sum(inner)`, `agg_mean(inner)`, …) via `compile_expr`.

Preserve the iteration order of **`named_aggs`** when building the backend aggregation list (Python dict insertion order).

### Minimal behavior

If you do not implement grouping yet, keep raising `NotImplementedError` from `group_by_agg` with a clear message, as in `examples/rows_adapter_minimal.py`.

## Notes

- PlanFrame validates many schema invariants *before* calling the backend. Your adapter can assume the plan is well-formed, but it should still validate backend-specific constraints (e.g. “pivot requires on_columns when lazy”).

