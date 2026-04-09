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
./.venv/bin/python docs/planframe/guides/examples/rows_adapter_minimal.py
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

## Execution boundaries and `ExecutionOptions`

Materialization and row export happen only at execution boundaries. On `BaseAdapter`, these methods take an optional **`options: ExecutionOptions | None`** (`planframe.execution_options`):

| Method | Role |
| --- | --- |
| `collect(df, *, options=...)` | Eager materialization (or no-op for eager backends). |
| `to_dicts(df, *, options=...)` | Row-oriented export. |
| `to_dict(df, *, options=...)` | Column-oriented export. |
| `acollect` / `ato_dicts` / `ato_dict` | Async variants; same `options=` contract (defaults delegate to the sync methods). |

`ExecutionOptions` currently exposes:

- **`streaming`**: user-level streaming hint (meaning is backend-defined).
- **`engine_streaming`**: engine-level streaming hint (distinct from `streaming` where the backend distinguishes them).

**Contract:** adapters should **accept** `options` on these signatures so the public API stays stable. Forward only the hints your engine understands into the backend’s `collect()` / export APIs; ignore the rest. If you do not support any hints yet, it is fine to `options` unused (as many shipped adapters do today), but keep the parameter.

`Frame.collect`, `Frame.to_dicts`, `Frame.to_dict`, and the async counterparts accept the same `ExecutionOptions` and pass them through to the adapter.

### Example: accept and forward hints

```python
from planframe.execution_options import ExecutionOptions

def collect(self, df: MyFrame, *, options: ExecutionOptions | None = None) -> MyFrame:
    kwargs = {}
    if options is not None:
        if options.streaming is not None:
            kwargs["streaming"] = options.streaming
        if options.engine_streaming is not None:
            kwargs["engine_streaming"] = options.engine_streaming
    return df.collect(**kwargs) if kwargs else df.collect()
```

## `join`

`BaseAdapter.join` receives **`left_on`** and **`right_on`** tuples of equal length (for symmetric joins they are identical). For a **`how="cross"`** join from `Frame.join`, both tuples are empty—there are no key columns.

The last argument is optional **`options: JoinOptions | None`** (`planframe.plan.join_options`). **`JoinOptions`** fields are **execution hints** (not relational join semantics). Current fields:

| Field | Purpose (hint) |
| --- | --- |
| `coalesce` | Backend-specific key coalescing. |
| `validate` | Join validation strategy (backend-defined strings). |
| `join_nulls` | Whether nulls compare equal in keys. |
| `maintain_order` | Preserve input order where supported. |
| `streaming` | User-level streaming / execution style hint. |
| `engine_streaming` | Engine-level streaming (pairs with `ExecutionOptions.engine_streaming` conceptually). |
| `allow_parallel` | Allow parallel join execution. |
| `force_parallel` | Prefer forcing parallel execution. |

**Omit-`None` rule:** only pass through kwargs for fields that are **not** `None`, so the engine’s defaults apply. Adapters may ignore hints they do not support.

### Example: join with hints

```python
from planframe.plan.join_options import JoinOptions

# Call site (conceptual): Frame.join(..., options=JoinOptions(...))
def join(self, left, right, *, left_on, right_on, how="inner", suffix="_right", options=None):
    if options is None:
        return backend_join(left, right, left_on=left_on, right_on=right_on, how=how, suffix=suffix)
    kwargs = {}
    if options.coalesce is not None:
        kwargs["coalesce"] = options.coalesce
    if options.engine_streaming is not None:
        kwargs["engine_streaming"] = options.engine_streaming
    # ... other non-None fields ...
    return backend_join(left, right, left_on=left_on, right_on=right_on, how=how, suffix=suffix, **kwargs)
```

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

