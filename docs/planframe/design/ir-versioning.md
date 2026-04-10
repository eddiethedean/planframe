# Plan / expression IR versioning (adapter compatibility)

PlanFrame adapters implement:

- **expression compilation** (`BaseAdapter.compile_expr`) over PlanFrame’s expression IR (`planframe.expr`)
- **plan execution** over PlanFrame’s plan nodes (`planframe.plan.nodes`)

As PlanFrame evolves, third-party adapters need a predictable way to detect when a PlanFrame upgrade may introduce **new IR shapes**.

## Coarse-grained IR versions

PlanFrame exposes two monotonic integer markers:

- `planframe.__plan_ir_version__`
- `planframe.__expr_ir_version__`

These are intended as a **compatibility guard**, not a full feature matrix.

### When versions bump

The versions bump when an adapter (or external tooling that pattern-matches IR) might reasonably need an update, e.g.:

- a new `PlanNode` or `Expr` node is added
- an existing node’s fields or semantics change
- an execution-relevant invariant changes (e.g. a node is reinterpreted)

## How adapters should use this

### Recommended: fail fast with a clear message

If your adapter is pinned to a known set of supported nodes, you can check versions during adapter initialization:

```python
import planframe

SUPPORTED_PLAN = 1
SUPPORTED_EXPR = 1

if planframe.__plan_ir_version__ != SUPPORTED_PLAN:
    raise RuntimeError(
        f\"Unsupported PlanFrame plan IR version: {planframe.__plan_ir_version__} (expected {SUPPORTED_PLAN})\"
    )

if planframe.__expr_ir_version__ != SUPPORTED_EXPR:
    raise RuntimeError(
        f\"Unsupported PlanFrame expr IR version: {planframe.__expr_ir_version__} (expected {SUPPORTED_EXPR})\"
    )
```

If you prefer a looser stance, you can accept a range and rely on runtime `NotImplementedError` for specific nodes. The key is to be explicit and user-friendly.

### Also required: handle unknown nodes

Even with version checks, adapters should still raise clear errors for unsupported shapes:

- unknown `Expr` in `compile_expr` → `NotImplementedError` / adapter-specific error
- unsupported `PlanNode` semantics → `NotImplementedError` during execution

## Plan `Source.ir_version`

`Source` carries an `ir_version` field. PlanFrame sets it to the current `PLAN_IR_VERSION` when constructing a new `Frame`.

This is useful for:

- **debugging** and “what created this plan?” provenance
- external tooling that serializes or snapshots plan trees

It is not a substitute for `planframe.__plan_ir_version__` (which is the stable marker adapters should check).

## Adapter release checklist (when PlanFrame adds a new node)

- implement the new IR node handling (compile and/or execute)
- add conformance tests
- bump your adapter’s version and document supported PlanFrame versions
- consider validating `__plan_ir_version__` / `__expr_ir_version__` at import/init time to fail fast

