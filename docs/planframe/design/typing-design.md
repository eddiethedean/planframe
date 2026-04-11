# PlanFrame — Resolve Typing Design for Pyright

## Purpose

This document defines a practical typing strategy for `Resolve`, the core mechanism that lets PlanFrame infer column types across schema-changing transformations without requiring users to define intermediate schema models.

The goal is not to make Python's type system do arbitrary computation. The goal is to design a type system that Pyright can follow reliably across a constrained, explicit API.

**Adapter authors:** runtime dtype and execution behavior live in `BaseAdapter` (`compile_expr`, `resolve_dtype`, materialization). For a per-minor-release integration checklist (sync/async, `execute_plan`, `planframe.materialize`), see [Third-party adapter integration checklist](../guides/creating-an-adapter.md#third-party-adapter-checklist).

---

## 1. Problem Statement

Given a typed frame:

```python
Frame[PlanT, BackendT]
```

we need to answer questions like:

- What is the type of column `"id"` after `select("id", "name")`?
- What is the type of `"age_plus_one"` after `with_column("age_plus_one", add(col("age"), lit(1)))`?
- What happens after `rename(name="full_name")`?
- What are the result column names and dtypes after `group_by` with expression keys (`__pf_g0`, …) and `agg` with both `(op, column)` tuples and `AggExpr` values?
- How do we preserve static typing without forcing the user to define every intermediate schema?

This is the job of `Resolve`.

---

## 2. Design Principle

PlanFrame should not try to eagerly materialize full schemas at type-check time.

Instead, it should support:

- plan-level typing
- column-level type resolution
- optional schema materialization at explicit boundaries

This means the central question is:

> Given a plan `P` and column name `K`, what is the static type of that column?

---

## 3. Recommended Typing Strategy

There are two layers:

### Layer A — Public Type Layer
This is what users interact with.

- `Frame[PlanT, BackendT]`
- `Expr[T]`
- typed methods like `.select(...)`, `.with_column(...)`, `.rename(...)`

### Layer B — Internal Resolution Layer
This is where type propagation is modeled.

Use:
- `Literal[...]`
- overloads
- Protocols
- phantom generic plan node types
- generated stubs for high-fidelity column access where needed

---

## 4. Core Type Definitions

```python
from __future__ import annotations

from typing import Any, Generic, Protocol, TypeVar
from typing_extensions import Literal, TypeAlias

PlanT = TypeVar("PlanT")
BackendT = TypeVar("BackendT")
T = TypeVar("T")
NameT = TypeVar("NameT", bound=str)
```

---

## 5. Phantom Plan Nodes

These nodes do not need to contain meaningful runtime state for typing to work.

```python
SchemaT = TypeVar("SchemaT")
PrevPlanT = TypeVar("PrevPlanT")
ExprT = TypeVar("ExprT")

class Source(Generic[SchemaT]):
    ...

class Select(Generic[PrevPlanT]):
    ...

class Drop(Generic[PrevPlanT]):
    ...

class Rename(Generic[PrevPlanT]):
    ...

class WithColumn(Generic[PrevPlanT, ExprT]):
    ...

class Cast(Generic[PrevPlanT, ExprT]):
    ...

class Filter(Generic[PrevPlanT]):
    ...
```

### Important note

Python's type system cannot directly express arbitrary mappings like:

- selected key sets
- rename maps
- schema key/value transforms

So for Pyright-friendly design, there are only three realistic options:

1. **Small static overload sets**
2. **Generated `.pyi` stubs**
3. **An external resolver tool (Tier 3)**

For MVP, use 1 and 2. Plugin can come later.

---

## 6. MVP Resolve Model

For the MVP, `Resolve` should be understood as a design abstraction, not a single executable type operator.

Represent it operationally using:

- method overloads
- typed column accessors
- generated access proxy types after each operation
- explicit materialization when necessary

In other words:

> `Resolve` is a logical spec, implemented via API typing patterns.

---

## 7. Concrete Rules for Resolve

### Rule 1 — Source

If a frame starts from a concrete schema `S`, then every source column resolves to the declared field type in `S`.

Conceptually:

```python
Resolve[Source[S], "id"] -> int
```

Implementation options:
- dataclass/TypedDict/Pydantic schema introspection at runtime
- generated `ColumnAccessor` stubs for static typing
- schema metadata IR for runtime checks

---

### Rule 2 — Select

A selected frame only exposes selected columns.

Conceptually:

```python
Resolve[Select[P, ("id", "name")], "id"] -> Resolve[P, "id"]
Resolve[Select[P, ("id", "name")], "age"] -> error
```

Pyright strategy:
- support `.select(...)` with overloads for 1–10 literal columns
- return a new frame type bound to a new accessor proxy
- optionally generate a materialized schema proxy type

---

### Rule 3 — WithColumn

Adding a column introduces a new name with the expression output type.

Conceptually:

```python
Resolve[WithColumn[P, "age_plus_one", int], "age_plus_one"] -> int
Resolve[WithColumn[P, "id_plus_one", int], "id"] -> Resolve[P, "id"]
```

---

### Rule 4 — Rename

Renaming moves the type from the old name to the new name.

Conceptually:

```python
Resolve[Rename[P, {"name": "full_name"}], "full_name"] -> Resolve[P, "name"]
Resolve[Rename[P, {"name": "full_name"}], "name"] -> error
```

Pyright strategy:
- only support keyword rename syntax in the typed API

---

### Rule 5 — Drop

Dropped columns disappear from the visible schema.

---

### Rule 6 — Cast

Casting keeps the name but changes the type.

---

### Rule 7 — Filter

Filtering does not change schema.

---

## 7.1 Expr operator overloads (typing semantics)

PlanFrame builds expression IR from `Expr` operator overloads (`>`, `==`, `&`, `|`, `~`, …). Typing tools treat these as returning **`Expr[bool]`** (or the appropriate comparison node type) so idiomatic code type-checks:

- **Comparisons** (`<`, `<=`, `>`, `>=`, `==`, `!=`): the right-hand side may be another `Expr` or a literal coerced via `lit` (`int`, `float`, `str`, `bool`, `None`, …).
- **Boolean combinators** (`&`, `|`, `~`): operands are **`Expr`** values interpreted as boolean expressions at execution time; `&` / `|` also accept Python `bool` on the left or right (coerced to `lit(...)`), matching lazy Spark/Polars-style patterns.

IR node dataclasses use `eq=False` so **operator** `==` / `!=` stay on `Expr` and produce `Eq` / `Ne` nodes instead of Python structural equality on dataclass fields.

Regression coverage: `tests/pyright/pass/expr_comparisons.py` (Pyright strict) and runtime tests in `tests/test_expr_api_coverage.py`.

---

## 8. Recommended Public Typing Constraints

To maximize Pyright success, the public typed API should enforce these rules:

### Required
- column names must be `Literal[...]` at call sites
- expressions must be `Expr[T]`
- transformations must be immutable

### Forbidden in the safe API
- runtime-computed column names
- `lambda`-based apply
- backend-native raw expressions in typed methods

## 8.1 Adapter/host annotation ergonomics (widening `Frame[...]`)

In downstream adapters and “host types” (composition wrappers), you often want to expose a precise `Frame[SchemaT, BackendFrameT, BackendExprT]` **internally**, but allow users to annotate it more loosely without repeating the exact type arguments everywhere.

Because `Frame[...]` is a generic with invariant parameters (as in most Python type checkers), the recommended pattern is to use a **deliberate widening alias**:

- `planframe.typing.FrameAny` (an alias for `Frame[Any, Any, Any]`)

Example:

```python
from planframe.typing import FrameAny

def takes_any_frame(x: FrameAny) -> None: ...
```

This keeps adapter surfaces ergonomic without changing core `Frame` semantics.

---

## 9. The Realistic Implementation Model

The practical design is:

### Stage 1 — Strong operation typing
- expressions are typed
- operations are typed
- frame plan type propagates

### Stage 2 — Exact schema views at explicit boundaries
- `materialize_model("OutputModel")`
- generated `.pyi` support for frozen pipelines

### Stage 3 — Optional plugin
Implement plan-AST-aware column resolution.

---

## 10. Materialization Boundary

This is the most important ergonomics tool.

```python
result = (
    pf
    .select("id", "name", "age")
    .with_column("age_plus_one", add(col("age"), lit(1)))
    .rename(name="full_name")
)

OutputModel = result.materialize_model("OutputModel")
```

At this point:

- the runtime schema is known exactly
- a Pydantic or dataclass model can be generated
- exact static types can be emitted through stubs or codegen

---

## 11. Recommended Resolution Tiers

### Tier 1 — No plugin
Use overloads + materialization + generated stubs.

### Tier 2 — Stub generation
For stable pipelines, emit `.pyi` or codegen classes.

### Tier 3 — Pyright plugin
Implement full logical `Resolve` over the plan AST.

#### Practical note (Pyright does not support plugins)
Despite the name “Tier 3 — Pyright plugin” in early drafts, **Pyright does not currently support a general plugin system** (unlike mypy). This means Tier 3 cannot be implemented as an in-process Pyright plugin without upstream Pyright changes.

Instead, Tier 3 should be treated as a **PlanFrame-owned external resolver** that can:

- evaluate `Resolve` over a `PlanNode` tree, and
- emit artifacts Pyright *can* consume (e.g. generated `.pyi` for stable pipelines, or codegen at explicit boundaries).

See `resolve-tier-3.md` for the feasibility note and an incremental scope proposal.

---

## 12. Exact Recommendation for MVP

For the first release:

1. Make `Expr[T]` robust and portable
2. Make `Frame[PlanT, BackendT]` immutable
3. Support exact typed methods for:
   - `select`
   - `drop`
   - `rename`
   - `with_column`
   - `cast`
   - `filter`
4. Use literal-only APIs
5. Make exact concrete output available at `materialize_model`
6. Treat fully-general `Resolve` as a future plugin feature

