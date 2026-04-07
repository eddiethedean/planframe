# PlanFrame — Resolve Typing Design for Pyright

## Purpose

This document defines a practical typing strategy for `Resolve`, the core mechanism that lets PlanFrame infer column types across schema-changing transformations without requiring users to define intermediate schema models.

The goal is not to make Python's type system do arbitrary computation. The goal is to design a type system that Pyright can follow reliably across a constrained, explicit API.

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
- `Column[T]`
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

### Frame

```python
class Frame(Generic[PlanT, BackendT]):
    ...
```

### Expressions

```python
class Expr(Generic[T]):
    ...
```

### Columns

```python
class Column(Generic[T]):
    ...
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
3. **A Pyright plugin**

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

## Rule 1 — Source
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

## Rule 2 — Select
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

Example overload pattern:

```python
from typing import overload
from typing_extensions import Literal

@overload
def select(self, __c1: Literal["id"]) -> Frame[Any, BackendT]: ...
@overload
def select(self, __c1: Literal["id"], __c2: Literal["name"]) -> Frame[Any, BackendT]: ...
```

The runtime implementation is generic, but the stubs drive static checking.

---

## Rule 3 — WithColumn
Adding a column introduces a new name with the expression output type.

Conceptually:

```python
Resolve[WithColumn[P, "age_plus_one", int], "age_plus_one"] -> int
Resolve[WithColumn[P, "id_plus_one", int], "id"] -> Resolve[P, "id"]
```

Pyright strategy:
- `with_column` must require a literal column name
- the expression type is carried by `Expr[T]`
- the return type is a new frame type tied to a new accessor proxy

Example:

```python
NameLit = TypeVar("NameLit", bound=str)
T = TypeVar("T")

def with_column(
    self,
    name: NameLit,
    expr: Expr[T],
) -> Frame[Any, BackendT]:
    ...
```

In raw Python typing this is not enough to fully expose the new column statically. That is why one of these must be true:

- you materialize
- you generate stubs
- or you use a plugin

MVP recommendation:
- make `with_column` fully typed for expression output
- expose exact resulting schema after `materialize()`
- optionally provide a debug `reveal_schema()` runtime method

---

## Rule 4 — Rename
Renaming moves the type from the old name to the new name.

Conceptually:

```python
Resolve[Rename[P, {"name": "full_name"}], "full_name"] -> Resolve[P, "name"]
Resolve[Rename[P, {"name": "full_name"}], "name"] -> error
```

Pyright strategy:
- only support keyword rename syntax in the typed API

Example:

```python
frame.rename(name="full_name")
```

This is easier to type than arbitrary dicts.

Support overloads for a limited number of renames in MVP.

---

## Rule 5 — Drop
Dropped columns disappear from the visible schema.

Conceptually:

```python
Resolve[Drop[P, ("age",)], "age"] -> error
```

Pyright strategy:
- same as `select`, but subtractive
- overload-based API for small arity
- exact schema becomes cleanest after `materialize()`

---

## Rule 6 — Cast
Casting keeps the name but changes the type.

Conceptually:

```python
Resolve[Cast[P, "id", str], "id"] -> str
```

Pyright strategy:
- explicit typed caster operations
- avoid accepting arbitrary Python types unless mapped through a supported dtype IR

---

## Rule 7 — Filter
Filtering does not change schema.

Conceptually:

```python
Resolve[Filter[P], K] -> Resolve[P, K]
```

This is one of the easiest operations to support statically and should be in v1.

---

## 8. Recommended Public Typing Constraints

To maximize Pyright success, the public typed API should enforce these rules:

### Required
- column names must be `Literal[...]` at call sites
- expressions must be `Expr[T]`
- transformations must be immutable
- no dict-based dynamic schemas in public typed methods
- no `Any` in public signatures unless it is an intentional unsafe boundary

### Forbidden in the safe API
- `df["col"]`
- runtime-computed column names
- `lambda`-based apply
- loops that mutate schema shape
- backend-native raw expressions in typed methods

---

## 9. The Realistic Implementation Model

The practical design is:

### Stage 1 — Strong operation typing
- expressions are typed
- operations are typed
- frame plan type propagates

### Stage 2 — Exact schema views at explicit boundaries
- `materialize_model("OutputModel")`
- `schema_view()`
- generated `.pyi` support for frozen pipelines

### Stage 3 — Optional plugin
The plugin can implement full logical `Resolve` over the plan AST.

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

This gives users the best of both worlds:
- fluent chains without manual intermediate models
- exact concrete schema when they care

---

## 11. Recommended Resolution Tiers

### Tier 1 — No plugin
Use overloads + materialization + generated stubs.

This is the fastest path to a useful package.

### Tier 2 — Stub generation
For stable pipelines, emit `.pyi` or codegen classes.

This improves exactness without requiring editor plugin logic.

### Tier 3 — Pyright plugin
Implement plan-AST-aware column resolution.

This is the most powerful, but also the highest maintenance cost.

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

This is enough to create a compelling package without overpromising impossible type magic in plain Python typing.

---

## 13. Example Signatures

```python
from __future__ import annotations

from typing import Generic, TypeVar, Any
from typing_extensions import Literal

PlanT = TypeVar("PlanT")
BackendT = TypeVar("BackendT")
T = TypeVar("T")

class Expr(Generic[T]):
    ...

class Frame(Generic[PlanT, BackendT]):
    def filter(self, predicate: Expr[bool]) -> Frame[PlanT, BackendT]:
        ...

    def with_column(
        self,
        name: str,
        expr: Expr[T],
    ) -> Frame[Any, BackendT]:
        ...

    def materialize_model(self, name: str) -> type[Any]:
        ...
```

### Recommended refinement

For polished typing, the user-facing package should ship `.pyi` files with richer overloads than the runtime implementation.

---

## 14. Final Position

`Resolve` should be treated as:

- a specification for how schema typing behaves
- a combination of public API restrictions and type propagation mechanisms
- not a promise that vanilla Python typing alone can evaluate arbitrary schema transformations perfectly

That honesty is essential to the package design.

PlanFrame can still deliver an extremely strong result:
- no user-defined intermediate models
- statically-typed transformations
- backend-agnostic execution
- exact concrete schemas at explicit boundaries
