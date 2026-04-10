# PlanFrame — Backend Adapter Interface Design (Polars First)

## Purpose

This document defines the backend adapter protocol for PlanFrame.

PlanFrame is a backend-agnostic typed transformation engine. It should not execute operations directly against pandas, Polars, or other runtimes in the core layer. Instead, each backend should implement a small, explicit adapter interface.

The adapter layer has one job:

> Translate PlanFrame's typed relational operations into backend-native operations.

---

## 1. Design Goals

The adapter layer must be:

- backend-agnostic at the core package boundary
- small and explicit
- stable enough for third-party adapters
- decoupled from static typing logic
- able to compile typed expressions into backend-native expressions

The adapter must not:
- redefine schema semantics
- expose backend-native typing semantics into the core API
- weaken PlanFrame's safe subset

---

## 2. Guiding Rule

The core package owns:
- schema semantics
- expression semantics
- transformation semantics
- plan nodes

The adapter owns:
- runtime execution (evaluating a plan only when `collect()` is called)
- backend expression compilation
- runtime collection/materialization
- backend dtype mapping
- backend IO entrypoints (via `BaseAdapter.reader` / `BaseAdapter.writer`)
- async IO entrypoints (optional) via `BaseAdapter.areader` / `BaseAdapter.awriter`
- row streaming (optional) via `AdapterRowStreamer` (`Frame.stream_dicts` / `Frame.astream_dicts`)

---

## 3. Proposed Package Split

## planframe
Contains:
- Frame API
- plan nodes
- expression IR
- schema IR
- materialization interfaces

## planframe-polars
Contains:
- PolarsAdapter
- expression compiler to Polars expressions
- dtype mapping to/from Polars
- runtime collection helpers

## planframe-pandas
Contains:
- PandasAdapter
- expression compiler to pandas operations
- dtype mapping
- eager execution helpers

MVP recommendation:
- ship `planframe-core`
- ship `planframe-polars` first
- treat pandas as second backend

---

## 4. Core Adapter Protocol

```python
from __future__ import annotations

from typing import Any, Protocol, TypeVar, Generic

BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")

class BackendAdapter(Protocol, Generic[BackendFrameT, BackendExprT]):
    name: str

    def select(self, df: BackendFrameT, columns: tuple[str, ...]) -> BackendFrameT:
        ...

    def drop(self, df: BackendFrameT, columns: tuple[str, ...]) -> BackendFrameT:
        ...

    def rename(self, df: BackendFrameT, mapping: dict[str, str]) -> BackendFrameT:
        ...

    def with_column(
        self,
        df: BackendFrameT,
        name: str,
        expr: BackendExprT,
    ) -> BackendFrameT:
        ...

    def cast(
        self,
        df: BackendFrameT,
        name: str,
        dtype: Any,
    ) -> BackendFrameT:
        ...

    def filter(
        self,
        df: BackendFrameT,
        predicate: BackendExprT,
    ) -> BackendFrameT:
        ...

    def compile_expr(self, expr: Any, *, schema: Any | None = None) -> BackendExprT:
        ...

    def collect(self, df: BackendFrameT, *, options: ExecutionOptions | None = None) -> BackendFrameT:
        ...

    async def acollect(self, df: BackendFrameT, *, options: ExecutionOptions | None = None) -> BackendFrameT:
        ...

    async def ato_dicts(
        self, df: BackendFrameT, *, options: ExecutionOptions | None = None
    ) -> list[dict[str, object]]:
        ...

    async def ato_dict(
        self, df: BackendFrameT, *, options: ExecutionOptions | None = None
    ) -> dict[str, list[object]]:
        ...
```

### Notes
- `compile_expr` converts PlanFrame expression IR into backend-native expression objects.
- `collect` may be a no-op for eager backends like pandas.
- `BackendFrameT` may be a DataFrame or LazyFrame depending on backend strategy.

### Async materialization ([issue #15](https://github.com/eddiethedean/planframe/issues/15))

PlanFrame stays **synchronous for lazy chaining**: building a `Frame` only updates the logical plan. **Materialization** can be sync or async:

| API | Role |
| --- | --- |
| `Frame.collect_backend()`, `Frame.to_dicts()`, `Frame.to_dict()` | Blocking; call from sync code or from `asyncio.to_thread`. |
| `Frame.acollect_backend()`, `Frame.ato_dicts()`, `Frame.ato_dict()` | Awaitable; use in async code. |

`BaseAdapter` provides default `acollect` / `ato_dicts` / `ato_dict` that run the matching sync method in `asyncio.to_thread`, so existing adapters work without changes. Backends backed by asyncio-only clients should **override** `acollect` (and optionally `ato_dicts` / `ato_dict`) to await their native I/O instead of blocking a thread.

**Plan evaluation** (`execute_plan` walking the `PlanNode` tree—what `Frame` runs before `collect` / `to_dict*`) remains synchronous on the event-loop thread for both sync and async terminals. Concretely, `Frame.acollect_backend()` first computes `planned = Frame._eval(Frame.plan)` (synchronous), then awaits `BaseAdapter.acollect(planned, options=...)`.

**ExecutionOptions propagation:** the optional `options: ExecutionOptions | None` is forwarded to both:

- `execute_plan(..., options=...)` (plan interpretation / compilation context)
- adapter materialization/export (`collect` / `to_dicts` / `to_dict` and async variants)

Adapters should accept `options` and treat it as a set of backend-defined hints: forward what is meaningful for the engine, ignore unknown hints, and keep signatures stable for third-party consumers.

**Thread safety:** default async methods may invoke the adapter from multiple thread-pool workers concurrently if several `acollect` tasks run in parallel. Adapters that mutate shared connection state should document constraints or serialize; thread-local or per-task clients are typical.

---

## 5. Recommended Runtime Model

PlanFrame's `Frame` object should hold:

- a backend adapter instance
- a backend-native runtime object
- a plan node or plan metadata
- schema metadata

Example:

```python
class Frame(Generic[PlanT, BackendT]):
    def __init__(
        self,
        data: Any,
        adapter: BackendAdapter[Any, Any],
        plan: Any,
        schema_ir: Any,
    ) -> None:
        self._data = data
        self._adapter = adapter
        self._plan = plan
        self._schema_ir = schema_ir
```

Each method:
1. updates the logical plan
2. updates derived schema metadata
3. returns a new immutable `Frame`

Execution is deferred:
- adapter methods like `select(...)` / `filter(...)` are applied when the plan is evaluated (typically inside `collect()` / `acollect()`), not during chaining.

---

## 6. Expression Compilation Contract

The expression IR must be owned by core PlanFrame.

The backend adapter is responsible for compiling it.

### PlanFrame expression examples
- `col("age")`
- `lit(1)`
- `add(col("age"), lit(1))`
- `eq(col("country"), lit("US"))`
- `and_(...)`

### Adapter responsibility
- pattern match or visit the expression tree
- return backend-native expression representation

---

## 7. Polars Adapter Design

Polars is the best first backend because it already has:
- an expression system
- clear column semantics
- eager and lazy execution models
- a relatively modern dtype model

### Proposed choice for v1
Prefer Polars `LazyFrame` internally where practical.

Advantages:
- natural fit for relational plan chaining
- backend execution remains deferred (PlanFrame evaluates only at `collect()`)
- better alignment with plan-based architecture

### Minimal Polars adapter skeleton

```python
from __future__ import annotations

from typing import Any
import polars as pl

class PolarsAdapter:
    name = "polars"

    def select(self, df: pl.DataFrame | pl.LazyFrame, columns: tuple[str, ...]):
        return df.select(list(columns))

    def drop(self, df: pl.DataFrame | pl.LazyFrame, columns: tuple[str, ...]):
        return df.drop(list(columns))

    def rename(self, df: pl.DataFrame | pl.LazyFrame, mapping: dict[str, str]):
        return df.rename(mapping)

    def with_column(self, df: pl.DataFrame | pl.LazyFrame, name: str, expr: pl.Expr):
        return df.with_columns(expr.alias(name))

    def cast(self, df: pl.DataFrame | pl.LazyFrame, name: str, dtype: Any):
        return df.with_columns(pl.col(name).cast(dtype))

    def filter(self, df: pl.DataFrame | pl.LazyFrame, predicate: pl.Expr):
        return df.filter(predicate)

    def compile_expr(self, expr: Any, *, schema: Any | None = None) -> pl.Expr:
        ...

    def collect(self, df: pl.DataFrame | pl.LazyFrame, *, options=None):
        _ = options
        return df.collect() if isinstance(df, pl.LazyFrame) else df
```

---

## 8. Polars Expression Compiler

Use a small visitor or dispatcher.

### Example expression IR nodes
```python
class Expr[T]: ...
class Col(Expr[T]): ...
class Lit(Expr[T]): ...
class Add(Expr[int]): ...
class Eq(Expr[bool]): ...
class AggExpr(Expr[object]): ...  # op + inner; compiled to per-group reductions in agg context
```

### Compiler sketch
```python
import polars as pl

def compile_expr(expr: Expr[Any]) -> pl.Expr:
    if isinstance(expr, Col):
        return pl.col(expr.name)
    if isinstance(expr, Lit):
        return pl.lit(expr.value)
    if isinstance(expr, Add):
        return compile_expr(expr.left) + compile_expr(expr.right)
    if isinstance(expr, Eq):
        return compile_expr(expr.left) == compile_expr(expr.right)
    raise TypeError(f"Unsupported expr node: {type(expr)!r}")
```

This keeps typing and execution clearly separated.

---

## 9. Pandas Adapter Design

Pandas should be treated as a second backend because:
- its expression model is less uniform
- eager mutation semantics are more dangerous
- dtype handling is more inconsistent

Still, it is important as a popular target.

### Recommended pandas strategy
- keep PlanFrame immutable even if pandas is mutable
- clone or assign into new frames in adapter methods
- compile expressions into vectorized pandas operations only
- do not support arbitrary `.apply(...)` in safe typed mode

### Pandas adapter sketch
```python
import pandas as pd

class PandasAdapter:
    name = "pandas"

    def select(self, df: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
        return df.loc[:, list(columns)].copy()

    def drop(self, df: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
        return df.drop(columns=list(columns)).copy()

    def rename(self, df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
        return df.rename(columns=mapping).copy()

    def with_column(self, df: pd.DataFrame, name: str, expr: Any) -> pd.DataFrame:
        out = df.copy()
        out[name] = expr
        return out

    def cast(self, df: pd.DataFrame, name: str, dtype: Any) -> pd.DataFrame:
        out = df.copy()
        out[name] = out[name].astype(dtype)
        return out

    def filter(self, df: pd.DataFrame, predicate: Any) -> pd.DataFrame:
        return df.loc[predicate].copy()

    def compile_expr(self, expr: Any, *, schema: Any | None = None) -> Any:
        ...

    def collect(self, df: pd.DataFrame, *, options=None) -> pd.DataFrame:
        _ = options
        return df
```

---

## 10. Join Support

Joins are **implemented** in the shipped `BaseAdapter`: symmetric `on` or asymmetric `left_on` / `right_on`, each key a column name or compiled expression (`CompiledJoinKey`), optional `JoinOptions` (including execution hints like `streaming` / `engine_streaming`, `allow_parallel` / `force_parallel`), and schema merge / suffix rules owned by core PlanFrame.

Historical note: early drafts deferred joins until collision semantics were fixed; the current protocol and Polars adapter reflect the merged design.

---

## 11. Error Handling Rules

Adapters should raise backend-specific errors internally, but the public API should normalize them into PlanFrame exceptions where appropriate.

Recommended core exceptions:
- `PlanFrameBackendError`
- `PlanFrameExpressionError`
- `PlanFrameSchemaError`
- `PlanFrameExecutionError`

This prevents backend leaks in the public contract.

---

## 12. Backend Feature Policy

Not every backend can support every feature equally.

PlanFrame should adopt this policy:

- core safe subset is portable
- adapters may support optional extensions
- extensions must be clearly marked as backend-specific and unsafe or experimental if they weaken portability

This matters because the package promise is not:
> every feature of every backend

The promise is:
> a sound typed relational subset across multiple backends

---

## 13. Recommended v1 Adapter Surface

The original v1 sketch listed only:

- `select`
- `drop`
- `rename`
- `with_column`
- `cast`
- `filter`
- `compile_expr`
- `collect`

The **shipped** `BaseAdapter` in this repository extends that surface (joins, sort, `unique`, **`group_by_agg`**, reshape helpers, I/O, etc.). Third-party adapters should implement the full abstract API in `packages/planframe/planframe/backend/adapter.py`.

---

## 14. Testing Strategy

Adapter tests should be split into two groups.

### Plan introspection (tooling)

Tool builders and adapter authors can walk a plan tree using `planframe.plan.iter_plan_nodes`.
By default it traverses only the primary `prev` chain; pass `include_side_frames=True` to
also descend into join/concat side frames (RHS/other frame plans) in a deterministic order.

### Conformance tests
These test that every backend satisfies the same logical behavior.

Examples:
- select preserves row count and selected columns
- drop removes requested columns
- with_column adds a correctly typed value column
- filter preserves schema but changes row count

### Backend-specific tests
These test backend-specific edge behavior.

Examples:
- Polars LazyFrame collection behavior
- pandas nullable dtype edge cases

---

## 15. Grouping and aggregation (`group_by_agg`)

PlanFrame represents **`Frame.group_by(...).agg(...)`** as plan nodes **`GroupBy`** then **`Agg`**. At execution time the adapter receives **`group_by_agg(df, keys=..., named_aggs=...)`** where **`df`** is the input frame *before* grouping.

### Group keys (`keys`)

Same structural contract as join/sort keys: a tuple of **`CompiledJoinKey[BackendExprT]`** (alias of **`CompiledSortKey`**), each slot either a column name or a compiled expression. Expression slots correspond to synthetic output names **`__pf_g0`**, **`__pf_g1`**, … in the derived schema.

### Aggregations (`named_aggs`)

A mapping from output column name to either:

1. **`(op, column_name)`** with `op` in `count`, `sum`, `mean`, `min`, `max`, `n_unique`.
2. A **compiled backend expression** that is already a valid per-group aggregation for that engine (PlanFrame obtains this by compiling **`AggExpr`** IR such as `agg_sum(inner)`).

Adapters should respect dict insertion order when building the backend’s aggregation list.

---

## 16. Final Recommendation

The project followed an incremental path:

1. **`planframe`** (core) plus **`planframe-polars`** as the first backend
2. A **minimal expression compiler**, then growth of the IR (including **`AggExpr`** for grouped reductions)
3. A **strict, typed subset** of dataframe operations—still intentionally smaller than “all of Polars”

The adapter layer has grown to include **join**, **sort**, **grouping** (`group_by_agg`), and other operations, but the design goal is unchanged: keep the protocol **explicit and boring**, with schema semantics owned by core and execution owned by adapters.

This keeps the package coherent, portable, and realistically shippable.

