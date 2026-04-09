# PlanFrame — Standalone Typed DataFrame Engine (Full Plan & Design Doc)

## Overview

PlanFrame is a backend-agnostic, statically-typed transformation engine for Python DataFrames.

It provides:
- Static typing across all transformations
- Schema evolution without manual intermediate models
- A typed expression system
- Execution via adapters (pandas, Polars, etc.)

PlanFrame is NOT a DataFrame library.
It is a typed relational planning layer that sits on top of existing DataFrame engines.

---

# 1. Core Principles

1. Schema is derived, not declared
2. Transformations are immutable
3. All schema changes must be statically knowable
4. Backend execution is decoupled from typing
5. Expressions are typed and portable

### Execution principle (updated)
PlanFrame is **always lazy**:
- chaining operations does not execute backend work
- execution happens at explicit boundaries (e.g. `collect()`)

---

# 2. Architecture Overview

PlanFrame is composed of 4 layers:

1. Core Plan Layer (typed AST)
2. Expression Layer (typed IR)
3. Schema Layer (IR + materialization)
4. Backend Adapter Layer

---

# 3. Core Types

## 3.1 Frame

class Frame[PlanT, BackendT]:
    ...

- PlanT = transformation pipeline
- BackendT = execution engine

In practice, a `Frame` holds a **source backend object** plus a **logical plan**. Backend operations are applied when the plan is evaluated at `collect()`.

---

## 3.2 Plan Nodes

- Source[Schema]
- Select[Prev, Keys]
- WithColumn[Prev, Name, Expr]
- Rename[Prev, Mapping]
- Drop[Prev, Keys]
- Join[Left, Right, left_on, right_on] (column and/or expression keys)
- GroupBy[Prev, keys] then Agg[GroupBy, named_aggs] — keys are column names or expressions; aggregations are `(op, column)` and/or `AggExpr` wrappers (`agg_sum(inner)`, …)

### IR versioning

`Source` carries an `ir_version` integer (default `1`) to make long-lived plans and cross-process
pipelines versionable. Increment this when making breaking changes to `PlanNode` shapes.

---

# 4. Expression System

Expressions are typed:

Expr[int]
Expr[str]
Expr[bool]

Supported operations:

- col("name")
- lit(value)
- add(a, b)
- eq(a, b)
- logical ops
- **AggExpr**: `agg_sum(expr)`, `agg_mean(expr)`, … for use only inside `group_by(...).agg(...)`

Expressions are backend-agnostic and compiled by adapters.

---

# 5. Schema System

## 5.1 Input Schema

Supported:
- Pydantic
- dataclass
- TypedDict

## 5.2 Schema Resolution

Schema is not stored — it is derived via Resolve:

Resolve rules:

- Source[S] → S
- Select[P, Keys] → subset
- WithColumn[P, Name, T] → add field
- Rename[P, Mapping] → rename keys

---

# 6. Backend Adapter System

## 6.1 Adapter Interface

class BackendAdapter:
    def select(self, df, cols): ...
    def with_column(self, df, name, expr): ...
    def rename(self, df, mapping): ...

---

## 6.2 Supported Backends (initial)

- Polars
- Pandas

Future:
- SQL
- PySpark

---

# 7. Public API

pf = from_polars(df, schema=UserSchema)

result = (
    pf
    .select("id", "name")
    .with_column("age_plus_one", add(col("age"), lit(1)))
    .rename(name="full_name")
)

---

# 8. Materialization

model = result.materialize_model("OutputSchema")
df_out = result.collect()

`materialize_model(...)` uses the derived schema and does not require executing the plan.

---

# 9. Package Structure

The repository ships a flatter layout than this early sketch (`planframe/expr`, `planframe/schema`, `planframe/backend`, …). For an up-to-date map of how **`Frame`**, **`PlanCompileContext`**, and **`execute_plan`** fit together, see [Core layout](core-layout.md).

---

# 10. Pyright Strategy

- Use Literal for column names
- Avoid Any leakage
- Provide .pyi stubs if needed
- Optional plugin for Resolve

---

# 11. MVP Scope

Supported operations:
- select
- drop
- rename
- with_column
- cast
- filter
- join (basic)
- group_by / agg (expression group keys; tuple reductions and `AggExpr` aggregations)

---

# 12. Future Enhancements

- Pyright plugin
- SQL compilation
- schema diffing
- query optimization

---

# 13. Target Users

- Data engineers
- Backend engineers using FastAPI
- Teams needing schema safety

---

# 14. Positioning

PlanFrame is:
"A typed relational planning layer for Python DataFrames"

---

# 15. Final Insight

PlanFrame separates:
- WHAT transformations mean (typing layer)
- HOW they execute (backend layer)

This enables:
- static safety
- backend flexibility
- clean architecture

---

# End of Document

