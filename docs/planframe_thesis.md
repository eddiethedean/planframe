
# PlanFrame — Product Thesis & Vision

## Introduction

Python has become the dominant language for data work because it is flexible, expressive, and backed by powerful libraries like pandas and Polars. These tools allow developers to rapidly manipulate and transform data with minimal friction.

But this flexibility comes with a cost.

As data pipelines grow in complexity, the structure of the data—the schema—becomes increasingly implicit. Columns appear and disappear across transformations. Types change silently. Downstream code assumes structure that may no longer exist. IDEs and type checkers offer limited help because most dataframe operations are dynamic and string-based.

In small scripts, this is manageable. In real systems, it becomes a major source of fragility.

PlanFrame exists to solve this problem.

---

## The Problem

Modern dataframe workflows suffer from three core issues:

### 1. Implicit Schema Evolution

Dataframes change shape constantly:

- columns are selected, dropped, or renamed
- new columns are derived
- types are cast or inferred

But these changes are rarely explicit in a way that tools can understand. The schema lives in the developer’s head, not in the type system.

---

### 2. Weak Static Guarantees

Static type checkers like Pyright cannot reliably track:

- whether a column exists
- what type a column has after transformations
- whether a rename or drop breaks downstream code

This leads to runtime errors that could have been caught earlier.

---

### 3. Poor Tooling Feedback

Because schema is not statically represented:

- autocomplete is weak
- refactoring is risky
- documentation becomes stale
- APIs built on top of dataframes lack strong contracts

---

## The Insight

Most dataframe transformations are not inherently dynamic.

When a developer writes:

```python
df.select("id", "name")
```

the schema change is obvious.

When they write:

```python
df.with_column("age_plus_one", add(col("age"), lit(1)))
```

the output type is known.

Humans can reason about these transformations. The problem is that tools cannot.

---

## The Core Idea

PlanFrame introduces a new abstraction:

> A typed relational transformation plan layered on top of existing dataframe engines.

Instead of treating a dataframe as a mutable object, PlanFrame treats it as a **program** that describes how data is transformed.

Each operation:

- has explicit schema semantics
- is statically representable
- is composable
- is backend-agnostic

And critically:

- **does not execute** when it is declared
- executes only at explicit boundaries (e.g. `collect()`)

---

## What PlanFrame Is

PlanFrame is:

- a typed transformation DSL
- a schema evolution engine
- a backend-agnostic execution layer
- a bridge between dynamic data processing and static typing
- capable of expressing **grouped analytics** with the same typed expression IR used elsewhere (e.g. expression group keys and `AggExpr`-wrapped reductions like `agg_sum(truediv(col("a"), col("b")))`)

---

## What PlanFrame Is Not

PlanFrame is not:

- a replacement for pandas or Polars
- a full-featured dataframe engine
- a dynamic scripting tool

It deliberately focuses on a **safe, typed subset** of dataframe operations.

---

## The Design Philosophy

### 1. Schema Is Derived, Not Declared

Users define the schema once at the source.

All subsequent schemas are derived automatically through typed transformations.

---

### 2. Transformations Are Immutable

Each operation returns a new frame.

This makes schema evolution predictable and composable.

---

### 3. Typing Is a First-Class Feature

Every operation carries type information.

Column types are propagated through the transformation plan.

---

### 4. Execution Is Delegated

PlanFrame does not execute data operations directly.

It delegates to backend engines like pandas or Polars via adapters.

Execution is **always lazy** from PlanFrame’s perspective: adapters are invoked when the plan is evaluated, not during chaining.

---

### 5. Safety Over Flexibility

Not all dataframe operations are supported.

Only those that can be expressed with clear schema semantics are included in the core API.

---

## The Developer Experience

A typical workflow looks like this:

```python
pf = from_polars(df, schema=UserSchema)

result = (
    pf
    .select("id", "name", "age")
    .with_column("age_plus_one", add(col("age"), lit(1)))
    .rename(name="full_name")
)
```

At this point:

- the transformation plan is fully typed
- schema evolution is tracked
- no intermediate models were defined

When needed:

```python
Model = result.materialize_model("Output")
data = result.collect()
```

Now the schema is concrete and can be used in APIs, validation, or storage.

---

## Why This Matters

PlanFrame bridges a gap between:

- the flexibility of dataframe programming
- the safety of typed software engineering

It allows developers to:

- catch schema errors earlier
- reason about transformations more clearly
- generate reliable data models
- maintain consistency across pipelines

---

## The Tradeoff

PlanFrame intentionally limits flexibility.

It does not support:

- arbitrary Python functions in transformations
- dynamic column discovery
- implicit schema changes

Instead, it offers:

- explicit operations
- predictable behavior
- strong guarantees

---

## The Bigger Vision

PlanFrame is not just a utility library.

It is a foundation for:

- typed data pipelines
- schema-aware APIs
- backend-agnostic data processing
- eventually, query compilation and optimization

It reframes dataframe programming as:

> building a typed data transformation program

---

## Conclusion

PlanFrame is an attempt to bring clarity, safety, and structure to Python dataframe workflows.

It does not replace existing tools.

It makes them more predictable, more composable, and more understandable.

By making schema evolution explicit and type-safe, PlanFrame turns data transformations into something both humans and tools can reason about.

That is its purpose.
