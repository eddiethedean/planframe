# Tier 3 Resolve: feasibility + scope (Pyright integration)

PlanFrame’s typing design describes “Tier 3” as a **Pyright plugin** that can perform full logical **Resolve** (compute output column names and types across a `Frame` plan AST).

## Feasibility: Pyright does not have a general plugin system

Pyright intentionally does **not** support a general-purpose plugin model (unlike mypy). That means PlanFrame cannot ship a “Pyright plugin” in the usual sense: there is no supported hook point to run custom Python/TypeScript logic during type checking.

**Implication:** Tier 3 should be implemented as a **PlanFrame-owned external resolver** that produces artifacts Pyright can already understand.

## What Tier 3 should deliver (incremental)

### 1) Spike (this document)

Clarify constraints and pick an approach that is:

- useful to adapter consumers,
- maintainable by PlanFrame,
- compatible with Pyright as it exists today.

### 2) MVP: external resolver + artifact emission

Define a small library surface (conceptual):

- **input**: a PlanFrame plan (`PlanNode`) plus a starting schema
- **output**: a “resolved schema view” (mapping `column_name -> python type`)

Then integrate via artifacts:

- **Generated stubs** (`.pyi`) for stable, pinned pipelines (Tier 2+)
- **Materialization boundary** (`materialize_model`) for explicit, ergonomic schema snapshots

### 3) Optional: editor workflow integration

If desired, provide a dev tool that:

- watches a project (or a list of pipelines),
- regenerates stubs into a known directory, and
- configures Pyright `extraPaths` to pick them up (similar to how this repo’s typing tests point Pyright at workspace packages).

## Suggested MVP scope for full Resolve

Start with a subset that matches the existing tiered docs:

- `select`
- `with_column`
- `rename`
- `drop`
- `cast`
- `filter` (schema-preserving)

Then expand to:

- `join` (suffix rules, nullability policies)
- `group_by(...).agg(...)` (key synthesis like `__pf_g0`, named aggs, `AggExpr`)

## CI strategy

Keep Tier 1–2 as the default (overloads + generated stubs).

If/when a resolver tool exists, add a **separate optional CI job** that runs the resolver over a curated set of pipelines and asserts it matches runtime schema derivation.

