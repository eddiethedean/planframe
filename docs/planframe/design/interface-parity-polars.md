# Interface parity: PlanFrame core vs Polars

This page compares the **core PlanFrame interface** (`planframe.Frame`) to the parent Polars interfaces (`polars.LazyFrame` / `polars.DataFrame`).

PlanFrame is **always lazy**, so we treat `polars.LazyFrame` as the primary parity target.

## Legend

- **Exact parity**: same name + same argument shape + same behavior (modulo “always lazy”).
- **Typed-parity**: same shape, but restricted to typed-safe subsets.
- **Divergence**: present but intentionally differs (documented).
- **Unsupported**: not implemented; should fail loudly.

## Core mapping (high-signal)

| Polars | PlanFrame core (`Frame`) | Status | Notes |
| --- | --- | --- | --- |
| `select(...)` | `select(...)` | Typed-parity | Supports `str`, `col("x")`, `(name, Expr)`, and `expr.alias("name")`. |
| `with_columns(...)` | `with_columns(...)` | Typed-parity | Positional exprs require `expr.alias("name")`; named exprs `with_columns(x=expr)` are typed. |
| `with_row_index(...)` | `with_row_index(...)` | Typed-parity | Name defaults align; PlanFrame tracks schema. |
| `filter(...)` | `filter(...)` | Typed-parity | Predicate is typed `Expr[bool]`. |
| `sort(...)` | `sort(...)` | Typed-parity | Supports names + expr keys; per-key flags are supported. |
| `unique(...)` | `unique(...)` | Typed-parity | `keep` and `maintain_order` supported. |
| `is_duplicated(...)` | `is_duplicated(...)` | Typed-parity | Wrapper over `duplicated(...)`. |
| `join(...)` | `join(...)` | Typed-parity | Normalizes `on/left_on/right_on` to typed shapes; forwards `JoinOptions` when supported. |
| `unpivot(...)` | `unpivot(...)` | Typed-parity | Canonical reshape primitive. |
| `vstack/hstack` | `vstack/hstack` | Typed-parity | PlanFrame enforces schema constraints more strictly (typing + validation). |
| `sink_*` | `sink_*` | Divergence | PlanFrame exposes `sink_*` (and also `write_*` convenience wrappers) as lazy execution boundaries. |

## Generated inventories (method-by-method)

For an exhaustive, generated inventory:

- `Frame` vs `polars.LazyFrame`: `planframe/design/_generated/interface-inventory-polars.md`
- Polars methods missing from PlanFrame core: `planframe/design/_generated/polars-missing.md`

These tables are generated from:

- PlanFrame stubs: `packages/planframe/planframe/frame/__init__.pyi`
- Runtime Polars introspection (when installed): `polars.LazyFrame`

## Major divergences (by design)

- **Always-lazy semantics**: PlanFrame does not expose eager `DataFrame`-style mutation APIs.
- **Typing-driven restrictions**: anywhere Polars accepts highly dynamic inputs (e.g. string expressions or loosely typed callables), PlanFrame restricts to typed `Expr`/selectors.
- **Schema evolution as a type-level concern**: operations that change columns must be representable in the typing/stub strategy.

## Missing Polars LazyFrame methods (categorized)

Polars is a very broad API surface; PlanFrame’s strategy is to prioritize parity for core relational verbs and typed expression workflows first, then add additional convenience methods when they can be represented safely in our typing model.

High-signal categories from `polars-missing.md`:

- **Execution/engine diagnostics** (likely divergence/unsupported in core): `explain`, `profile`, `show_graph`, `remote`, `inspect`, `cache`.
- **Eager-returning helpers** (divergence; PlanFrame stays lazy): `describe`, `fetch`, `show`.
- **Async/batching collection APIs** (potential future additions; today we use `acollect` / adapters): `collect_async`, `collect_batches`.
- **SQL/string DSL and opaque Python callables** (typed restriction): `sql`, `map_batches`, `pipe`, `pipe_with_schema`.
- **Aggregation shorthands** (potential typed-parity additions via `select` + expressions): `sum`, `mean`, `min`, `max`, `median`, `std`, `count`, `null_count`.
- **Legacy aliases** (we intentionally removed deprecated names): `melt` (canonical in PlanFrame is `unpivot`).

## Known backend-dependent gaps

Some parity gaps live in adapter implementations (not the core API) and may vary by backend:

- `sample()` on lazy frames (adapter-dependent)
- `explode(..., outer=True)` and `posexplode(..., outer=True)` (adapter-dependent)
- forwarding `storage_options` everywhere Polars supports it (version/adapter-dependent)

