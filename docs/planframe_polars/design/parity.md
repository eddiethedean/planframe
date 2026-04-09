# planframe-polars parity matrix (Polars `DataFrame` / `LazyFrame`)

This page tracks how closely **`planframe_polars`** matches the parent Polars interfaces.

PlanFrame remains **lazy-first** (plans execute at `collect()` boundaries), so we primarily target **`polars.LazyFrame`** parity. Where `DataFrame`/`LazyFrame` differ in Polars, we either:

- implement the `LazyFrame` shape, or
- provide a typed-friendly wrapper with an explicit divergence note.

## Legend

- **Exact parity**: same name + same argument shape + same behavior (modulo “always lazy”).
- **Typed-parity**: same name/args, but restricted to a typed-safe subset (e.g. disallow string SQL).
- **Divergence**: exists but intentionally differs (documented).
- **Not supported**: explicitly unsupported; should raise a clear error.

## High-value verbs (current focus)

| Polars | PlanFrame polars frontend | Status | Notes |
| --- | --- | --- | --- |
| `select(...)` | `Frame.select(...)` | Typed-parity | Supports strings, `(name, Expr)`, `col(\"x\")`, and `expr.alias(\"name\")`. |
| `with_columns(...)` | `Frame.with_columns(...)` | Typed-parity | Named exprs are typed; positional exprs require `expr.alias(\"name\")`. |
| `filter(...)` | `Frame.filter(...)` | Exact parity | Predicate is `Expr[bool]` (typed). |
| `sort(...)` / `sort_by(...)` | `Frame.sort(...)` / `Frame.sort_by(...)` | Typed-parity | Supports column names and expressions; per-key flags supported. |
| `unique(...)` | `Frame.unique(...)` | Typed-parity | Args align closely. |
| `is_duplicated(...)` | `Frame.is_duplicated(...)` | Typed-parity | Implemented as a wrapper over `duplicated(...)`. |
| `join(...)` | `Frame.join(...)` | Typed-parity | Options forwarded via `JoinOptions` where supported. |
| `vstack/hstack` | `Frame.vstack(...)` / `Frame.hstack(...)` | Typed-parity | Schema validation enforced. |
| `unpivot(...)` | `Frame.unpivot(...)` | Typed-parity | `melt(...)` remains as deprecated alias. |
| `sink_*` | `Frame.sink_parquet/csv/...` | Divergence | Polars uses `sink_*` on `LazyFrame` and `write_*` on `DataFrame`; PlanFrame exposes `sink_*` and keeps `write_*` deprecated wrappers. |

## Known gaps / divergences (to address)

- **Lazy sampling**: current Polars adapter may reject `sample()` on `LazyFrame` for this backend (needs parity decision).
- **`outer=True` explode/posexplode**: current Polars adapter rejects `outer=True`.
- **storage options**: some Polars IO methods do not accept `storage_options` (PlanFrame keeps `StorageOptions` but may not be able to forward).

## Where this is implemented

- Core plan engine: `packages/planframe/planframe/frame/_mixin_ops.py`, `packages/planframe/planframe/frame/_mixin_io.py`
- Polars adapter: `packages/planframe-polars/planframe_polars/adapter.py`
- Polars expr compiler: `packages/planframe-polars/planframe_polars/compile_expr.py`

