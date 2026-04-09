# Polars `LazyFrame` API alignment (PlanFrame `Frame`)

This document tracks how `planframe.Frame` is being aligned to Polars' `LazyFrame` interface.

PlanFrame remains **always lazy** and **statically typed** (literal column names are encouraged via `.pyi` stubs). Where Polars' Python API patterns would weaken typing, PlanFrame may use small, typed-friendly deviations (noted below).

## Initial alignment set (vNext)

| Polars `LazyFrame` | PlanFrame (new canonical) | PlanFrame (legacy) | Notes |
| --- | --- | --- | --- |
| `with_columns(...)` | `with_columns(exprs: Mapping[str, Expr] \| None = None, **named_exprs: Expr)` | `with_column(name, expr)` | PlanFrame requires column names to evolve schema typing; `Expr` aliasing is not part of PlanFrame IR. |
| `with_row_index(name=..., offset=...)` | `with_row_index(name=\"row_nr\", offset=0)` | `with_row_count(...)` | Renamed for Polars parity; behavior unchanged. |
| `unpivot(index=..., on=..., variable_name=..., value_name=...)` | `unpivot(...)` | `melt(id_vars=..., value_vars=...)` | `unpivot` is the Polars-first name; `melt` remains as a deprecated alias. |
| `rename(mapping=...)` | `rename(mapping: Mapping[str,str] \| None = None, **kwargs: str)` | `rename(**mapping)` | Accepts Polars-style `mapping=` while preserving typed `**kwargs` ergonomics. |
| `drop_nulls(subset=...)` | `drop_nulls(subset: Sequence[str] \| str \| None = None, how=..., threshold=...)` | `drop_nulls(*subset, how=..., threshold=...)` | PlanFrame keeps `how`/`threshold` as extensions; positional subset is deprecated. |
| `vstack(...)` / `hstack(...)` | `vstack(other)` / `hstack(other)` | `concat_vertical(...)` / `concat_horizontal(...)` | Method naming aligned to Polars DataFrame verbs (PlanFrame stays lazy). |

## Deprecation policy

Legacy names remain temporarily as wrappers that emit `DeprecationWarning` and delegate to the new canonical method names. After one compatibility window, legacy names will be removed.

