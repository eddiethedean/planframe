# planframe-pandas parity matrix (pandas `DataFrame`)

This page tracks how closely **`planframe_pandas`** matches the parent pandas interface (`pandas.DataFrame`).

PlanFrame is **always lazy**, schema-driven, and typed. pandas is eager, index-heavy, and mutation-friendly. Full behavioral parity is not the goal; **API shape parity** and a clear, typed subset is.

## Legend

- **Exact parity**: same name + same argument shape + same behavior (rare for pandas, due to eager/index semantics).
- **Typed-parity**: same name/args, but restricted to typed-safe subsets.
- **Divergence**: exists but intentionally differs (documented).
- **Not supported**: explicitly unsupported; should raise a clear error.

## Core pandas-surface (current focus)

| pandas | PlanFrame pandas frontend | Status | Notes |
| --- | --- | --- | --- |
| `__getitem__` | `PandasLikeFrame.__getitem__` | Typed-parity | `df[\"col\"]` returns a typed expression-like `Series`, not data. `df[[...]]` selects columns. `df[mask]` is typed boolean indexing. |
| `assign(...)` | `PandasLikeFrame.assign(...)` | Typed-parity | Lowered to `with_columns` / `with_column` plan ops. |
| `query(...)` | `PandasLikeFrame.query(...)` | Divergence | Typed predicate only; no string expression parser. |
| `sort_values(...)` | `PandasLikeFrame.sort_values(...)` | Typed-parity | Lowered to core `sort`. |
| `drop(...)` | `PandasLikeFrame.drop(...)` | Divergence | Column drop only; no index semantics. |
| `rename(...)` | `PandasLikeFrame.rename_pandas(columns=..., errors=...)` | Typed-parity | Pandas-flavored API exposed via `rename_pandas` (core `rename` remains PlanFrame-typed). |
| `dropna(...)` / `fillna(...)` | `PandasLikeFrame.dropna(...)` / `fillna(...)` | Divergence | Lowered to `drop_nulls` / `fill_null`; no `axis='columns'` support. |
| `merge(...)` | `PandasLikeFrame.merge(...)` | Divergence | Lowered to `join`; join key retention differs from pandas in some cases. |
| IO (`to_parquet`, `to_csv`, ...) | `Frame.sink_*` | Divergence | PlanFrame prefers `sink_*` names (lazy boundary). |

## Major “pandas parity” gaps (expected)

- **Index semantics**: `.loc`, `.iloc`, index-aligned arithmetic, `drop(axis=0)`, etc.
- **Mutation**: `df["x"] = ...`
- **String expression systems**: `query("x > 1")`, `eval("...")`
- **Arbitrary UDFs**: `apply`, `map`, etc.

These can be addressed only via an explicit “compat mode” (weaker typing) or separate frontend classes.

## Where this is implemented

- pandas frontend skin: `packages/planframe/planframe/pandas/frame.py`, `packages/planframe/planframe/pandas/series.py`
- pandas backend: `packages/planframe-pandas/planframe_pandas/frame.py`, `packages/planframe-pandas/planframe_pandas/adapter.py`
