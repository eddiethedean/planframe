# Interface parity: PlanFrame pandas skin vs pandas `DataFrame`

This page compares PlanFrame’s **pandas-flavored interface** (`planframe.pandas.PandasLikeFrame`) to `pandas.DataFrame`.

PlanFrame is **always lazy** and (in the typed surface) **index-free**, so parity is primarily about **API shape** and common call patterns, not exact eager/index semantics.

## Legend

- **Exact parity**: same name + same argument shape + same behavior (rare for pandas).
- **Typed-parity**: same shape, but restricted to typed-safe subsets.
- **Divergence**: present but intentionally differs (documented).
- **Unsupported**: not implemented; should fail loudly.

## High-value mapping

| pandas | PlanFrame pandas skin | Status | Notes |
| --- | --- | --- | --- |
| `df["col"]` | `__getitem__(str)` | Typed-parity | Returns a typed `Series` expression wrapper, not data. |
| `df[["a","b"]]` | `__getitem__(Sequence[str])` | Typed-parity | Lowered to `Frame.select`. |
| boolean indexing | `__getitem__(Series[bool] \| Expr[bool])` | Typed-parity | Lowered to `query(...)` (typed predicate). |
| `assign(...)` | `assign(...)` | Typed-parity | Lowered to repeated `with_columns(...)`. |
| `sort_values(...)` | `sort_values(...)` | Typed-parity | Lowered to core `sort`. |
| `drop(columns=...)` | `drop(...)` | Divergence | Columns-only; `axis="index"` is unsupported. |
| `rename(columns=..., errors=...)` | `rename_pandas(...)` | Typed-parity | Mirrors pandas argument shape; lowered to core `rename`. |
| `dropna(...)` | `dropna(...)` | Divergence | Row-wise only; lowered to core `drop_nulls(subset=...)`. |
| `fillna(...)` | `fillna(...)` | Divergence | Lowered to typed `fill_null`/`fill_null_many`. |
| `query("...")` | `query(Series[bool] \| Expr[bool])` | Divergence | No string expression parser in typed surface. |
| `filter(...)` | `filter(predicate)` / `filter(items/like/regex)` | Divergence | Dual semantics (row filter vs column selection), chosen by arguments. |
| `merge(...)` | `merge(...)` | Divergence | Lowered to core `join`; index semantics differ. |

## Major gaps (expected)

- **Index semantics**: `.loc`, `.iloc`, alignment, `axis=0` drop, etc.
- **Mutation**: assignment (`df["x"]=...`) and inplace ops.
- **String expression systems**: pandas `query("...")` and `eval("...")`.
- **Arbitrary UDFs**: `apply`, `map`, Python lambdas over rows/columns.

