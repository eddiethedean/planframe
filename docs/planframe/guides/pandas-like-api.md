# Pandas-like API (`planframe.pandas`)

PlanFrame includes an optional **pandas-flavored API skin** (no pandas dependency) under the
`planframe.pandas` submodule.

This is a **naming/ergonomics layer** only: PlanFrame remains **lazy**, schema-driven, and executes
through adapters at `collect()`.

## Imports

```python
from planframe.pandas import PandasLikeFrame
from planframe_polars import PolarsFrame


class User(PolarsFrame, PandasLikeFrame):
    id: int
    name: str


df = User({"id": [1], "name": ["a"]})
out = (
    df.assign(id2=df["id"] + 1)
    .sort_values("name")
    .rename(columns={"name": "full_name"})
)
rows = out.to_dicts()
```

## Supported subset (initial)

- `df["col"]` returns an **expression-like** `Series` wrapper (not data)
- `df[["a", "b"]]` is a shorthand for `select("a", "b")`
- `df[mask]` where `mask` is a `Series[bool]` is typed boolean indexing sugar for `query(mask)`
- `assign(...)`
- `query(<typed predicate>)` (accepts `Series[bool]` / `Expr[bool]`)
- `filter(items=... | like=... | regex=...)` (column selection helper)
- `astype({...}, errors=...)`
- `eval(**columns)` (typed alias of `assign`, not string eval)
- `drop_duplicates(...)`
- `sort_values(by, ascending=..., na_position=...)`
- `rename(columns=..., errors=...)`
- `drop(columns=..., errors=...)` (column drop only; no index semantics)
- `merge(...)` (lowered to PlanFrame `join`)
- `dropna(...)` and `fillna(...)` (lowered to PlanFrame `drop_nulls` / `fill_null`)

## Limits

- No pandas index semantics (`loc`, `iloc`, index-aligned ops, `drop(axis=0)`, etc.)
- No arbitrary Python UDFs (`apply`, `map`, ...)
- No string-expression parser (`query("x > 1")`) in this initial version

