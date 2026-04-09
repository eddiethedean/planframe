# Generated interface inventory (pandas parity)

This file is generated from source. It inventories the pandas-like skin surface and attempts to
show parent-interface signatures when available in the local environment.

## `PandasLikeFrame` vs `pandas.DataFrame`

| Method | Our signature | Parent | Parent signature | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| `assign` | `def assign(self, **columns)` | `pandas.DataFrame` | `(self, **kwargs) -> 'DataFrame'` | **typed-parity** | — |
| `astype` | `def astype(self, dtype, errors)` | `pandas.DataFrame` | `(self, dtype, copy: 'bool_t | None' = None, errors: 'IgnoreRaise' = 'raise') -> 'Self'` | **typed-parity** | — |
| `columns` | `def columns(self)` | `pandas.DataFrame` | — | **typed-parity** | — |
| `drop` | `def drop(self, *args, strict, columns, axis, errors)` | `pandas.DataFrame` | `(self, labels: 'IndexLabel | None' = None, *, axis: 'Axis' = 0, index: 'IndexLabel | None' = None, columns: 'IndexLabel | None' = None, level: 'Level | None' = None, inplace: 'bool' = False, errors: 'IgnoreRaise' = 'raise') -> 'DataFrame | None'` | **divergence** | columns-only; no index semantics |
| `drop_duplicates` | `def drop_duplicates(self, *subset, keep, maintain_order, **kwargs)` | `pandas.DataFrame` | `(self, subset: 'Hashable | Sequence[Hashable] | None' = None, *, keep: 'DropKeep' = 'first', inplace: 'bool' = False, ignore_index: 'bool' = False) -> 'DataFrame | None'` | **typed-parity** | — |
| `dropna` | `def dropna(self, axis, how, thresh, subset)` | `pandas.DataFrame` | `(self, *, axis: 'Axis' = 0, how: 'AnyAll | lib.NoDefault' = <no_default>, thresh: 'int | lib.NoDefault' = <no_default>, subset: 'IndexLabel | None' = None, inplace: 'bool' = False, ignore_index: 'bool' = False) -> 'DataFrame | None'` | **divergence** | lowered to core ops; eager/index semantics differ |
| `eval` | `def eval(self, **columns)` | `pandas.DataFrame` | `(self, expr: 'str', *, inplace: 'bool' = False, **kwargs) -> 'Any | None'` | **typed-parity** | — |
| `fillna` | `def fillna(self, value, subset)` | `pandas.DataFrame` | `(self, value: 'Hashable | Mapping | Series | DataFrame | None' = None, *, method: 'FillnaOptions | None' = None, axis: 'Axis | None' = None, inplace: 'bool_t' = False, limit: 'int | None' = None, downcast: 'dict | None | lib.NoDefault' = <no_default>) -> 'Self | None'` | **divergence** | lowered to core ops; eager/index semantics differ |
| `filter` | `def filter(self, *predicates, items, like, regex)` | `pandas.DataFrame` | `(self, items=None, like: 'str | None' = None, regex: 'str | None' = None, axis: 'Axis | None' = None) -> 'Self'` | **typed-parity** | — |
| `head` | `def head(self, n)` | `pandas.DataFrame` | `(self, n: 'int' = 5) -> 'Self'` | **typed-parity** | — |
| `merge` | `def merge(self, right, how, on, left_on, right_on, suffixes, options)` | `pandas.DataFrame` | `(self, right: 'DataFrame | Series', how: 'MergeHow' = 'inner', on: 'IndexLabel | AnyArrayLike | None' = None, left_on: 'IndexLabel | AnyArrayLike | None' = None, right_on: 'IndexLabel | AnyArrayLike | None' = None, left_index: 'bool' = False, right_index: 'bool' = False, sort: 'bool' = False, suffixes: 'Suffixes' = ('_x', '_y'), copy: 'bool | None' = None, indicator: 'str | bool' = False, validate: 'MergeValidate | None' = None) -> 'DataFrame'` | **divergence** | lowered to core ops; eager/index semantics differ |
| `nlargest` | `def nlargest(self, n, columns, keep)` | `pandas.DataFrame` | `(self, n: 'int', columns: 'IndexLabel', keep: 'NsmallestNlargestKeep' = 'first') -> 'DataFrame'` | **typed-parity** | — |
| `nsmallest` | `def nsmallest(self, n, columns, keep)` | `pandas.DataFrame` | `(self, n: 'int', columns: 'IndexLabel', keep: 'NsmallestNlargestKeep' = 'first') -> 'DataFrame'` | **typed-parity** | — |
| `query` | `def query(self, expr)` | `pandas.DataFrame` | `(self, expr: 'str', *, inplace: 'bool' = False, **kwargs) -> 'DataFrame | None'` | **divergence** | typed predicate only; no string expression parser |
| `rename` | `def rename(self, mapping, strict, **named)` | `pandas.DataFrame` | `(self, mapper: 'Renamer | None' = None, *, index: 'Renamer | None' = None, columns: 'Renamer | None' = None, axis: 'Axis | None' = None, copy: 'bool | None' = None, inplace: 'bool' = False, level: 'Level | None' = None, errors: 'IgnoreRaise' = 'ignore') -> 'DataFrame | None'` | **typed-parity** | — |
| `rename_pandas` | `def rename_pandas(self, columns, errors)` | `pandas.DataFrame` | — | **typed-parity** | — |
| `sort_values` | `def sort_values(self, by, ascending, na_position)` | `pandas.DataFrame` | `(self, by: 'IndexLabel', *, axis: 'Axis' = 0, ascending: 'bool | list[bool] | tuple[bool, ...]' = True, inplace: 'bool' = False, kind: 'SortKind' = 'quicksort', na_position: 'str' = 'last', ignore_index: 'bool' = False, key: 'ValueKeyFunc | None' = None) -> 'DataFrame | None'` | **typed-parity** | — |
| `tail` | `def tail(self, n)` | `pandas.DataFrame` | `(self, n: 'int' = 5) -> 'Self'` | **typed-parity** | — |
