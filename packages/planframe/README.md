## planframe

[![Docs](https://readthedocs.org/projects/planframe/badge/?version=latest)](https://planframe.readthedocs.io/en/latest/planframe/)
[![PyPI](https://img.shields.io/pypi/v/planframe)](https://pypi.org/project/planframe/)
[![License: MIT](https://img.shields.io/badge/License-MIT-informational)](../../LICENSE)

Core package for PlanFrame (typed planning layer). Import as `planframe`.

Documentation (ReadTheDocs):

- Core (adapter authors): `https://planframe.readthedocs.io/en/latest/planframe/`
- Design docs: `https://planframe.readthedocs.io/en/latest/planframe/design/`
- Light API reference: `https://planframe.readthedocs.io/en/latest/planframe/reference/api/`

### What you get
- `planframe.Frame`: immutable, schema-aware transformation plan (**always lazy**)
- `planframe.expr`: typed expression IR (`col`, `lit`, arithmetic/compare/boolean ops, `coalesce`, `if_else`, etc.), plus **aggregation wrappers** for use inside `group_by(...).agg(...)`: `agg_sum`, `agg_mean`, `agg_min`, `agg_max`, `agg_count`, `agg_n_unique` (these build `AggExpr` nodes)
- `planframe.groupby.GroupedFrame`: produced by `Frame.group_by`; **`group_by`** accepts column names and/or expressions (expression keys show up as `__pf_g0`, `__pf_g1`, … in the result schema). **`agg`** accepts `(op, column)` tuples and/or `AggExpr` values—not arbitrary bare expressions
- `planframe.schema`: schema reflection (dataclass + Pydantic) and materialization

### Common transforms

Some commonly used Frame transforms:

- `with_row_count(name="row_nr", offset=0)`: add a monotonically increasing row number column.
- `clip(lower=..., upper=..., subset=...)`: clamp numeric columns (if `subset=None`, clamps all numeric schema fields).
- `drop_nulls(*columns, how="any"|"all", threshold=...)`: drop rows by null pattern over a column subset.
- `select_schema(selector, strict=True)`: schema-only selectors (backend-independent); `ColumnSelector` is runtime-checkable.
- `cast_many(mapping, strict=True)` / `cast_subset(*columns, dtype, strict=True)`: multi-column cast helpers.
- `fill_null_subset(value|strategy, *columns)` / `fill_null_many(mapping, strict=True)`: multi-column fill-null helpers.
- `rename_upper/lower/title/strip(...)`: schema-driven rename helpers.
- `pivot_longer(...)` / `pivot_wider(...)`: reshape convenience wrappers around `melt` / `pivot`.

Materialization accepts optional **`ExecutionOptions`** on `collect` / `to_dicts` / `to_dict` (and async counterparts). **`JoinOptions`** on `Frame.join` carries execution hints (including `engine_streaming` where the backend supports it).

### Note on backends
`planframe` is backend-agnostic. It does not execute anything until `collect()` (even for eager backends). To execute plans you need an adapter package (e.g. `planframe-polars`).

For async stacks, `Frame.acollect()`, `Frame.ato_dicts()`, and `Frame.ato_dict()` await adapter hooks (`BaseAdapter.acollect` and friends); defaults run sync methods in a thread pool. See `https://planframe.readthedocs.io/en/latest/planframe/design/backend-adapter-design/`.

### Typing
PlanFrame includes `py.typed` plus generated stubs (notably `planframe/frame.pyi`) to improve static typing in editors and Pyright.

If you modify the `Frame` API, regenerate stubs from the repo root:

```bash
python scripts/generate_typing_stubs.py
python scripts/generate_typing_stubs.py --check
```
 
