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
 
