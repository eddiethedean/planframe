## planframe-pandas

[![Docs](https://readthedocs.org/projects/planframe/badge/?version=latest)](https://planframe.readthedocs.io/en/latest/planframe_pandas/)
[![PyPI](https://img.shields.io/pypi/v/planframe-pandas)](https://pypi.org/project/planframe-pandas/)
[![License: MIT](https://img.shields.io/badge/License-MIT-informational)](../../LICENSE)

pandas adapter package for PlanFrame. Import as `planframe_pandas`.

Documentation (ReadTheDocs):

- pandas track (end users): `https://planframe.readthedocs.io/en/latest/planframe_pandas/`
- Light API reference: `https://planframe.readthedocs.io/en/latest/planframe_pandas/reference/api/`

### Notes

- **`PandasFrame`** subclasses core **`planframe.pandas.PandasLikeFrame`**, so you get pandas-flavored helpers (`assign`, `sort_values`, boolean indexing, column `filter`, `astype`, `eval`, `drop_duplicates`, …) on top of the shared `Frame` plan. See the [pandas-like API](https://planframe.readthedocs.io/en/latest/planframe/guides/pandas-like-api/) guide.
- PlanFrame is **always lazy**: chaining does not touch backend data; execution happens at `collect()` boundaries.
- `collect()` returns `list[pydantic.BaseModel]`. Use `collect_backend()` for a `pandas.DataFrame`, or `stream_dicts()` / `stream()` to iterate rows (see [Streaming rows](https://planframe.readthedocs.io/en/latest/planframe/guides/streaming-rows/)).

### Common transforms

- `with_row_index(name="row_nr", offset=0)`: add a monotonically increasing row number column.
- `clip(lower=..., upper=..., subset=...)`: clamp numeric columns (if `subset=None`, clamps all numeric schema fields).
- `cast_many` / `cast_subset`: multi-column cast helpers.
- `fill_null_many` / `fill_null_subset`: multi-column fill-null helpers.
- `rename_upper/lower/title/strip(...)`: schema-driven rename helpers.
- `pivot_longer(...)` / `pivot_wider(...)`: reshape convenience wrappers around `melt` / `pivot`.

