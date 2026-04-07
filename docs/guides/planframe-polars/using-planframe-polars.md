# Using `planframe-polars`

This guide covers the intended public usage pattern:

- define a schema as a **`PolarsFrame` subclass**
- construct frames from **Python-native data** (PlanFrame constructs Polars internally)
- chain transforms (always lazy)
- execute via boundaries (`collect`, `to_dicts`, `to_dict`, `collect(kind=...)`)

## Quickstart

Run:

```bash
./.venv/bin/python docs/guides/planframe-polars/examples/basic_usage.py
```

Expected output:

```text
columns=['id', 'full_name', 'age_plus_one']
to_dict={'id': [1], 'full_name': ['a'], 'age_plus_one': [11]}
rows=[{'id': 1, 'full_name': 'a', 'age_plus_one': 11}]
row_models=[('Row', 1, 'a', 11)]
```

## Construction rules

- **Do**: `User({"id": [1], "name": ["a"], "age": [10]})`
- **Do**: `User([{"id": 1, "name": "a", "age": 10}])`
- **Don’t**: pass `polars.DataFrame` / `polars.LazyFrame` directly into `User(...)`

If you need advanced construction, use `Frame.source(...)` with a backend frame (this is intentionally “escape hatch” territory).

## Defaults for missing columns

If your schema defines defaults, PlanFrame will fill **missing input keys/columns** on construction.

Example:

```python
class User(PolarsFrame):
    id: int
    name: str
    age: int
    active: bool = True
```

If the input data omits `active`, it will be filled with `True` for all rows.

