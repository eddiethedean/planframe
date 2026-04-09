# Streaming rows

PlanFrame is lazy-first: it builds a plan, then executes only at materialization boundaries.

This guide explains the **row-streaming** APIs:

- **`stream_dicts()`** / **`astream_dicts()`**: yield rows as `dict[str, object]`
- **`stream(name=...)`** / **`astream(name=...)`**: yield rows as schema-derived `pydantic.BaseModel`

## Why streaming exists (and what it does not guarantee)

`stream_*` is a convenience API for *iterating* rows.

- If the adapter implements **`AdapterRowStreamer`**, PlanFrame can stream rows without building an intermediate `list[dict]`.
- If not, PlanFrame falls back to **`to_dicts()` / `ato_dicts()`** internally and yields from the materialized list.

In other words:

- **Streaming API is additive** (always available).
- **True streaming is adapter-defined** (only available when the adapter provides it).

## `to_dicts()` vs `stream_dicts()`

Choose based on what you need:

- **`to_dicts()`**: you want a `list[dict]` immediately (e.g., testing, small outputs).
- **`stream_dicts()`**: you want an iterator of rows (e.g., piping rows to another system, early exit).

## Examples

### Polars

```python
from planframe_polars.frame import PolarsFrame as PF

pf = PF.scan_parquet("s3://bucket/data/*.parquet")

# Stream dict rows (adapter may use engine streaming where supported)
for row in pf.select("id", "age").stream_dicts():
    if row["age"] > 100:
        print(row)
        break

# Stream Pydantic row models derived from the current schema
for row in pf.select("id", "age").stream(name="UserRow"):
    print(row.id, row.age)
```

### Pandas

```python
from planframe_pandas.frame import PandasFrame as PF

pf = PF.read_csv("data.csv")

for row in pf[["id", "age"]].stream_dicts():
    print(row)
```

## Adapter authors: implementing true streaming

To enable true streaming, implement `AdapterRowStreamer` on your adapter (in addition to the normal `to_dicts` export).

Conceptually:

- `stream_dicts(df, ...) -> Iterator[dict[str, object]]`
- `astream_dicts(df, ...) -> AsyncIterator[dict[str, object]]`

PlanFrame will prefer these hooks when present.

