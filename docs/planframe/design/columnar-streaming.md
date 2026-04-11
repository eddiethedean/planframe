# Columnar streaming (chunked export) — design note

This note describes an **optional** adapter-facing protocol for **chunked columnar** export. It is a *north star* for adapter authors; full integration with `Frame.to_dict` / `planframe.materialize` may land in a later minor release.

## Motivation

[`materialize_columns`](../guides/creating-an-adapter.md#columnar-boundary-materialize) and `Frame.to_dict` return a single `dict[str, list[object]]` holding **all** rows. For very large results, hosts may need **streaming** or **chunked** columnar batches without materializing the full dict at once, while still forwarding [`ExecutionOptions`](../reference/api.md) hints such as `streaming` / `engine_streaming`.

## Relationship to row streaming

| Mechanism | Shape | Integrated with `Frame` today? |
| --- | --- | --- |
| `AdapterRowStreamer` | Iterator of **row** dicts (`dict[str, object]` per row) | Yes — `Frame.stream_dicts` / `astream_dicts` when both sync and async methods are implemented |
| **`AdapterColumnarStreamer`** | Iterator of **columnar** chunks (`dict[str, list[object]]` per chunk; lists same length within a chunk) | **No** — protocol only; hosts call the adapter after `collect` / `acollect` |

Row streaming and columnar chunking solve different consumption patterns (record-at-a-time vs batch column builders). See [Streaming rows](../guides/streaming-rows.md) for row-oriented streaming.

## Protocol sketch

Defined in **`planframe.backend.io`** as **`AdapterColumnarStreamer`**:

- **`iter_columnar_chunks(df, *, options=...)`** — sync iterator of columnar chunks.
- **`aiter_columnar_chunks(df, *, options=...)`** — async iterator of the same chunk shape.

**Contract:** implement **both** if you claim support (same rule as `AdapterRowStreamer`).

**Semantics:**

- Column names should be **stable** across chunks for one materialization.
- Each chunk’s value lists are **aligned** (same row count per chunk).
- Concatenating lists per column across chunks (in order) should reproduce the full columnar result you would have obtained from `to_dict` for that plan — unless the adapter documents a different contract (e.g. keyed batches).

## `ExecutionOptions`

Reuse existing hints; PlanFrame does not assign new meaning here beyond “forward what your engine understands”:

- `streaming` / `engine_streaming` may influence batch size or engine execution mode where supported.

## Recommended host pattern (today)

Until core wires `AdapterColumnarStreamer` into `materialize_*`:

1. Evaluate the plan: `planned = frame.collect_backend(options=options)` or `await frame.acollect_backend(...)`.
2. If `isinstance(frame._adapter, AdapterColumnarStreamer)`, call `iter_columnar_chunks(planned, options=options)` or `async for chunk in adapter.aiter_columnar_chunks(...)`.
3. Otherwise fall back to `frame.to_dict(options=options)` or `materialize_columns`.

Keep `options=` identical to what you would pass to `to_dict` so behavior stays comparable.

## Future work (phased)

- Optional `Frame` / `materialize_*` entrypoints that delegate to `AdapterColumnarStreamer` when present.
- Stronger typing for chunk invariants (optional `TypedDict` helpers).

## See also

- [Creating an adapter — Columnar boundary helpers](../guides/creating-an-adapter.md#columnar-boundary-materialize)
