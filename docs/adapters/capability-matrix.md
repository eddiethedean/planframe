# Adapter capability matrix

This matrix summarizes the **current** behavior of the official adapters.

Notes:

- PlanFrame is **always lazy** (plans execute at materialization boundaries).
- “Streaming” below refers to **true row streaming** via `AdapterRowStreamer`. If an adapter does not qualify, `stream_dicts()` / `astream_dicts()` still exist but will **materialize then yield**.

## Official adapters

| Adapter | Package | Backend | Primary audience |
| --- | --- | --- | --- |
| Polars | `planframe-polars` | `polars` | End users |
| pandas | `planframe-pandas` | `pandas` | End users |
| sparkless | `planframe-sparkless` | `sparkless` | End users (Spark-like UI without JVM) |

## Core execution boundaries

| Capability | `planframe-polars` | `planframe-pandas` | `planframe-sparkless` |
| --- | --- | --- | --- |
| `collect()` (Pydantic rows) | Yes | Yes | Yes |
| `collect_backend()` (native frame) | Yes (`polars`) | Yes (`pandas`) | Yes (`sparkless`) |
| `to_dicts()` / `to_dict()` | Yes | Yes | Yes |
| Async materialization (`acollect`/`ato_*`) | Yes (default hooks or adapter overrides) | Yes (default hooks or adapter overrides) | Yes (default hooks or adapter overrides) |

## Row streaming

| Capability | `planframe-polars` | `planframe-pandas` | `planframe-sparkless` |
| --- | --- | --- | --- |
| `stream_dicts()` exists | Yes | Yes | Yes |
| True streaming (`AdapterRowStreamer`) | Backend-defined | Backend-defined | Not currently (falls back to `to_dicts`) |
| `astream_dicts()` exists | Yes | Yes | Yes |

## Semantics & known limitations

| Area | Notes |
| --- | --- |
| Null ordering in sort | Backend-specific. sparkless adapter maps PlanFrame `nulls_last` to Spark-style null ordering when sorting. |
| Empty exports | Adapters should return stable shapes for `to_dict()`; sparkless returns `{col: []}` for empty frames. |
| Join keys | Polars and sparkless support symmetric `on=` and asymmetric `left_on`/`right_on` joins. sparkless prefers `on=` / `left_on`/`right_on` to avoid ambiguous column references. |

If you need a stronger guarantee for a specific capability, treat this page as a starting point and consult:

- The adapter’s track in the docs
- The parity matrices under each adapter’s Design section (where available)

