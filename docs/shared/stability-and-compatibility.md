# Stability & compatibility

This page documents what you can rely on when using PlanFrame packages in production.

## Supported Python versions

All packages currently target **Python 3.10+** (see each package’s `requires-python`).

## Packages and versioning

This repository ships multiple Python packages:

- **`planframe`**: core planning layer (backend-agnostic)
- **`planframe-polars`**: Polars execution adapter
- **`planframe-pandas`**: pandas execution adapter
- **`planframe-sparkless`**: sparkless execution adapter (Spark-like UI on `sparkless`)

Versioning policy:

- `planframe`, `planframe-polars`, and `planframe-pandas` are released together at the **same version**.
- `planframe-sparkless` is **versioned independently**.

## API stability (what is “public API”)

The “public API” is:

- The `Frame`/`Expr` surface exposed by stable imports (e.g. `from planframe import Frame`, `from planframe import expr`)
- Adapter package entrypoints (e.g. `planframe_polars.PolarsFrame`, `planframe_pandas.PandasFrame`, `planframe_sparkless.SparklessFrame`)
- Documented contracts and behaviors in the guides (materialization boundaries, join hint forwarding, streaming hook contract, etc.)

Internal modules (anything not referenced by docs or stable imports) may change between releases.

## Deprecation policy

When changing public API, we aim to:

- **Deprecate first** (warning + doc note), then remove in a later release
- Provide a short upgrade note in the changelog and/or migration guide

If a breaking change is required, it should be reflected by SemVer (major bump for the affected package set).

## Compatibility guarantees and contracts

### Laziness

PlanFrame is **always lazy**: chaining transformations builds a plan; work runs at explicit execution boundaries.

### Materialization boundaries

Supported execution boundaries include:

- `collect()` / `collect_backend()`
- `to_dicts()` / `to_dict()`
- `stream_dicts()` / `stream()`
- Async equivalents: `acollect()` / `ato_dicts()` / `ato_dict()` / `astream_dicts()` / `astream()`

### Async behavior

Adapters may provide true async implementations by overriding async hooks on `BaseAdapter`.
If they do not, PlanFrame’s defaults wrap synchronous methods using `asyncio.to_thread`.

### Row streaming contract

If an adapter implements `AdapterRowStreamer`, it must provide **both**:

- `stream_dicts(...) -> Iterator[dict[str, object]]`
- `astream_dicts(...) -> AsyncIterator[dict[str, object]]`

Adapters that provide only the sync method are treated as non-streaming and will fall back to materialize-then-yield behavior.

