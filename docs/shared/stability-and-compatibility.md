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

## Typing stability (stubs + generics)

PlanFrame treats **static typing behavior** as part of the public API. This matters because downstream adapters may pin PlanFrame and re-export its types (notably `Frame`).

### What counts as “typing public API”

- **Generated `.pyi` stubs shipped in the wheels**, especially:
  - `planframe/frame/__init__.pyi` (generated `Frame` surface)
  - `planframe/typing/_schema_types.pyi`
- **Public generics used at adapter boundaries**, including:
  - `Frame[SchemaT, BackendFrameT, BackendExprT]` (and any other exported type parameters on `Frame`)
  - `Expr[T]` and other exported typing helpers referenced by docs or stable imports
- **Typing-only constraints that are intentionally enforced**, e.g. “must be a known `Literal[...]`” rules in the typed API (these are validated in CI by the Pyright suite under `tests/pyright/`).

### SemVer policy for typing changes

In the rules below, “breaking” means **a reasonable downstream user’s previously-valid code now fails type checking** with the same settings (Pyright strictness, etc.).

- **Patch releases (\(x.y.Z\))**
  - Allowed: typing bug fixes that are clearly incorrect behavior (e.g. missing overloads, `Any` leakage fixes) *as long as they don’t commonly break valid code*.
  - Not allowed: intentional tightening that removes valid programs from the accepted set. If in doubt, treat it as **minor** or document as a breaking change.
- **Minor releases (\(x.Y.z\))**
  - Allowed: additive typing improvements (new overloads, new typed methods, better inference) and small tightening when justified.
  - Required: call out **user-visible typing changes** in the changelog, especially if they can cause new diagnostics for previously-passing code.
- **Major releases (\(X.y.z\))**
  - Allowed: breaking typing changes, including changes to `Frame`/`Expr` generic parameters, overload strategy, and stub structure.
  - Required: migration notes for adapter authors and any widely-used patterns.

### CHANGELOG expectations

When a release changes shipped stubs or any public generic surface:

- include a **“Typing”** bullet (or a dedicated “Typing” section) describing what changed and why
- for breaking or potentially-breaking changes, include a short **upgrade note** (what to change downstream)

### CI contract: what `generate_typing_stubs.py --check` means

CI runs `python scripts/generate_typing_stubs.py --check`, which verifies that the **committed generated stubs match the generator templates** for this repo version. It does **not** guarantee that stubs will remain byte-for-byte stable across releases—only that changes are intentional and reviewed.

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

