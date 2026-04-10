# Adapter documentation template

Copy this folder as a starting point for a new adapter track.

## Overview

- What backend does this adapter target?
- Is it lazy, eager, or hybrid?
- What PlanFrame features are supported (joins, grouping, pivot, I/O, etc.)?

## Installation

```bash
pip install <your-adapter-package>
```

## Adapter author checklist (recommended)

- Run PlanFrame’s published conformance helper in your CI:
  - Docs: `https://planframe.readthedocs.io/en/latest/planframe/guides/adapter-conformance/`
  - API: `planframe.adapter_conformance.run_minimal_adapter_conformance`
- Set conservative capability flags via `BaseAdapter.capabilities` (see core docs and `AdapterCapabilities`).

## Quickstart

Show a minimal example:

- define a schema
- construct a frame
- run 2–3 transforms
- collect / materialize

See the official adapters for concrete examples:

- `planframe-polars`
- `planframe-pandas`
- `planframe-sparkless` (Spark UI + `sparkless` engine)

## Backend notes

- dtype mapping expectations
- null behavior
- join semantics differences (if any)
- performance notes

