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

