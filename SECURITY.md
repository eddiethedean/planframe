# Security policy

## Supported versions

Security fixes are applied to the **latest minor release** of maintained packages in this repository:

- `planframe`, `planframe-polars`, and `planframe-pandas` (released together; see `CHANGELOG.md`)
- `planframe-sparkless` (independent versioning)

We recommend using **Python 3.10+** as declared in each package’s `requires-python`.

Older releases may not receive backports unless a serious issue affects supported dependency ranges.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for undisclosed security vulnerabilities.

Instead:

1. Open a **private security advisory** on GitHub:  
   `https://github.com/eddiethedean/planframe/security/advisories/new`  
   (Repository: **Security** tab → **Report a vulnerability**.)

2. Or email the maintainers if that form is unavailable (use the contact path you already use for this repo, if any).

Include:

- A short description of the issue and its impact
- Steps to reproduce (proof-of-concept or scenario), if possible
- Affected versions or components (e.g. `planframe`, a specific adapter, or a script)
- Whether you need coordinated disclosure or a preferred timeline

We aim to acknowledge reports within a few business days and will work with you on severity, fix, and disclosure.

## What we do in this repository

- **Dependency scanning**: locked dependencies are audited with [`pip-audit`](https://pypi.org/project/pip-audit/) against the PyPI advisory database. The audit uses a `uv export` of the root lockfile (dev and docs extras, dependency groups, excluding the editable project line and requirement hashes so `pip-audit` can resolve wheels). See `scripts/audit-deps.sh` and the **Security** GitHub Actions workflow. When releasing wheels from `packages/`, also review runtime dependencies declared in each package’s `pyproject.toml`.
- **Code quality**: Ruff, `ty`, tests, and typing checks run in development; they are not a substitute for a full application security review in your deployment.

## Supply chain and deployment

- Prefer **pinned or locked** dependencies in production (`uv.lock`, `pip-tools`, or your org’s standard).
- Review **adapter and optional** dependencies (Polars, pandas, sparkless, cloud connectors) under your own threat model.
- PlanFrame does not execute arbitrary user Python as part of core plan execution; still validate **untrusted inputs** (paths, connection strings, SQL passed to backends) in your application layer.

## Coordinated disclosure

We ask that reporters allow reasonable time for a fix before public disclosure. We will credit reporters in release notes if they wish.
