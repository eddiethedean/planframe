#!/usr/bin/env bash
# Audit locked dependencies for known vulnerabilities using pip-audit + uv export.
# Requires: uv, pip-audit (install via `pip install -e ".[dev]"`).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
TMPREQ="$(mktemp)"
cleanup() { rm -f "$TMPREQ"; }
trap cleanup EXIT

# Omit hashes and the editable root line so pip-audit can install from the file (hashes + -e are incompatible).
# Match CI: `uv sync --all-extras --group dev` (root project is named `planframe` in uv.lock).
uv export --frozen --format requirements-txt -o "$TMPREQ" \
  --package planframe \
  --all-extras \
  --all-groups \
  --no-hashes \
  --no-emit-project \
  >/dev/null
if command -v uv >/dev/null 2>&1; then
  uv run pip-audit -r "$TMPREQ" "$@"
else
  pip-audit -r "$TMPREQ" "$@"
fi
