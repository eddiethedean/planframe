#!/usr/bin/env bash
# Build wheels and source distributions for all publishable packages into ./dist/
# (directory is gitignored). Used locally and by .github/workflows/publish-pypi.yml
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
rm -rf dist
mkdir -p dist
for p in packages/planframe packages/planframe-polars packages/planframe-pandas packages/planframe-sparkless; do
  uv build "$p" --out-dir dist
done
echo "Built:"
ls -la dist
