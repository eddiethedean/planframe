"""
Temporary shim for Pyright's bundled typeshed.

Pyright 1.1.408's typeshed fallback references `annotationlib` from `typing` /
`typing_extensions`, but the module isn't present in the fallback set shipped
with the package in this repo's environment.

This stub exists only to unblock the typing regression suite under `tests/pyright/`.
"""

from __future__ import annotations

