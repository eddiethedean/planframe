"""Minimal conformance suite for third-party :class:`~planframe.backend.adapter.BaseAdapter` implementations.

Run :func:`run_minimal_adapter_conformance` from adapter CI to get a pass/fail signal with
named cases (select/filter, projections, sort, group_by/agg, optional join).

Install optional dev deps: ``pip install planframe[adapter-dev]`` (includes pytest for local runs).
"""

from __future__ import annotations

from planframe.adapter_conformance.suite import (
    ConformanceCase,
    ConformanceResult,
    run_minimal_adapter_conformance,
)

__all__ = [
    "ConformanceCase",
    "ConformanceResult",
    "run_minimal_adapter_conformance",
]
