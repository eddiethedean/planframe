"""Pandas-flavored API skin for PlanFrame.

This module provides a small subset of pandas-like naming (`assign`, `sort_values`, `merge`, ...)
while preserving PlanFrame semantics (lazy plans + adapter execution).
"""

from __future__ import annotations

from .frame import PandasLikeFrame
from .series import Series

__all__ = ["PandasLikeFrame", "Series"]
