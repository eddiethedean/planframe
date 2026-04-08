"""PlanFrame public entry points.

These re-exports are intended to be stable, discoverable imports for end-users.
For everything else, prefer importing from the submodules directly.
"""

from __future__ import annotations

import importlib
from typing import Any

from planframe.dynamic_groupby import DynamicGroupedFrame
from planframe.execution import execute_plan
from planframe.frame import Frame
from planframe.groupby import GroupedFrame
from planframe.plan.join_options import JoinOptions
from planframe.schema.ir import Schema
from planframe.selector import ColumnSelector


def __getattr__(name: str) -> Any:
    # Lazily expose `planframe.expr` to avoid import-time cycles.
    if name == "expr":
        return importlib.import_module("planframe.expr")
    raise AttributeError(name)


__all__ = [
    "Frame",
    "Schema",
    "GroupedFrame",
    "DynamicGroupedFrame",
    "ColumnSelector",
    "JoinOptions",
    "execute_plan",
    "expr",
]
