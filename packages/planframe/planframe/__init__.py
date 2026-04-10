"""PlanFrame public entry points.

These re-exports are intended to be stable, discoverable imports for end-users.
For everything else, prefer importing from the submodules directly.
"""

from __future__ import annotations

import importlib
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version
from typing import Any

from planframe.dynamic_groupby import DynamicGroupedFrame
from planframe.execution import execute_plan
from planframe.execution_options import ExecutionOptions
from planframe.frame import Frame
from planframe.groupby import GroupedFrame
from planframe.ir_versions import EXPR_IR_VERSION, PLAN_IR_VERSION
from planframe.plan.join_options import JoinOptions
from planframe.schema.ir import Schema
from planframe.selector import ColumnSelector


def _get_version() -> str:
    try:
        return _dist_version("planframe")
    except PackageNotFoundError:
        # Editable installs or unusual envs may not have dist metadata available.
        return "0+unknown"


__version__: str = _get_version()


def __getattr__(name: str) -> Any:
    # Lazily expose `planframe.expr` / `planframe.spark` / `planframe.pandas` to avoid import-time cycles.
    if name == "expr":
        return importlib.import_module("planframe.expr")
    if name == "spark":
        return importlib.import_module("planframe.spark")
    if name == "pandas":
        return importlib.import_module("planframe.pandas")
    raise AttributeError(name)


__all__ = [
    "__version__",
    "__expr_ir_version__",
    "__plan_ir_version__",
    "Frame",
    "Schema",
    "GroupedFrame",
    "DynamicGroupedFrame",
    "ColumnSelector",
    "JoinOptions",
    "execute_plan",
    "ExecutionOptions",
    "expr",
    "spark",
    "pandas",
]

# IR compatibility markers for adapter authors and external tooling.
__plan_ir_version__: int = PLAN_IR_VERSION
__expr_ir_version__: int = EXPR_IR_VERSION
