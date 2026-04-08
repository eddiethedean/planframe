from __future__ import annotations

from dataclasses import fields, is_dataclass
from typing import Literal

from planframe.plan.nodes import Drop, PlanNode, Rename, Select


def optimize_plan(plan: PlanNode, *, level: Literal[0, 1, 2] = 1) -> PlanNode:
    """Return an optimized version of *plan*.

    This performs only backend-independent, semantics-preserving rewrites. It is **opt-in**:
    passing `level=0` returns the input plan unchanged.

    Current rewrites (level >= 1):
    - Fuse consecutive `Select` nodes: `Select(Select(prev, a), b) -> Select(prev, b)`
    - Prune no-op `Drop` with no columns: `Drop(prev, ()) -> prev`
    - Prune no-op `Rename` with empty mapping: `Rename(prev, {}) -> prev`
    """

    if level == 0:
        return plan

    cur: PlanNode = plan
    while True:
        nxt = _optimize_once(cur)
        if nxt is cur:
            return cur
        cur = nxt


def _optimize_once(node: PlanNode) -> PlanNode:
    # First, recursively optimize the prev chain (if present).
    prev = getattr(node, "prev", None)
    if isinstance(prev, PlanNode):
        prev2 = _optimize_once(prev)
        if prev2 is not prev:
            node = _replace_prev(node, prev2)

    # Now apply local rewrites.
    if isinstance(node, Select) and isinstance(node.prev, Select):
        # Later select overrides earlier; safe for Frame-built plans.
        return Select(prev=node.prev.prev, columns=node.columns)

    if isinstance(node, Drop) and not node.columns:
        return node.prev

    if isinstance(node, Rename) and not node.mapping:
        return node.prev

    return node


def _replace_prev(node: PlanNode, prev: PlanNode) -> PlanNode:
    if not is_dataclass(node):
        raise TypeError(f"Expected dataclass PlanNode, got: {type(node).__name__}")

    cls = type(node)
    kw: dict[str, object] = {}
    for f in fields(node):
        kw[f.name] = prev if f.name == "prev" else getattr(node, f.name)
    return cls(**kw)
