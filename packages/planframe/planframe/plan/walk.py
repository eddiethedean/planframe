from __future__ import annotations

from collections.abc import Iterator
from typing import cast

from planframe.backend.errors import PlanFrameBackendError
from planframe.plan.nodes import (
    Agg,
    ConcatHorizontal,
    ConcatVertical,
    GroupBy,
    Join,
    PlanNode,
)
from planframe.typing.frame_like import FrameLike


def iter_plan_nodes(*, root: PlanNode, include_side_frames: bool = False) -> Iterator[PlanNode]:
    """Iterate plan nodes in a deterministic pre-order traversal.

    Pre-order means the current node is yielded before its children.

    By default this walks only the linear `prev` chain (the primary pipeline). Nodes that
    reference other frames (`Join.right`, concat `other`) are treated as boundaries and
    are *not* descended into unless `include_side_frames=True`.

    When `include_side_frames=True`, the traversal yields:
    - the current node
    - then its `prev` subtree (depth-first)
    - then any side frame subtrees (depth-first), in a stable order:
      - for `Join`: RHS (`right`) after the left chain
      - for concats: `other` after the left chain
    """

    def _frame_plan(frame: FrameLike) -> PlanNode:
        plan = getattr(frame, "_plan", None)
        if not isinstance(plan, PlanNode):
            raise PlanFrameBackendError("Side frame does not contain a valid PlanNode plan")
        return plan

    stack: list[PlanNode] = [root]
    while stack:
        node = stack.pop()
        yield node

        # Special-case nodes whose semantics mean "next node is not meaningful by itself".
        # GroupBy only becomes meaningful when immediately followed by Agg; traversal should
        # still remain deterministic, so we just recurse into prev like any other node.
        if isinstance(node, Agg) and isinstance(node.prev, GroupBy):
            # `Agg.prev` is a GroupBy node; the "real" input is `GroupBy.prev`.
            # We still visit GroupBy itself as part of the tree.
            stack.append(node.prev)
            continue

        # Side frames (optional)
        if include_side_frames:
            if isinstance(node, Join):
                stack.append(_frame_plan(node.right))
            elif isinstance(node, (ConcatVertical, ConcatHorizontal)):
                stack.append(_frame_plan(node.other))

        # Primary chain
        prev = getattr(node, "prev", None)
        if isinstance(prev, PlanNode):
            stack.append(prev)
