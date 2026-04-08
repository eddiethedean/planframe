from __future__ import annotations

import pytest

from planframe.backend.errors import PlanFrameBackendError
from planframe.plan.nodes import Agg, GroupBy, PlanNode, Source
from planframe.plan.walk import iter_plan_nodes


def test_iter_plan_nodes_agg_groupby_special_case_yields_groupby() -> None:
    src = Source(schema_type=object, ir_version=1)
    gb = GroupBy(prev=src, keys=())
    agg = Agg(prev=gb, named_aggs={"x": ("count", "id")})

    names = [type(n).__name__ for n in iter_plan_nodes(root=agg)]
    assert names[:2] == ["Agg", "GroupBy"]
    assert names[-1] == "Source"


def test_iter_plan_nodes_side_frame_invalid_plan_errors() -> None:
    class BadFrame:
        _plan = object()

    class JoinLike(PlanNode):  # type: ignore[misc]
        pass

    # Easiest: use real Join node shape via duck typing isn't possible here; instead,
    # call the internal side-frame plan validator by walking a Join from Frame API.
    # We can still trigger the error by constructing a Join node manually.
    from planframe.plan.nodes import Join

    src = Source(schema_type=object, ir_version=1)
    j = Join(
        prev=src,
        right=BadFrame(),
        left_keys=(),
        right_keys=(),
        how="inner",
        suffix="_r",
        options=None,
    )  # type: ignore[arg-type]

    with pytest.raises(PlanFrameBackendError, match="Side frame does not contain a valid PlanNode"):
        list(iter_plan_nodes(root=j, include_side_frames=True))
