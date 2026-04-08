from __future__ import annotations

from planframe.plan.walk import iter_plan_nodes
from planframe_polars import PolarsFrame


def test_iter_plan_nodes_linear_chain_preorder() -> None:
    class S(PolarsFrame):
        id: int
        a: int | None

    pf = S({"id": [1], "a": [None]})
    out = pf.select("id", "a").fill_null(0, "a").drop_nulls("a").head(1)

    names = [type(n).__name__ for n in iter_plan_nodes(root=out.plan())]
    assert names[:4] == ["Head", "DropNulls", "FillNull", "Select"]
    assert names[-1] == "Source"


def test_iter_plan_nodes_join_does_not_descend_rhs_by_default() -> None:
    class L(PolarsFrame):
        id: int
        k1: int

    class R(PolarsFrame):
        id: int
        k1: int

    left = L({"id": [1], "k1": [1]})
    right = R({"id": [1], "k1": [1]})
    joined = left.join(right, on=("k1",))

    names = [type(n).__name__ for n in iter_plan_nodes(root=joined.plan())]
    # Should traverse only the LHS chain + Join itself.
    assert names[0] == "Join"
    assert names[-1] == "Source"
    assert names.count("Source") == 1


def test_iter_plan_nodes_join_can_include_rhs_plan() -> None:
    class L2(PolarsFrame):
        id: int
        k1: int

    class R2(PolarsFrame):
        id: int
        k1: int

    left = L2({"id": [1], "k1": [1]}).select("k1")
    right = R2({"id": [1], "k1": [1]}).select("k1").head(1)
    joined = left.join(right, on=("k1",))

    names = [
        type(n).__name__ for n in iter_plan_nodes(root=joined.plan(), include_side_frames=True)
    ]
    assert names[0] == "Join"
    # One Source for LHS and one Source for RHS.
    assert names.count("Source") == 2
    # RHS nodes appear after LHS chain (deterministic order).
    rhs_head_idx = names.index("Head")
    assert rhs_head_idx > names.index("Select")


def test_iter_plan_nodes_concat_can_include_other_plan() -> None:
    class A(PolarsFrame):
        id: int

    class Bv(PolarsFrame):
        id: int

    class Bh(PolarsFrame):
        x: int

    left = A({"id": [1]}).select("id").head(1)
    right_v = Bv({"id": [2]}).select("id")
    right_h = Bh({"x": [2]}).select("x")

    out_v = left.concat_vertical(right_v)
    names_v = [
        type(n).__name__ for n in iter_plan_nodes(root=out_v.plan(), include_side_frames=True)
    ]
    assert names_v[0] == "ConcatVertical"
    assert names_v.count("Source") == 2
    # RHS nodes come after LHS chain.
    src_idx_v = [i for i, n in enumerate(names_v) if n == "Source"]
    assert len(src_idx_v) == 2
    assert src_idx_v[1] > src_idx_v[0]

    out_h = left.concat_horizontal(right_h)
    names_h = [
        type(n).__name__ for n in iter_plan_nodes(root=out_h.plan(), include_side_frames=True)
    ]
    assert names_h[0] == "ConcatHorizontal"
    assert names_h.count("Source") == 2
