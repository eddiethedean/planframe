from __future__ import annotations

import planframe


def test_ir_version_markers_exist_and_are_ints() -> None:
    assert isinstance(planframe.__plan_ir_version__, int)
    assert isinstance(planframe.__expr_ir_version__, int)
    assert planframe.__plan_ir_version__ >= 1
    assert planframe.__expr_ir_version__ >= 1
