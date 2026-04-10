from __future__ import annotations

import planframe


def test_planframe_has_version_attr() -> None:
    assert isinstance(planframe.__version__, str)
    assert planframe.__version__
