"""Local pytest defaults."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def ensure_greenlet_context() -> None:
    """Sync no-op so parent-directory autouse async fixtures do not break this suite."""

    yield
