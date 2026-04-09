"""Local pytest defaults."""

from __future__ import annotations

import pytest


def pytest_configure() -> None:
    # Reserved for future test-time configuration hooks.
    return None


@pytest.fixture(autouse=True)
def ensure_greenlet_context() -> None:
    """Sync no-op so parent-directory autouse async fixtures do not break this suite."""
    yield
