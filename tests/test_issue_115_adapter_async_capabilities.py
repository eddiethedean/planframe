"""Issue #115: advisory native_async_materialize on AdapterCapabilities."""

from __future__ import annotations

import pytest

from planframe.backend.adapter import AdapterCapabilities


def test_adapter_capabilities_native_async_materialize_defaults_false() -> None:
    assert AdapterCapabilities().native_async_materialize is False


def test_shipped_polars_adapter_declares_thread_pooled_async_by_default() -> None:
    pytest.importorskip("planframe_polars")

    from planframe_polars import PolarsAdapter

    assert PolarsAdapter().capabilities.native_async_materialize is False
