from __future__ import annotations

from planframe_pandas.adapter import PandasAdapter
from planframe_polars.adapter import PolarsAdapter


def test_adapter_capabilities_are_exposed() -> None:
    p = PolarsAdapter()
    caps = p.capabilities
    assert caps.explode_outer is False
    assert caps.posexplode_outer is False
    assert caps.lazy_sample is False

    pd = PandasAdapter()
    caps2 = pd.capabilities
    assert caps2.explode_outer is False
    assert caps2.posexplode_outer is True
    assert caps2.lazy_sample is True
