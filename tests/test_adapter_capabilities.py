from __future__ import annotations

from planframe_pandas.adapter import PandasAdapter
from planframe_polars.adapter import PolarsAdapter


def test_adapter_capabilities_are_exposed() -> None:
    p = PolarsAdapter()
    caps = p.capabilities
    assert caps.explode_outer is False
    assert caps.posexplode_outer is False
    assert caps.lazy_sample is False
    assert caps.scan_delta is True
    assert caps.read_delta is True
    assert caps.sink_delta is True
    assert caps.read_avro is True
    assert caps.sink_avro is True
    assert caps.read_excel is True
    assert caps.sink_excel is True
    assert caps.read_database_uri is True
    assert caps.sink_database is True
    assert caps.storage_options is True

    pd = PandasAdapter()
    caps2 = pd.capabilities
    assert caps2.explode_outer is False
    assert caps2.posexplode_outer is True
    assert caps2.lazy_sample is True
    assert caps2.scan_delta is False
    assert caps2.read_delta is False
    assert caps2.sink_delta is False
    assert caps2.read_avro is False
    assert caps2.sink_avro is False
    assert caps2.read_excel is False
    assert caps2.sink_excel is True
    assert caps2.read_database_uri is False
    assert caps2.sink_database is True
    assert caps2.storage_options is False
