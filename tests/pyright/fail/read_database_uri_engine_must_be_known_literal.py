from __future__ import annotations

from dataclasses import dataclass

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    x: int


# should fail: engine must be a known Literal
_pf = PolarsFrame.read_database_uri("select 1 as x", uri="sqlite:///tmp.db", engine="sqlalchemy", schema=S)

