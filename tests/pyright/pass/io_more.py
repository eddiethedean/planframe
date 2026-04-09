from __future__ import annotations

from dataclasses import dataclass

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int
    age: int


pf1 = PolarsFrame.scan_ndjson("data.ndjson", schema=S)
pf2 = PolarsFrame.scan_ipc("data.ipc", schema=S)

pf1.select("id").sink_ndjson("out.ndjson")
pf2.select("id").sink_ipc("out.ipc")

conn: object = object()
pf3 = PolarsFrame.read_database("select 1 as id, 2 as age", connection=conn, schema=S)
pf3.sink_database("t", connection=conn, if_table_exists="append")
