from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int
    age: int


parquet_path = Path("data.parquet")
csv_path = Path("data.csv")

pf1 = PolarsFrame.scan_parquet(str(parquet_path), schema=S)
pf2 = PolarsFrame.scan_csv(str(csv_path), schema=S)

pf1.select("id").sink_parquet("out.parquet")
pf2.select("id").sink_csv("out.csv")
