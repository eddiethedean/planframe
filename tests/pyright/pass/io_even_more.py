from __future__ import annotations

from dataclasses import dataclass

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int
    part: str
    age: int


pf = PolarsFrame.scan_parquet_dataset(
    "s3://bucket/data/**/*.parquet", schema=S, storage_options={"aws_region": "us-east-1"}
)
pf.sink_parquet(
    "s3://bucket/out/",
    partition_by=("part",),
    compression="zstd",
    storage_options={"aws_region": "us-east-1"},
)
