from __future__ import annotations

from planframe.expr import agg_sum, col, lower, truediv
from planframe_polars import PolarsFrame


class Campaign(PolarsFrame):
    name: str
    id: int
    revenue: int
    clicks: int


def main() -> None:
    pf = Campaign(
        {
            "name": ["A", "a", "B"],
            "id": [1, 2, 3],
            "revenue": [10, 30, 100],
            "clicks": [2, 2, 5],
        }
    )
    out = (
        pf.group_by(lower(col("name")))
        .agg(
            n=("count", "id"),
            rpc=agg_sum(truediv(col("revenue"), col("clicks"))),
        )
        .sort("__pf_g0")
    )
    df = out.collect()
    print(f"columns={df.columns}")
    print(df.to_dict(as_series=False))


if __name__ == "__main__":
    main()
