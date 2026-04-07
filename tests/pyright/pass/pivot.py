from __future__ import annotations

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    k: str
    v: int


pf = S({"id": [1, 1], "k": ["a", "b"], "v": [10, 20]})
out = pf.pivot(index=("id",), on="k", values="v", on_columns=("a", "b"))
df = out.collect()

reveal_type(out)
reveal_type(df)

