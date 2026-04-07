from __future__ import annotations

from typing_extensions import reveal_type

from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    x: int
    lst: object
    s: object


pf = S({"id": [1], "x": [1], "lst": [[1, 2]], "s": [{"a": 1, "b": 2}]})

out = (
    pf.select("id", "x")
    .concat_horizontal(pf.select("id").rename(id="id2"))
    .union_distinct(pf.select("id", "x"))
    .explode("lst")
    .unnest("s", fields=("a", "b"))
    .drop_nulls_all("a", "b")
)

df = out.collect()
reveal_type(df)
