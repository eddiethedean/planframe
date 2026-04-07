from __future__ import annotations

from typing_extensions import reveal_type

from planframe.expr import add, col, lit
from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    age: int


pf = User({"id": [1], "age": [2]})

out = pf.select("id", "age").with_column("age_plus_one", add(col("age"), lit(1)))

OutDC = out.materialize_model("OutDC", kind="dataclass")
OutPD = out.materialize_model("OutPD", kind="pydantic")

reveal_type(OutDC)
reveal_type(OutPD)
