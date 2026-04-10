from __future__ import annotations

from planframe.expr import add, agg_sum, col, lit
from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    age: int


pf = User({"id": [1, 1, 2], "age": [10, 20, 30]})

# Expression keys are allowed in group_by(...), and appear as __pf_g{i} in output schema.
out = pf.group_by(add(col("id"), lit(1))).agg(total=("sum", "age"))
_ = out.collect()

out2 = pf.group_by("id", add(col("age"), lit(1))).agg(ratio=agg_sum(add(col("age"), lit(1))))
_ = out2.collect()

