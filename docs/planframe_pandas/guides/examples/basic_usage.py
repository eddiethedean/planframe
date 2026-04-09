from __future__ import annotations

from planframe.expr import add, col, lit
from planframe_pandas import PandasFrame


class User(PandasFrame):
    id: int
    age: int


pf = User({"id": [1], "age": [10]})

out = pf.with_columns(age_plus_one=add(col("age"), lit(1))).select("id", "age", "age_plus_one")

df = out.collect()

print(f"columns={df.columns.tolist()}")
print(f"to_dict={out.to_dict()}")
print(f"rows={out.to_dicts()}")
