from __future__ import annotations

from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    age: int


pf = User({"id": [1], "age": [2]})

col_name: str = "id"

bad_col_name: int = 1

# This should fail: column names must be strings (ideally literals).
pf2 = pf.select(bad_col_name)

