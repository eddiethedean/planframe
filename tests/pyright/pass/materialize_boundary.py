from __future__ import annotations

from planframe import materialize_columns, materialize_into
from planframe.execution_options import ExecutionOptions
from planframe.expr import add, col, eq, lit
from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    age: int


pf = User({"id": [1], "age": [2]})

out = (
    pf.select("id", "age")
    .with_columns(age_plus_one=add(col("age"), lit(1)))
    .filter(eq(col("id"), lit(1)))
)

cols: dict[str, list[object]] = materialize_columns(out, options=ExecutionOptions(streaming=True))


def _row_count(c: dict[str, list[object]]) -> int:
    return len(next(iter(c.values())))


n: int = materialize_into(out, _row_count, options=None)

_: object = cols
_: object = n
