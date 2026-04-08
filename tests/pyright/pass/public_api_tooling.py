from __future__ import annotations

from planframe import Frame, execute_plan
from planframe.plan.walk import iter_plan_nodes
from planframe_polars import PolarsFrame


class S(PolarsFrame):
    id: int
    a: int | None


pf = S({"id": [1], "a": [None]})

# public execution API
planned = execute_plan(adapter=pf._adapter, plan=pf.plan(), root_data=pf._data, schema=pf.schema())
_ = pf._adapter.collect(planned)

# plan walking tooling
names = [type(n).__name__ for n in iter_plan_nodes(root=pf.fill_null(0, "a").plan())]
assert "FillNull" in names

# root Frame source typing still works
_ = Frame.source([{"id": 1, "a": None}], adapter=pf._adapter, schema=S)
