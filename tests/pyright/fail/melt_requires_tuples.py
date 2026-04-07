from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from typing import Any, cast

from planframe_polars import PolarsFrame


@dataclass(frozen=True)
class S:
    id: int
    a: int
    b: int


lf = pl.DataFrame({"id": [1], "a": [2], "b": [3]}).lazy()
pf = cast(Any, PolarsFrame[S])(lf)

# This should fail: melt expects tuples for id_vars/value_vars.
pf2 = pf.melt(id_vars=("id",), value_vars=["a", "b"])

