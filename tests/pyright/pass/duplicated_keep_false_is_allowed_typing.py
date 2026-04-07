from __future__ import annotations

from planframe_polars import PolarsFrame
from typing_extensions import reveal_type


class S(PolarsFrame):
    id: int


pf = S({"id": [1, 1]})

# Typing allows bool keep; runtime backend may reject keep=False depending on implementation.
mask = pf.duplicated("id", keep=False)
df = mask.collect()
reveal_type(df)
