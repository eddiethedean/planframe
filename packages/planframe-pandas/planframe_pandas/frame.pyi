from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, TypeVar

import pandas as pd

from planframe.frame import Frame

SchemaT = TypeVar("SchemaT")

PandasBackendFrame = pd.DataFrame

class PandasFrame(Frame[Any, PandasBackendFrame, object]):
    def __new__(
        cls,
        data: Mapping[str, Sequence[object]] | Sequence[Mapping[str, object]],
    ) -> PandasFrame: ...
