from __future__ import annotations

from planframe_polars import PolarsFrame


class Left(PolarsFrame):
    k1: int
    k2: int
    k3: int
    k4: int
    k5: int
    k6: int


class Right(PolarsFrame):
    k1: int
    k2: int
    k3: int
    k4: int
    k5: int
    k6: int


left = Left({"k1": [1], "k2": [1], "k3": [1], "k4": [1], "k5": [1], "k6": [1]})
right = Right({"k1": [1], "k2": [1], "k3": [1], "k4": [1], "k5": [1], "k6": [1]})

_ = left.join(right, on=("k1", "k2", "k3", "k4", "k5", "k6"))
