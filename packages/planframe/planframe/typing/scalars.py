from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TypeAlias

# Common “scalar” values that dataframe backends generally accept as fill values.
# This is intentionally conservative (and backend-agnostic): adapters may support
# additional literal types, but these cover the typical cross-backend set.
Scalar: TypeAlias = (
    None
    | bool
    | int
    | float
    | str
    | bytes
    | date
    | datetime
    | time
    | timedelta
    | Decimal
)

