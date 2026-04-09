from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TypeAlias

Scalar: TypeAlias = (
    None | bool | int | float | str | bytes | date | datetime | time | timedelta | Decimal
)

__all__ = ["Scalar"]
