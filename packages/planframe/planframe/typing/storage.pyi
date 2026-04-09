from __future__ import annotations

from typing import TypeAlias

from .scalars import Scalar

StorageOptions: TypeAlias = dict[str, Scalar]

__all__ = ["StorageOptions"]
