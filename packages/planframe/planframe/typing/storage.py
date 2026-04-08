from __future__ import annotations

from collections.abc import Mapping
from typing import TypeAlias

from planframe.typing.scalars import Scalar

# Cloud storage options are generally a string->scalar mapping (s3 creds, endpoints, flags, etc.).
StorageOptions: TypeAlias = Mapping[str, Scalar]

