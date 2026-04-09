from __future__ import annotations

from typing import TypeAlias

from .scalars import Scalar

# Cloud storage options are generally a string->scalar mapping (s3 creds, endpoints, flags, etc.).
# Use a concrete dict type because some backend stubs (e.g. Polars) require a dict.
StorageOptions: TypeAlias = dict[str, Scalar]
