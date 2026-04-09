from __future__ import annotations

from functools import lru_cache

from sparkless.sql import SparkSession


@lru_cache(maxsize=1)
def _spark() -> SparkSession:
    # `SparkSession` is lightweight in sparkless and doesn’t require a JVM.
    return SparkSession("planframe_sparkless")
