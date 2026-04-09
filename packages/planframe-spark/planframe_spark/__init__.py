"""PySpark adapter package for PlanFrame.

- The canonical Spark-shaped interface lives in core `planframe.spark`.
- This package provides a PySpark backend adapter (`PySparkAdapter`) for executing
  plans on Spark when `pyspark` is installed.
"""

from __future__ import annotations

from planframe.spark import Column, GroupedData, SparkFrame, functions, lit_value, unwrap_expr

from .adapter import PySparkAdapter

__all__ = [
    "Column",
    "GroupedData",
    "PySparkAdapter",
    "SparkFrame",
    "functions",
    "lit_value",
    "unwrap_expr",
]
