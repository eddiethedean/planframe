# planframe-spark

`planframe-spark` provides a **PySpark backend adapter** for PlanFrame.

It also re-exports the **Spark-shaped typing/interface surface** from core `planframe.spark` for convenience, but the canonical interface lives in `planframe`.

## Install

```bash
pip install planframe planframe-spark
```

## Quickstart

```python
from planframe.spark import SparkFrame, functions as F
from planframe_spark import PySparkAdapter

# Configure your Frame subclass to use the PySpark adapter when sourcing data.
# (You still need a SparkSession and a pyspark.sql.DataFrame to execute on Spark.)
```

## Notes

- `planframe-spark` requires PySpark: `pip install planframe-spark`.
- This package does not create or manage a SparkSession; it only defines an adapter contract.

