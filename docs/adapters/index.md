# Adapters

PlanFrame is backend-agnostic. Execution is provided by **adapters**.

## Official adapters

- **planframe-polars**: Polars backend adapter (end-user docs live under the `planframe-polars` track).
- **planframe-pandas**: pandas backend adapter (end-user docs live under the `planframe-pandas` track).

## Adding a new adapter

Start from the core guide:

- [Creating an adapter](../planframe/guides/creating-an-adapter.md)

End-user packages may ship optional **API skins** (`planframe.spark`, `planframe.pandas`) as mixins; adapters only need to implement `BaseAdapter` (including optional hooks such as `hint()` if you interpret plan-level hints). See [PySpark-like API](../planframe/guides/pyspark-like-api.md) and [pandas-like API](../planframe/guides/pandas-like-api.md).

