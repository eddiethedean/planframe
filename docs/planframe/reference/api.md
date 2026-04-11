# API (PlanFrame core)

This is a light API reference intended for quick discovery. For the full surface area, see the generated reference for each module.

## Optional API skins

Typed mixins (import from `planframe.spark` / `planframe.pandas`, or `from planframe import spark` / `pandas`) layer familiar naming on `Frame` without adding backend dependencies. Guides: [PySpark-like API](../guides/pyspark-like-api.md), [pandas-like API](../guides/pandas-like-api.md).

## `planframe.spark.frame.SparkFrame`

::: planframe.spark.frame.SparkFrame

## `planframe.pandas.frame.PandasLikeFrame`

::: planframe.pandas.frame.PandasLikeFrame

## `planframe.frame.Frame`

::: planframe.frame.Frame
    options:
      members:
        - source
        - schema
        - plan
        - optimize
        - collect
        - acollect
        - collect_async
        - collect_backend_async
        - to_dicts
        - ato_dicts
        - to_dicts_async
        - to_dict
        - ato_dict
        - to_dict_async

## `planframe.execution_options.ExecutionOptions`

::: planframe.execution_options.ExecutionOptions

## `planframe.plan.join_options.JoinOptions`

::: planframe.plan.join_options.JoinOptions

## `planframe.execution.execute_plan`

::: planframe.execution.execute_plan

## `planframe.execution.execute_plan_async`

::: planframe.execution.execute_plan_async

## `planframe.materialize`

::: planframe.materialize

## `planframe.backend.io.AdapterColumnarStreamer`

Optional protocol (spike) for chunked columnar batches — see [Columnar streaming (design)](../design/columnar-streaming.md).

::: planframe.backend.io.AdapterColumnarStreamer

## `planframe.compile_context.PlanCompileContext`

Internal helper shared by `Frame` and `execute_plan` for compiling expression IR and related structures. Adapter authors rarely import it directly; see [Core layout](../design/core-layout.md).

::: planframe.compile_context.PlanCompileContext

## `planframe.plan.walk.iter_plan_nodes`

::: planframe.plan.walk.iter_plan_nodes

