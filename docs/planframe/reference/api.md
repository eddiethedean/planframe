# API (PlanFrame core)

This is a light API reference intended for quick discovery. For the full surface area, see the generated reference for each module.

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
        - to_dicts
        - ato_dicts
        - to_dict
        - ato_dict

## `planframe.execution_options.ExecutionOptions`

::: planframe.execution_options.ExecutionOptions

## `planframe.plan.join_options.JoinOptions`

::: planframe.plan.join_options.JoinOptions

## `planframe.execution.execute_plan`

::: planframe.execution.execute_plan

## `planframe.compile_context.PlanCompileContext`

Internal helper shared by `Frame` and `execute_plan` for compiling expression IR and related structures. Adapter authors rarely import it directly; see [Core layout](../design/core-layout.md).

::: planframe.compile_context.PlanCompileContext

## `planframe.plan.walk.iter_plan_nodes`

::: planframe.plan.walk.iter_plan_nodes

