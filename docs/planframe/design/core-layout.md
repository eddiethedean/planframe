# Core package layout (implementation map)

This page summarizes how the **`planframe`** package is organized internally. It is aimed at **contributors and adapter authors** who need to navigate the codebase; the public API remains `from planframe import Frame`, `execute_plan`, etc.

## `Frame` lives under `planframe/frame/`

The user-facing type **`Frame`** is a single class composed from mixins (single responsibility per module, easier navigation):

| Module | Role |
| --- | --- |
| `packages/planframe/planframe/frame/_class.py` | Declares `class Frame(...)` and `__slots__`. |
| `packages/planframe/planframe/frame/_mixin_core.py` | Construction (`source`), `schema` / `plan`, `optimize`, expression compilation helpers, and delegation to `execute_plan`. |
| `packages/planframe/planframe/frame/_mixin_ops.py` | Lazy transforms from selection through `pivot` (the bulk of the fluent API). |
| `packages/planframe/planframe/frame/_mixin_io.py` | Materialization (`collect`, async helpers), row/column export, and `write_*` sinks. |
| `packages/planframe/planframe/frame/_utils.py` | Small helpers shared by mixins (e.g. sort flag coercion). |

Imports stay stable: **`from planframe.frame import Frame`** re-exports from `packages/planframe/planframe/frame/__init__.py`.

## Shared compilation: `PlanCompileContext`

`planframe.compile_context.PlanCompileContext` (`packages/planframe/planframe/compile_context.py`) holds **`(adapter, schema)`** and centralizes:

- compiling expression IR via `adapter.compile_expr`
- lowering join keys, named aggregations, sort keys, and `Project` items into adapter-facing structures

Both **`Frame`** (when building or validating plans) and **`execute_plan`** use the same type so compilation behavior does not drift between “plan building” and “plan execution”.

## Plan execution: `execute_plan` and node dispatch

`planframe.execution.execute_plan` (`packages/planframe/planframe/execution.py`) is the supported interpreter for `PlanNode` trees (`packages/planframe/planframe/plan/nodes.py`). It:

1. Builds a **`PlanCompileContext`** for the given adapter and schema.
2. Walks the plan using a **registry** of handlers keyed by concrete node type (instead of one giant `if`/`elif` chain), so new node kinds can be added in a localized way.

`Frame` materialization calls into this interpreter; adapters still implement the per-operation primitives (`select`, `filter`, `join`, …).

**`execute_plan_async`:** does not run a separate async plan interpreter. It schedules the same synchronous `execute_plan` implementation on a worker thread via `asyncio.to_thread`, so callers can `await` plan evaluation without blocking the event loop. Async engine I/O remains on `BaseAdapter` async materialization methods (`acollect`, `ato_dict`, …). See [Migrating since v1.1.0](../guides/migrating-since-1-1.md) (async public API) and [Creating an adapter — Async execution contract](../guides/creating-an-adapter.md#async-execution-contract-third-party-adapters).

## Typing stubs

Generated Pyright stubs for `Frame` are emitted to **`planframe/frame/__init__.pyi`**. Regenerate with:

```bash
python scripts/generate_typing_stubs.py
```

See also: [Typing design](typing-design.md), [Creating an adapter](../guides/creating-an-adapter.md).
