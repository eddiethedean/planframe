# Migrating since v1.1.0

This guide summarizes changes after **v1.1.0** of `planframe`, `planframe-polars`, and `planframe-pandas` (released together). Upgrade those packages to the **same** new version.

If you are jumping from **v1.0.x**, read [Migrating to v1.0.0](migrating-to-1-0.md) first, then return here.

## v1.3.0 (upcoming)

### `CompileExprContext.resolve_backend_dtype` (#113)

- **`CompileExprContext`** may carry **`resolve_backend_dtype`**, set by **`execute_plan`** when it has the live backend frame for the step. **`BaseAdapter.resolve_dtype`** consults it after the step **`Schema`** lookup, so column dtypes can be recovered from native metadata when the PlanFrame schema is partial.
- Adapters can implement **`BaseAdapter.resolve_backend_dtype_from_frame`**; Polars, pandas, and sparkless provide defaults that introspect each engine’s column types.

**Adapter authors:** optional hooks; existing adapters that only use `ctx.schema` keep the same behavior when the callback is absent.

### Unknown columns during `compile_expr` (#114)

- **Documentation**: [Creating an adapter — Unknown columns during `compile_expr`](creating-an-adapter.md#unknown-columns-during-compile_expr) describes the **permissive** policy for shipped adapters (`resolve_dtype` returning `None` is a missing hint, not a compile-time error; engines typically fail at execution if the column is absent).
- **Tests**: `tests/test_issue_114_compile_expr_unknown_column_policy.py` locks in that policy for Polars and pandas.

### Async materialization: thread pool vs native async (#115)

- **`AdapterCapabilities.native_async_materialize`**: advisory flag (default `False`). Set `True` when async materializers are overridden for native `async` I/O; PlanFrame does not branch on it today.
- **Docs**: [Creating an adapter — Default async behavior](creating-an-adapter.md#default-async-behavior-asyncioto_thread) and [Declaring native async materialization](creating-an-adapter.md#declaring-native-async-materialization-advisory).

### Columnar boundary discoverability (#116)

- **`Frame` docstrings** for `to_dict` / `to_dicts` / `ato_dict` / `ato_dicts` / `to_dict_async` / `to_dicts_async` link to **`planframe.materialize`** and the [Columnar boundary](creating-an-adapter.md#columnar-boundary-materialize) section (stable anchor).
- **`planframe.materialize`** module docstring links back to `Frame` methods.

### Chunked columnar export (design spike) (#117)

- **`AdapterColumnarStreamer`** (`planframe.backend.io`): optional protocol sketch for chunked `dict[str, list[object]]` batches; not yet invoked from `materialize_columns` / `Frame.to_dict`.
- **Design:** [Columnar streaming (chunked export)](../design/columnar-streaming.md).

### Typing CI: Expr stubs + generated Frame stubs (#118)

- **[CONTRIBUTING.md](https://github.com/eddiethedean/planframe/blob/main/CONTRIBUTING.md)** describes the Pyright / `ty` / stub parity workflow; CI runs `scripts/generate_typing_stubs.py --check`.
- **`tests/pyright/pass/expr_ir_public_contract.py`**: core-only `planframe.expr` contract (no Polars frame).

## v1.2.0

### Correctness: expression compilation uses each step’s input schema (#103)

`execute_plan` (and thus `Frame` materialization) compiles filter predicates, projections, sort keys, join keys, and similar expressions using the **schema at that plan step**, not the final frame schema.

**User impact:** chains like `filter(...).select(...)` behave correctly when the filter references columns that are **dropped** by a later `select` / projection. No API change—if you had relied on the old (incorrect) behavior, update your plans.

### `CompileExprContext` and `BaseAdapter.resolve_dtype` (#104)

- **`CompileExprContext`** is exported from `planframe` and carries the active schema during `compile_expr`.
- Adapters may implement **`BaseAdapter.resolve_dtype`** for dtype-aware lowering of `Col(...)` (Polars, pandas, and sparkless adapters call it for column references).

**Adapter authors:** optional hook; default remains a no-op.

### Async public API (#105)

- **`execute_plan_async`**: async wrapper around `execute_plan` using `asyncio.to_thread` (same keyword arguments). Exported from `planframe`.
- **Frame** adds discoverable aliases: **`collect_async`**, **`collect_backend_async`**, **`to_dict_async`**, **`to_dicts_async`** (same behavior as **`acollect`**, **`acollect_backend`**, **`ato_dict`**, **`ato_dicts`**).

### Expr operator overloads (#106)

`Expr` supports `==`, `!=`, `&`, `|`, and `~` to build expression IR (alongside existing ordered comparisons).

**Breaking:** expression IR dataclasses use `eq=False` so operators are not masked by dataclass-generated equality. **`expr1 == expr2`** now builds an **`Eq`** node when both sides are expressions (or coerced literals)—it does **not** mean Python structural equality between IR nodes. Use **`is`**, explicit field comparison, or the **`eq()`** function if you meant the functional API.

See [Typing design — Expr operator overloads](../design/typing-design.md#71-expr-operator-overloads-typing-semantics).

### `planframe.materialize` (#107)

Thin helpers for the columnar boundary: **`materialize_columns`**, **`materialize_into`**, plus async **`amaterialize_*`**. They forward **`ExecutionOptions`** like `Frame.to_dict` / `ato_dict`.

Use them when adapters or host libraries want a **single import** for `Frame → dict[str, list[object]]` before applying Pydantic, dataclasses, or other factories.

See [Creating an adapter — Columnar boundary helpers](creating-an-adapter.md#columnar-boundary-materialize).

## See also

- [CHANGELOG.md](https://github.com/eddiethedean/planframe/blob/main/CHANGELOG.md) (authoritative list)
- [Stability & compatibility](../../shared/stability-and-compatibility.md)
