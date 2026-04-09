from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from planframe.backend.adapter import BackendAdapter
from planframe.backend.errors import PlanFrameBackendError
from planframe.compile_context import PlanCompileContext
from planframe.execution_options import ExecutionOptions
from planframe.expr.api import Expr
from planframe.plan.nodes import (
    Agg,
    Cast,
    ConcatHorizontal,
    ConcatVertical,
    Drop,
    DropNulls,
    DropNullsAll,
    Duplicated,
    DynamicGroupByAgg,
    Explode,
    FillNull,
    Filter,
    GroupBy,
    Head,
    Join,
    Melt,
    Pivot,
    PlanNode,
    Posexplode,
    Project,
    Rename,
    RollingAgg,
    Sample,
    Select,
    Slice,
    Sort,
    Source,
    Tail,
    Unique,
    Unnest,
    WithColumn,
    WithRowCount,
)
from planframe.schema.ir import Schema
from planframe.typing.frame_like import FrameLike

BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


@dataclass
class _ExecState(Generic[BackendFrameT, BackendExprT]):
    adapter: BackendAdapter[BackendFrameT, BackendExprT]
    root_data: BackendFrameT
    ctx: PlanCompileContext[BackendFrameT, BackendExprT]

    def evaluate(self, node: object) -> BackendFrameT:
        if not isinstance(node, PlanNode):
            raise PlanFrameBackendError(f"Unsupported plan node: {type(node)!r}")
        handler = _NODE_HANDLERS.get(type(node))
        if handler is None:
            raise PlanFrameBackendError(f"Unsupported plan node: {type(node)!r}")
        return handler(self, node)


def _handle_source(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Source)
    return state.root_data


def _handle_select(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Select)
    return state.adapter.select(state.evaluate(node.prev), node.columns)


def _handle_project(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, Project)
    prev = state.evaluate(node.prev)
    parts = state.ctx.compile_project_items(node.items)
    return state.adapter.project(prev, parts)


def _handle_drop(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Drop)
    return state.adapter.drop(state.evaluate(node.prev), node.columns, strict=node.strict)


def _handle_rename(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Rename)
    return state.adapter.rename(state.evaluate(node.prev), node.mapping, strict=node.strict)


def _handle_with_column(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, WithColumn)
    return state.adapter.with_column(
        state.evaluate(node.prev), node.name, state.ctx.compile_expr(node.expr)
    )


def _handle_cast(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Cast)
    return state.adapter.cast(state.evaluate(node.prev), node.name, node.dtype)


def _handle_with_row_count(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, WithRowCount)
    return state.adapter.with_row_count(
        state.evaluate(node.prev), name=node.name, offset=node.offset
    )


def _handle_filter(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Filter)
    return state.adapter.filter(state.evaluate(node.prev), state.ctx.compile_expr(node.predicate))


def _handle_sort(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Sort)
    prev = state.evaluate(node.prev)
    compiled = state.ctx.compile_sort_keys(node.keys)
    return state.adapter.sort(
        prev,
        compiled,
        descending=node.descending,
        nulls_last=node.nulls_last,
    )


def _handle_unique(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Unique)
    return state.adapter.unique(
        state.evaluate(node.prev),
        node.subset,
        keep=node.keep,
        maintain_order=node.maintain_order,
    )


def _handle_duplicated(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, Duplicated)
    return state.adapter.duplicated(
        state.evaluate(node.prev),
        node.subset,
        keep=node.keep,
        out_name=node.out_name,
    )


def _handle_group_by(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, GroupBy)
    return state.evaluate(node.prev)


def _handle_agg(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Agg)
    if not isinstance(node.prev, GroupBy):
        raise PlanFrameBackendError("Agg must follow GroupBy")
    compiled_keys = state.ctx.compile_join_keys_tuple(node.prev.keys)
    compiled_aggs = state.ctx.compile_named_aggs(node.named_aggs)
    return state.adapter.group_by_agg(
        state.evaluate(node.prev.prev),
        keys=compiled_keys,
        named_aggs=compiled_aggs,
    )


def _handle_dynamic_group_by_agg(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, DynamicGroupByAgg)
    compiled_aggs = state.ctx.compile_named_aggs(node.named_aggs)
    return state.adapter.group_by_dynamic_agg(
        state.evaluate(node.prev),
        index_column=node.index_column,
        every=node.every,
        period=node.period,
        by=node.by,
        named_aggs=compiled_aggs,
    )


def _handle_drop_nulls(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, DropNulls)
    return state.adapter.drop_nulls(
        state.evaluate(node.prev),
        node.subset,
        how=node.how,
        threshold=node.threshold,
    )


def _handle_drop_nulls_all(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, DropNullsAll)
    return state.adapter.drop_nulls_all(state.evaluate(node.prev), node.subset)


def _handle_fill_null(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, FillNull)
    prev = state.evaluate(node.prev)
    if node.value is not None and isinstance(node.value, Expr):
        compiled_value: object | BackendExprT = state.ctx.compile_expr(node.value)
    else:
        compiled_value = node.value
    return state.adapter.fill_null(prev, compiled_value, node.subset, strategy=node.strategy)


def _handle_melt(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Melt)
    return state.adapter.melt(
        state.evaluate(node.prev),
        id_vars=node.id_vars,
        value_vars=node.value_vars,
        variable_name=node.variable_name,
        value_name=node.value_name,
    )


def _handle_join(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Join)
    left_df = state.evaluate(node.prev)
    right_frame: FrameLike = node.right
    if getattr(right_frame, "_adapter", None) is None:
        raise PlanFrameBackendError("Join node right frame is invalid")
    if getattr(right_frame._adapter, "name", None) != state.adapter.name:
        raise PlanFrameBackendError("Cannot join frames from different backends")
    right_df = right_frame._eval(right_frame._plan)
    if node.left_keys is node.right_keys:
        compiled = state.ctx.compile_join_keys_tuple(node.left_keys)
        lo = ro = compiled
    else:
        lo = state.ctx.compile_join_keys_tuple(node.left_keys)
        ro = state.ctx.compile_join_keys_tuple(node.right_keys)
    return state.adapter.join(
        left_df,
        right_df,
        left_on=lo,
        right_on=ro,
        how=node.how,
        suffix=node.suffix,
        options=node.options,
    )


def _handle_slice(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Slice)
    return state.adapter.slice(state.evaluate(node.prev), offset=node.offset, length=node.length)


def _handle_head(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Head)
    return state.adapter.head(state.evaluate(node.prev), node.n)


def _handle_tail(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Tail)
    return state.adapter.tail(state.evaluate(node.prev), node.n)


def _handle_concat_vertical(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, ConcatVertical)
    left_df = state.evaluate(node.prev)
    other_frame: FrameLike = node.other
    if getattr(other_frame, "_adapter", None) is None:
        raise PlanFrameBackendError("ConcatVertical node other frame is invalid")
    if getattr(other_frame._adapter, "name", None) != state.adapter.name:
        raise PlanFrameBackendError("Cannot concat frames from different backends")
    right_df = other_frame._eval(other_frame._plan)
    return state.adapter.concat_vertical(left_df, right_df)


def _handle_concat_horizontal(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, ConcatHorizontal)
    left_df = state.evaluate(node.prev)
    other_frame: FrameLike = node.other
    if getattr(other_frame, "_adapter", None) is None:
        raise PlanFrameBackendError("ConcatHorizontal node other frame is invalid")
    if getattr(other_frame._adapter, "name", None) != state.adapter.name:
        raise PlanFrameBackendError("Cannot concat frames from different backends")
    right_df = other_frame._eval(other_frame._plan)
    return state.adapter.concat_horizontal(left_df, right_df)


def _handle_pivot(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Pivot)
    return state.adapter.pivot(
        state.evaluate(node.prev),
        index=node.index,
        on=node.on,
        values=node.values,
        agg=node.agg,
        on_columns=node.on_columns,
        separator=node.separator,
        sort_columns=node.sort_columns,
    )


def _handle_rolling_agg(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, RollingAgg)
    return state.adapter.rolling_agg(
        state.evaluate(node.prev),
        on=node.on,
        column=node.column,
        window_size=node.window_size,
        op=node.op,
        out_name=node.out_name,
        by=node.by,
        min_periods=node.min_periods,
    )


def _handle_explode(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, Explode)
    return state.adapter.explode(state.evaluate(node.prev), node.columns, outer=node.outer)


def _handle_unnest(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Unnest)
    return state.adapter.unnest(state.evaluate(node.prev), node.items)


def _handle_pos_explode(
    state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode
) -> BackendFrameT:
    assert isinstance(node, Posexplode)
    return state.adapter.posexplode(
        state.evaluate(node.prev),
        node.column,
        pos=node.pos,
        value=node.value,
        outer=node.outer,
    )


def _handle_sample(state: _ExecState[BackendFrameT, BackendExprT], node: PlanNode) -> BackendFrameT:
    assert isinstance(node, Sample)
    return state.adapter.sample(
        state.evaluate(node.prev),
        n=node.n,
        frac=node.frac,
        with_replacement=node.with_replacement,
        shuffle=node.shuffle,
        seed=node.seed,
    )


_NODE_HANDLERS: dict[type[PlanNode], Callable[[_ExecState[Any, Any], PlanNode], BackendFrameT]] = {
    Source: _handle_source,
    Select: _handle_select,
    Project: _handle_project,
    Drop: _handle_drop,
    Rename: _handle_rename,
    WithColumn: _handle_with_column,
    Cast: _handle_cast,
    WithRowCount: _handle_with_row_count,
    Filter: _handle_filter,
    Sort: _handle_sort,
    Unique: _handle_unique,
    Duplicated: _handle_duplicated,
    GroupBy: _handle_group_by,
    Agg: _handle_agg,
    DynamicGroupByAgg: _handle_dynamic_group_by_agg,
    DropNulls: _handle_drop_nulls,
    DropNullsAll: _handle_drop_nulls_all,
    FillNull: _handle_fill_null,
    Melt: _handle_melt,
    Join: _handle_join,
    Slice: _handle_slice,
    Head: _handle_head,
    Tail: _handle_tail,
    ConcatVertical: _handle_concat_vertical,
    ConcatHorizontal: _handle_concat_horizontal,
    Pivot: _handle_pivot,
    RollingAgg: _handle_rolling_agg,
    Explode: _handle_explode,
    Unnest: _handle_unnest,
    Posexplode: _handle_pos_explode,
    Sample: _handle_sample,
}


def execute_plan(
    *,
    adapter: BackendAdapter[BackendFrameT, BackendExprT],
    plan: PlanNode,
    root_data: BackendFrameT,
    schema: Schema,
    options: ExecutionOptions | None = None,
    collect: bool = False,
) -> BackendFrameT:
    """Execute a :class:`planframe.plan.nodes.PlanNode` tree.

    This is the supported public plan interpreter used by :meth:`planframe.frame.Frame.collect`.

    Important:
    - This returns the backend frame after applying the plan, but it does **not**
      call :meth:`planframe.backend.adapter.BaseAdapter.collect` unless
      `collect=True` is provided.
    """

    ctx = PlanCompileContext(adapter, schema)
    state = _ExecState(adapter=adapter, root_data=root_data, ctx=ctx)
    out = state.evaluate(plan)
    if collect:
        return adapter.collect(out, options=options)
    return out
