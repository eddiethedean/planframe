from __future__ import annotations

from typing import Any, TypeVar, cast

from planframe.backend.adapter import (
    BackendAdapter,
    CompiledJoinKey,
    CompiledProjectItem,
    CompiledSortKey,
)
from planframe.backend.errors import PlanFrameBackendError
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
    Explode,
    FillNull,
    Filter,
    GroupBy,
    Head,
    Join,
    JoinKeyColumn,
    JoinKeyExpr,
    Melt,
    Pivot,
    PlanNode,
    Posexplode,
    Project,
    ProjectPick,
    Rename,
    Sample,
    Select,
    Slice,
    Sort,
    SortColumnKey,
    Source,
    Tail,
    Unique,
    Unnest,
    WithColumn,
)
from planframe.schema.ir import Schema
from planframe.typing.frame_like import FrameLike

BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


def execute_plan(
    *,
    adapter: BackendAdapter[BackendFrameT, BackendExprT],
    plan: PlanNode,
    root_data: BackendFrameT,
    schema: Schema,
) -> BackendFrameT:
    """Execute a :class:`planframe.plan.nodes.PlanNode` tree.

    This is the supported public plan interpreter used by :meth:`planframe.frame.Frame.collect`.

    Important:
    - This returns the backend frame after applying the plan, but it does **not**
      call :meth:`planframe.backend.adapter.BaseAdapter.collect`.
    """

    def _compile(expr: Any) -> BackendExprT:
        try:
            return adapter.compile_expr(expr, schema=schema)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameBackendError(
                f"Failed to compile expression for backend {adapter.name}"
            ) from e

    def _compile_join_keys_tuple(
        keys: tuple[JoinKeyColumn | JoinKeyExpr, ...],
    ) -> tuple[CompiledJoinKey[BackendExprT], ...]:
        out: list[CompiledJoinKey[BackendExprT]] = []
        for k in keys:
            if isinstance(k, JoinKeyColumn):
                out.append(CompiledJoinKey(column=k.name, expr=None))
            else:
                out.append(CompiledJoinKey(column=None, expr=_compile(k.expr)))
        return tuple(out)

    def _compile_named_aggs(
        named_aggs: dict[str, tuple[str, str] | Expr[Any]],
    ) -> dict[str, tuple[str, str] | BackendExprT]:
        out: dict[str, tuple[str, str] | BackendExprT] = {}
        for name, spec in named_aggs.items():
            if (
                isinstance(spec, tuple)
                and len(spec) == 2
                and isinstance(spec[0], str)
                and isinstance(spec[1], str)
            ):
                out[name] = cast(tuple[str, str], spec)
            else:
                out[name] = _compile(spec)
        return out

    def _eval(node: object) -> BackendFrameT:
        if not isinstance(node, PlanNode):
            raise PlanFrameBackendError(f"Unsupported plan node: {type(node)!r}")

        if isinstance(node, Source):
            return root_data
        if isinstance(node, Select):
            return adapter.select(_eval(node.prev), node.columns)
        if isinstance(node, Project):
            prev = _eval(node.prev)
            parts: list[CompiledProjectItem[BackendExprT]] = []
            for it in node.items:
                if isinstance(it, ProjectPick):
                    parts.append(
                        CompiledProjectItem(name=it.column, from_column=it.column, expr=None)
                    )
                else:
                    parts.append(
                        CompiledProjectItem(
                            name=it.name,
                            from_column=None,
                            expr=_compile(it.expr),
                        )
                    )
            return adapter.project(prev, tuple(parts))
        if isinstance(node, Drop):
            return adapter.drop(_eval(node.prev), node.columns, strict=node.strict)
        if isinstance(node, Rename):
            return adapter.rename(_eval(node.prev), node.mapping, strict=node.strict)
        if isinstance(node, WithColumn):
            return adapter.with_column(_eval(node.prev), node.name, _compile(node.expr))
        if isinstance(node, Cast):
            return adapter.cast(_eval(node.prev), node.name, node.dtype)
        if isinstance(node, Filter):
            return adapter.filter(_eval(node.prev), _compile(node.predicate))
        if isinstance(node, Sort):
            prev = _eval(node.prev)
            compiled: list[CompiledSortKey[BackendExprT]] = []
            for k in node.keys:
                if isinstance(k, SortColumnKey):
                    compiled.append(CompiledSortKey(column=k.name, expr=None))
                else:
                    compiled.append(CompiledSortKey(column=None, expr=_compile(k.expr)))
            return adapter.sort(
                prev,
                tuple(compiled),
                descending=node.descending,
                nulls_last=node.nulls_last,
            )
        if isinstance(node, Unique):
            return adapter.unique(
                _eval(node.prev),
                node.subset,
                keep=node.keep,
                maintain_order=node.maintain_order,
            )
        if isinstance(node, Duplicated):
            return adapter.duplicated(
                _eval(node.prev),
                node.subset,
                keep=node.keep,
                out_name=node.out_name,
            )
        if isinstance(node, GroupBy):
            return _eval(node.prev)
        if isinstance(node, Agg):
            if not isinstance(node.prev, GroupBy):
                raise PlanFrameBackendError("Agg must follow GroupBy")
            compiled_keys = _compile_join_keys_tuple(node.prev.keys)
            compiled_aggs = _compile_named_aggs(node.named_aggs)
            return adapter.group_by_agg(
                _eval(node.prev.prev),
                keys=compiled_keys,
                named_aggs=compiled_aggs,
            )
        if isinstance(node, DropNulls):
            return adapter.drop_nulls(
                _eval(node.prev),
                node.subset,
                how=node.how,
                threshold=node.threshold,
            )
        if isinstance(node, DropNullsAll):
            return adapter.drop_nulls_all(_eval(node.prev), node.subset)
        if isinstance(node, FillNull):
            prev = _eval(node.prev)
            if node.value is not None and isinstance(node.value, Expr):
                compiled_value: object | BackendExprT = _compile(node.value)
            else:
                compiled_value = node.value
            return adapter.fill_null(prev, compiled_value, node.subset, strategy=node.strategy)
        if isinstance(node, Melt):
            return adapter.melt(
                _eval(node.prev),
                id_vars=node.id_vars,
                value_vars=node.value_vars,
                variable_name=node.variable_name,
                value_name=node.value_name,
            )
        if isinstance(node, Join):
            left_df = _eval(node.prev)
            right_frame: FrameLike = node.right
            if getattr(right_frame, "_adapter", None) is None:
                raise PlanFrameBackendError("Join node right frame is invalid")
            if getattr(right_frame._adapter, "name", None) != adapter.name:
                raise PlanFrameBackendError("Cannot join frames from different backends")
            right_df = right_frame._eval(right_frame._plan)
            if node.left_keys is node.right_keys:
                compiled = _compile_join_keys_tuple(node.left_keys)
                lo = ro = compiled
            else:
                lo = _compile_join_keys_tuple(node.left_keys)
                ro = _compile_join_keys_tuple(node.right_keys)
            return adapter.join(
                left_df,
                right_df,
                left_on=lo,
                right_on=ro,
                how=node.how,
                suffix=node.suffix,
                options=node.options,
            )
        if isinstance(node, Slice):
            return adapter.slice(_eval(node.prev), offset=node.offset, length=node.length)
        if isinstance(node, Head):
            return adapter.head(_eval(node.prev), node.n)
        if isinstance(node, Tail):
            return adapter.tail(_eval(node.prev), node.n)
        if isinstance(node, ConcatVertical):
            left_df = _eval(node.prev)
            other_frame: FrameLike = node.other
            if getattr(other_frame, "_adapter", None) is None:
                raise PlanFrameBackendError("ConcatVertical node other frame is invalid")
            if getattr(other_frame._adapter, "name", None) != adapter.name:
                raise PlanFrameBackendError("Cannot concat frames from different backends")
            right_df = other_frame._eval(other_frame._plan)
            return adapter.concat_vertical(left_df, right_df)
        if isinstance(node, ConcatHorizontal):
            left_df = _eval(node.prev)
            other_frame: FrameLike = node.other
            if getattr(other_frame, "_adapter", None) is None:
                raise PlanFrameBackendError("ConcatHorizontal node other frame is invalid")
            if getattr(other_frame._adapter, "name", None) != adapter.name:
                raise PlanFrameBackendError("Cannot concat frames from different backends")
            right_df = other_frame._eval(other_frame._plan)
            return adapter.concat_horizontal(left_df, right_df)
        if isinstance(node, Pivot):
            return adapter.pivot(
                _eval(node.prev),
                index=node.index,
                on=node.on,
                values=node.values,
                agg=node.agg,
                on_columns=node.on_columns,
                separator=node.separator,
            )
        if isinstance(node, Explode):
            return adapter.explode(_eval(node.prev), node.columns, outer=node.outer)
        if isinstance(node, Unnest):
            return adapter.unnest(_eval(node.prev), node.items)
        if isinstance(node, Posexplode):
            return adapter.posexplode(
                _eval(node.prev),
                node.column,
                pos=node.pos,
                value=node.value,
                outer=node.outer,
            )
        if isinstance(node, Sample):
            return adapter.sample(
                _eval(node.prev),
                n=node.n,
                frac=node.frac,
                with_replacement=node.with_replacement,
                shuffle=node.shuffle,
                seed=node.seed,
            )

        raise PlanFrameBackendError(f"Unsupported plan node: {type(node)!r}")

    return _eval(plan)
