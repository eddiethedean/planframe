"""Shared expression / join-key compilation for :class:`~planframe.frame.Frame` and :func:`~planframe.execution.execute_plan`."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Generic, TypeVar, cast

from planframe.backend.adapter import (
    BackendAdapter,
    CompiledJoinKey,
    CompiledProjectItem,
    CompiledSortKey,
    CompileExprContext,
)
from planframe.backend.errors import PlanFrameBackendError
from planframe.expr.api import Expr
from planframe.plan.nodes import (
    JoinKeyColumn,
    JoinKeyExpr,
    ProjectExpr,
    ProjectPick,
    SortColumnKey,
    SortExprKey,
)
from planframe.schema.ir import Schema

BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


class PlanCompileContext(Generic[BackendFrameT, BackendExprT]):
    """Holds ``(adapter, schema)`` and compiles expressions and join metadata once per context."""

    __slots__ = ("_adapter", "_schema", "_resolve_backend_dtype")

    def __init__(
        self,
        adapter: BackendAdapter[BackendFrameT, BackendExprT],
        schema: Schema,
        *,
        resolve_backend_dtype: Callable[[str], object | None] | None = None,
    ) -> None:
        self._adapter = adapter
        self._schema = schema
        self._resolve_backend_dtype = resolve_backend_dtype

    def compile_expr(self, expr: object) -> BackendExprT:
        try:
            ctx = CompileExprContext(
                schema=self._schema,
                resolve_backend_dtype=self._resolve_backend_dtype,
            )
            return self._adapter.compile_expr(expr, schema=self._schema, ctx=ctx)
        except Exception as e:  # noqa: BLE001
            raise PlanFrameBackendError(
                f"Failed to compile expression for backend {self._adapter.name}"
            ) from e

    def compile_join_keys_tuple(
        self, keys: tuple[JoinKeyColumn | JoinKeyExpr, ...]
    ) -> tuple[CompiledJoinKey[BackendExprT], ...]:
        out: list[CompiledJoinKey[BackendExprT]] = []
        for k in keys:
            if isinstance(k, JoinKeyColumn):
                out.append(CompiledJoinKey(column=k.name, expr=None))
            else:
                out.append(CompiledJoinKey(column=None, expr=self.compile_expr(k.expr)))
        return tuple(out)

    def compile_named_aggs(
        self, named_aggs: dict[str, tuple[str, str] | Expr[Any]]
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
                out[name] = self.compile_expr(spec)
        return out

    def compile_sort_keys(
        self, keys: tuple[SortColumnKey | SortExprKey, ...]
    ) -> tuple[CompiledSortKey[BackendExprT], ...]:
        compiled: list[CompiledSortKey[BackendExprT]] = []
        for k in keys:
            if isinstance(k, SortColumnKey):
                compiled.append(CompiledSortKey(column=k.name, expr=None))
            else:
                compiled.append(CompiledSortKey(column=None, expr=self.compile_expr(k.expr)))
        return tuple(compiled)

    def compile_project_items(
        self, items: tuple[ProjectPick | ProjectExpr, ...]
    ) -> tuple[CompiledProjectItem[BackendExprT], ...]:
        """Lower :class:`~planframe.plan.nodes.Project` items to :class:`CompiledProjectItem`."""

        parts: list[CompiledProjectItem[BackendExprT]] = []
        for it in items:
            if isinstance(it, ProjectPick):
                parts.append(CompiledProjectItem(name=it.column, from_column=it.column, expr=None))
            else:
                parts.append(
                    CompiledProjectItem(
                        name=it.name,
                        from_column=None,
                        expr=self.compile_expr(it.expr),
                    )
                )
        return tuple(parts)
