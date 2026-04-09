"""Frame lifecycle: repr, source, schema, plan optimization, compilation, eval."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from planframe.backend.adapter import BackendAdapter, CompiledJoinKey
from planframe.backend.errors import PlanFrameBackendError
from planframe.compile_context import PlanCompileContext
from planframe.execution import execute_plan
from planframe.expr.api import Expr
from planframe.plan.nodes import JoinKeyColumn, JoinKeyExpr, PlanNode, Source
from planframe.plan.optimize import optimize_plan
from planframe.schema.ir import Schema
from planframe.schema.source import schema_from_type

if TYPE_CHECKING:
    from planframe.frame._class import Frame

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


class FramePlanMixin(Generic[SchemaT, BackendFrameT, BackendExprT]):
    """Construction, schema/plan access, compilation, and plan execution."""

    __slots__ = ()

    def __init__(
        self,
        _data: BackendFrameT,
        _adapter: BackendAdapter[BackendFrameT, BackendExprT],
        _plan: PlanNode,
        _schema: Schema,
    ) -> None:
        self._data = _data
        self._adapter = _adapter
        self._plan = _plan
        self._schema = _schema
        self._compile_ctx = None

    def __repr__(self) -> str:
        # Keep repr cheap: never execute, never compile expressions, and keep traversal bounded.
        verbose = os.getenv("PLANFRAME_REPR_VERBOSE", "").lower() in {"1", "true", "yes", "on"}

        cols = self._schema.names()
        col_preview_limit = 12 if verbose else 6
        if len(cols) <= col_preview_limit:
            cols_preview = ", ".join(cols)
        else:
            head = ", ".join(cols[:col_preview_limit])
            cols_preview = f"{head}, …(+{len(cols) - col_preview_limit})"

        # Plan shape: follow the primary `prev` chain for a small number of nodes.
        plan_limit = 20 if verbose else 6
        kinds: list[str] = []
        node: PlanNode | None = self._plan
        schema_type_name: str | None = None
        steps = 0
        while isinstance(node, PlanNode) and steps < plan_limit:
            kinds.append(type(node).__name__)
            if isinstance(node, Source) and schema_type_name is None:
                schema_type_name = getattr(node.schema_type, "__name__", None)
            prev = getattr(node, "prev", None)
            node = prev if isinstance(prev, PlanNode) else None
            steps += 1

        if node is not None:
            kinds.append("…")

        plan_preview = "->".join(kinds)
        schema_tag = f"[{schema_type_name}]" if schema_type_name else ""
        adapter_tag = f", adapter={self._adapter.name!r}" if verbose else ""
        return f"Frame{schema_tag}(cols={len(cols)} [{cols_preview}], plan={plan_preview}{adapter_tag})"

    @classmethod
    def source(
        cls,
        data: BackendFrameT,
        *,
        adapter: BackendAdapter[BackendFrameT, BackendExprT],
        schema: type[SchemaT],
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        schema_ir = schema_from_type(schema)
        return cls(
            _data=data,
            _adapter=adapter,
            _plan=Source(schema_type=schema, ir_version=1),
            _schema=schema_ir,
        )

    def schema(self) -> Schema:
        return self._schema

    def plan(self) -> PlanNode:
        return self._plan

    def optimize(
        self, *, level: Literal[0, 1, 2] = 1
    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        """Return a new Frame with an optimized plan.

        This is opt-in and performs only backend-independent, semantics-preserving rewrites.
        """

        if level == 0:
            return self
        plan2 = optimize_plan(self._plan, level=level)
        if plan2 is self._plan:
            return self
        return type(self)(
            _data=self._data, _adapter=self._adapter, _plan=plan2, _schema=self._schema
        )

    @property
    def _plan_compiler(self) -> PlanCompileContext[BackendFrameT, BackendExprT]:
        ctx = self._compile_ctx
        if ctx is None:
            ctx = PlanCompileContext(self._adapter, self._schema)
            self._compile_ctx = ctx
        return ctx

    def _compile(self, expr: object) -> BackendExprT:
        return self._plan_compiler.compile_expr(expr)

    def _compile_join_keys_tuple(
        self, keys: tuple[JoinKeyColumn | JoinKeyExpr, ...]
    ) -> tuple[CompiledJoinKey[BackendExprT], ...]:
        return self._plan_compiler.compile_join_keys_tuple(keys)

    def _compile_named_aggs(
        self, named_aggs: dict[str, tuple[str, str] | Expr[Any]]
    ) -> dict[str, tuple[str, str] | BackendExprT]:
        return self._plan_compiler.compile_named_aggs(named_aggs)

    def _normalize_join_keys(
        self, items: tuple[str | Expr[Any], ...]
    ) -> tuple[JoinKeyColumn | JoinKeyExpr, ...]:
        out: list[JoinKeyColumn | JoinKeyExpr] = []
        for x in items:
            if isinstance(x, str):
                out.append(JoinKeyColumn(name=x))
            elif isinstance(x, Expr):
                out.append(JoinKeyExpr(expr=x))
            else:
                raise TypeError(
                    f"join keys must be column names (str) or Expr, got {type(x).__name__!r}"
                )
        return tuple(out)

    def _eval(self, node: object) -> BackendFrameT:
        if not isinstance(node, PlanNode):
            raise PlanFrameBackendError(f"Unsupported plan node: {type(node)!r}")
        return execute_plan(
            adapter=self._adapter,
            plan=node,
            root_data=self._data,
            schema=self._schema,
        )
