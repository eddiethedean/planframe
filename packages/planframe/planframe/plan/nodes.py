from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from planframe.expr.api import Expr
from planframe.plan.join_options import JoinOptions
from planframe.typing.frame_like import FrameLike
from planframe.typing.scalars import Scalar


class PlanNode:
    pass


@dataclass(frozen=True, slots=True)
class Source(PlanNode):
    schema_type: type[object]
    ir_version: int = 1


@dataclass(frozen=True, slots=True)
class Select(PlanNode):
    prev: PlanNode
    columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ProjectPick:
    """Existing column to include in a :class:`Project` (output name equals *column*)."""

    column: str


@dataclass(frozen=True, slots=True)
class ProjectExpr:
    """Computed column: *name* is the output name, *expr* the PlanFrame expression IR."""

    name: str
    expr: Expr[Any]


@dataclass(frozen=True, slots=True)
class Project(PlanNode):
    """Mixed projection: existing columns (:class:`ProjectPick`) and computed (:class:`ProjectExpr`)."""

    prev: PlanNode
    items: tuple[ProjectPick | ProjectExpr, ...]


@dataclass(frozen=True, slots=True)
class Drop(PlanNode):
    prev: PlanNode
    columns: tuple[str, ...]
    strict: bool = True


@dataclass(frozen=True, slots=True)
class Rename(PlanNode):
    prev: PlanNode
    mapping: dict[str, str]
    strict: bool = True


@dataclass(frozen=True, slots=True)
class WithColumn(PlanNode):
    prev: PlanNode
    name: str
    expr: Expr[Any]


@dataclass(frozen=True, slots=True)
class Cast(PlanNode):
    prev: PlanNode
    name: str
    dtype: object


@dataclass(frozen=True, slots=True)
class Filter(PlanNode):
    prev: PlanNode
    predicate: Expr[bool]


@dataclass(frozen=True, slots=True)
class SortColumnKey:
    """Sort by an existing column named *name*."""

    name: str


@dataclass(frozen=True, slots=True)
class SortExprKey:
    """Sort by a computed expression (schema unchanged; ordering only)."""

    expr: Expr[Any]


@dataclass(frozen=True, slots=True)
class Sort(PlanNode):
    prev: PlanNode
    keys: tuple[SortColumnKey | SortExprKey, ...]
    descending: tuple[bool, ...]
    nulls_last: tuple[bool, ...]


@dataclass(frozen=True, slots=True)
class Unique(PlanNode):
    prev: PlanNode
    subset: tuple[str, ...] | None
    keep: str = "first"
    maintain_order: bool = False


@dataclass(frozen=True, slots=True)
class Duplicated(PlanNode):
    prev: PlanNode
    subset: tuple[str, ...] | None
    keep: str | bool = "first"
    out_name: str = "duplicated"


@dataclass(frozen=True, slots=True)
class JoinKeyColumn:
    """Existing column name as a join or group-by key."""

    name: str


@dataclass(frozen=True, slots=True)
class JoinKeyExpr:
    """Expression as a join or group-by key."""

    expr: Expr[Any]


@dataclass(frozen=True, slots=True)
class GroupBy(PlanNode):
    prev: PlanNode
    keys: tuple[JoinKeyColumn | JoinKeyExpr, ...]


@dataclass(frozen=True, slots=True)
class Agg(PlanNode):
    prev: PlanNode
    named_aggs: dict[str, tuple[str, str] | Expr[Any]]


@dataclass(frozen=True, slots=True)
class DynamicGroupByAgg(PlanNode):
    prev: PlanNode
    index_column: str
    every: str
    period: str | None
    by: tuple[str, ...] | None
    named_aggs: dict[str, tuple[str, str] | Expr[Any]]


@dataclass(frozen=True, slots=True)
class RollingAgg(PlanNode):
    prev: PlanNode
    on: str
    column: str
    window_size: int | str
    op: str
    out_name: str
    by: tuple[str, ...] | None
    min_periods: int = 1


@dataclass(frozen=True, slots=True)
class DropNulls(PlanNode):
    prev: PlanNode
    subset: tuple[str, ...] | None
    how: Literal["any", "all"] = "any"
    threshold: int | None = None


@dataclass(frozen=True, slots=True)
class FillNull(PlanNode):
    prev: PlanNode
    # One of value or strategy must be provided.
    # - value may be a literal or an Expr (for per-cell fill)
    # - strategy names a backend-defined fill strategy (forward/backward/etc.)
    value: Scalar | Expr[Any] | None
    subset: tuple[str, ...] | None
    strategy: str | None = None


@dataclass(frozen=True, slots=True)
class Melt(PlanNode):
    prev: PlanNode
    id_vars: tuple[str, ...]
    value_vars: tuple[str, ...]
    variable_name: str
    value_name: str


@dataclass(frozen=True, slots=True)
class Join(PlanNode):
    prev: PlanNode
    right: FrameLike
    left_keys: tuple[JoinKeyColumn | JoinKeyExpr, ...]
    right_keys: tuple[JoinKeyColumn | JoinKeyExpr, ...]
    how: str = "inner"
    suffix: str = "_right"
    options: JoinOptions | None = None


@dataclass(frozen=True, slots=True)
class Slice(PlanNode):
    prev: PlanNode
    offset: int
    length: int | None


@dataclass(frozen=True, slots=True)
class Head(PlanNode):
    prev: PlanNode
    n: int


@dataclass(frozen=True, slots=True)
class Tail(PlanNode):
    prev: PlanNode
    n: int


@dataclass(frozen=True, slots=True)
class ConcatVertical(PlanNode):
    prev: PlanNode
    other: FrameLike


@dataclass(frozen=True, slots=True)
class Pivot(PlanNode):
    prev: PlanNode
    index: tuple[str, ...]
    on: str
    values: tuple[str, ...]
    agg: str
    on_columns: tuple[str, ...] | None
    separator: str = "_"
    sort_columns: bool = False


@dataclass(frozen=True, slots=True)
class Explode(PlanNode):
    prev: PlanNode
    columns: tuple[str, ...]
    outer: bool = False


@dataclass(frozen=True, slots=True)
class Unnest(PlanNode):
    prev: PlanNode
    items: tuple[UnnestItem, ...]


@dataclass(frozen=True, slots=True)
class UnnestItem:
    column: str
    fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Posexplode(PlanNode):
    prev: PlanNode
    column: str
    pos: str = "pos"
    value: str | None = None
    outer: bool = False


@dataclass(frozen=True, slots=True)
class ConcatHorizontal(PlanNode):
    prev: PlanNode
    other: FrameLike


@dataclass(frozen=True, slots=True)
class DropNullsAll(PlanNode):
    prev: PlanNode
    subset: tuple[str, ...] | None


@dataclass(frozen=True, slots=True)
class Sample(PlanNode):
    prev: PlanNode
    n: int | None
    frac: float | None
    with_replacement: bool = False
    shuffle: bool = False
    seed: int | None = None
