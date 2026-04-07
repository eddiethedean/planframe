from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from planframe.expr.api import Expr
from planframe.plan.join_options import JoinOptions


class PlanNode:
    pass


@dataclass(frozen=True, slots=True)
class Source(PlanNode):
    schema_type: type[Any]


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
    dtype: Any


@dataclass(frozen=True, slots=True)
class Filter(PlanNode):
    prev: PlanNode
    predicate: Expr[bool]


@dataclass(frozen=True, slots=True)
class Sort(PlanNode):
    prev: PlanNode
    columns: tuple[str, ...]
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
class GroupBy(PlanNode):
    prev: PlanNode
    keys: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Agg(PlanNode):
    prev: PlanNode
    named_aggs: dict[str, tuple[str, str]]


@dataclass(frozen=True, slots=True)
class DropNulls(PlanNode):
    prev: PlanNode
    subset: tuple[str, ...] | None


@dataclass(frozen=True, slots=True)
class FillNull(PlanNode):
    prev: PlanNode
    value: Any
    subset: tuple[str, ...] | None


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
    right: Any
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
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
    other: Any


@dataclass(frozen=True, slots=True)
class Pivot(PlanNode):
    prev: PlanNode
    index: tuple[str, ...]
    on: str
    values: str
    agg: str
    on_columns: tuple[str, ...] | None
    separator: str = "_"


@dataclass(frozen=True, slots=True)
class Explode(PlanNode):
    prev: PlanNode
    column: str


@dataclass(frozen=True, slots=True)
class Unnest(PlanNode):
    prev: PlanNode
    column: str


@dataclass(frozen=True, slots=True)
class ConcatHorizontal(PlanNode):
    prev: PlanNode
    other: Any


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
