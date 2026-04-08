from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol, cast, runtime_checkable

from planframe.backend.errors import PlanFrameSchemaError
from planframe.schema.ir import Schema


@runtime_checkable
class ColumnSelector(Protocol):
    """Schema-only column selection protocol (no backend dependency)."""

    def select(self, schema: Schema) -> tuple[str, ...]: ...


@dataclass(frozen=True, slots=True)
class ByName(ColumnSelector):
    names: tuple[str, ...]

    def select(self, schema: Schema) -> tuple[str, ...]:
        # preserve user order
        for n in self.names:
            schema.get(n)
        return self.names


@dataclass(frozen=True, slots=True)
class Prefix(ColumnSelector):
    prefix: str

    def select(self, schema: Schema) -> tuple[str, ...]:
        return tuple(n for n in schema.names() if n.startswith(self.prefix))


@dataclass(frozen=True, slots=True)
class Suffix(ColumnSelector):
    suffix: str

    def select(self, schema: Schema) -> tuple[str, ...]:
        return tuple(n for n in schema.names() if n.endswith(self.suffix))


@dataclass(frozen=True, slots=True)
class Regex(ColumnSelector):
    pattern: str

    def select(self, schema: Schema) -> tuple[str, ...]:
        rx = re.compile(self.pattern)
        return tuple(n for n in schema.names() if rx.search(n))


@dataclass(frozen=True, slots=True)
class DType(ColumnSelector):
    is_subclass: type[object] | None = None
    is_exact: type[object] | None = None

    def __post_init__(self) -> None:
        if self.is_subclass is None and self.is_exact is None:
            raise ValueError("dtype selector requires is_subclass= or is_exact=")
        if self.is_subclass is not None and self.is_exact is not None:
            raise ValueError("dtype selector accepts only one of is_subclass= or is_exact=")

    def select(self, schema: Schema) -> tuple[str, ...]:
        out: list[str] = []
        for f in schema.fields:
            dt = f.dtype
            if not isinstance(dt, type):
                continue
            if self.is_exact is not None:
                if dt is self.is_exact:
                    out.append(f.name)
            else:
                base = cast(type[object], self.is_subclass)
                if issubclass(dt, base):
                    out.append(f.name)
        return tuple(out)


@dataclass(frozen=True, slots=True)
class Union(ColumnSelector):
    left: ColumnSelector
    right: ColumnSelector

    def select(self, schema: Schema) -> tuple[str, ...]:
        left_cols = self.left.select(schema)
        right_cols = self.right.select(schema)
        seen: set[str] = set()
        out: list[str] = []
        for n in (*left_cols, *right_cols):
            if n not in seen:
                seen.add(n)
                out.append(n)
        return tuple(out)


@dataclass(frozen=True, slots=True)
class Intersection(ColumnSelector):
    left: ColumnSelector
    right: ColumnSelector

    def select(self, schema: Schema) -> tuple[str, ...]:
        left_cols = self.left.select(schema)
        rset = set(self.right.select(schema))
        return tuple(n for n in left_cols if n in rset)


@dataclass(frozen=True, slots=True)
class Difference(ColumnSelector):
    left: ColumnSelector
    right: ColumnSelector

    def select(self, schema: Schema) -> tuple[str, ...]:
        left_cols = self.left.select(schema)
        rset = set(self.right.select(schema))
        return tuple(n for n in left_cols if n not in rset)


def by_name(*names: str) -> ByName:
    if not names:
        raise ValueError("by_name requires at least one name")
    if len(set(names)) != len(names):
        raise ValueError("by_name names must be unique")
    return ByName(names=tuple(names))


def prefix(value: str) -> Prefix:
    if not value:
        raise ValueError("prefix selector requires non-empty prefix")
    return Prefix(prefix=value)


def suffix(value: str) -> Suffix:
    if not value:
        raise ValueError("suffix selector requires non-empty suffix")
    return Suffix(suffix=value)


def regex(pattern: str) -> Regex:
    if not pattern:
        raise ValueError("regex selector requires non-empty pattern")
    return Regex(pattern=pattern)


def dtype(
    *, is_subclass: type[object] | None = None, is_exact: type[object] | None = None
) -> DType:
    return DType(is_subclass=is_subclass, is_exact=is_exact)


def _apply_strict(
    *, cols: tuple[str, ...], strict: bool, selector: ColumnSelector
) -> tuple[str, ...]:
    if cols:
        return cols
    if strict:
        raise PlanFrameSchemaError(f"select_schema matched no columns for selector={selector!r}")
    return cols
