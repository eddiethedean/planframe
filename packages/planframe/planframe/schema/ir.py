from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from planframe.backend.errors import PlanFrameSchemaError

PFType = Any


@dataclass(frozen=True, slots=True)
class Field:
    name: str
    dtype: PFType


@dataclass(frozen=True, slots=True)
class Schema:
    fields: tuple[Field, ...]

    def names(self) -> tuple[str, ...]:
        return tuple(f.name for f in self.fields)

    def field_map(self) -> dict[str, Field]:
        return {f.name: f for f in self.fields}

    def get(self, name: str) -> Field:
        fm = self.field_map()
        try:
            return fm[name]
        except KeyError as e:
            raise PlanFrameSchemaError(f"Unknown column: {name}") from e

    def select(self, columns: Iterable[str]) -> Schema:
        fm = self.field_map()
        out: list[Field] = []
        for c in columns:
            if c not in fm:
                raise PlanFrameSchemaError(f"Cannot select missing column: {c}")
            out.append(fm[c])
        return Schema(fields=tuple(out))

    def drop(self, columns: Iterable[str]) -> Schema:
        drop_set = set(columns)
        fm = self.field_map()
        missing = drop_set.difference(fm.keys())
        if missing:
            raise PlanFrameSchemaError(f"Cannot drop missing columns: {sorted(missing)}")
        return Schema(fields=tuple(f for f in self.fields if f.name not in drop_set))

    def rename(self, mapping: dict[str, str]) -> Schema:
        fm = self.field_map()
        missing = set(mapping.keys()).difference(fm.keys())
        if missing:
            raise PlanFrameSchemaError(f"Cannot rename missing columns: {sorted(missing)}")

        new_names = list(self.names())
        for i, old in enumerate(new_names):
            if old in mapping:
                new_names[i] = mapping[old]

        if len(set(new_names)) != len(new_names):
            raise PlanFrameSchemaError("Rename would create duplicate column names.")

        out_fields: list[Field] = []
        for f in self.fields:
            out_fields.append(Field(name=mapping.get(f.name, f.name), dtype=f.dtype))
        return Schema(fields=tuple(out_fields))

    def with_column(self, name: str, dtype: PFType) -> Schema:
        fm = self.field_map()
        if name in fm:
            out = [
                Field(name=f.name, dtype=(dtype if f.name == name else f.dtype)) for f in self.fields
            ]
            return Schema(fields=tuple(out))
        return Schema(fields=tuple([*self.fields, Field(name=name, dtype=dtype)]))

    def cast(self, name: str, dtype: PFType) -> Schema:
        if name not in self.field_map():
            raise PlanFrameSchemaError(f"Cannot cast missing column: {name}")
        return self.with_column(name=name, dtype=dtype)

    def select_exclude(self, columns: Iterable[str]) -> Schema:
        drop_set = set(columns)
        fm = self.field_map()
        missing = drop_set.difference(fm.keys())
        if missing:
            raise PlanFrameSchemaError(f"Cannot exclude missing columns: {sorted(missing)}")
        return Schema(fields=tuple(f for f in self.fields if f.name not in drop_set))

    def reorder_columns(self, columns: Iterable[str]) -> Schema:
        cols = tuple(columns)
        fm = self.field_map()
        missing = set(cols).difference(fm.keys())
        if missing:
            raise PlanFrameSchemaError(f"Cannot reorder with missing columns: {sorted(missing)}")
        if len(set(cols)) != len(cols):
            raise PlanFrameSchemaError("Cannot reorder with duplicate column names.")
        if set(cols) != set(fm.keys()):
            extra = set(fm.keys()).difference(cols)
            raise PlanFrameSchemaError(
                "reorder_columns must include every column exactly once; "
                f"missing from new order: {sorted(extra)}"
            )
        return Schema(fields=tuple(fm[c] for c in cols))

    def select_first(self, columns: Iterable[str]) -> Schema:
        cols = tuple(columns)
        fm = self.field_map()
        missing = set(cols).difference(fm.keys())
        if missing:
            raise PlanFrameSchemaError(f"Cannot select_first missing columns: {sorted(missing)}")
        if len(set(cols)) != len(cols):
            raise PlanFrameSchemaError("Cannot select_first with duplicate column names.")
        rest = [n for n in self.names() if n not in set(cols)]
        return Schema(fields=tuple([*(fm[c] for c in cols), *(fm[r] for r in rest)]))

    def select_last(self, columns: Iterable[str]) -> Schema:
        cols = tuple(columns)
        fm = self.field_map()
        missing = set(cols).difference(fm.keys())
        if missing:
            raise PlanFrameSchemaError(f"Cannot select_last missing columns: {sorted(missing)}")
        if len(set(cols)) != len(cols):
            raise PlanFrameSchemaError("Cannot select_last with duplicate column names.")
        front = [n for n in self.names() if n not in set(cols)]
        return Schema(fields=tuple([*(fm[f] for f in front), *(fm[c] for c in cols)]))

    def move(self, column: str, *, before: str | None = None, after: str | None = None) -> Schema:
        if (before is None) == (after is None):
            raise PlanFrameSchemaError("move requires exactly one of before= or after=")
        fm = self.field_map()
        if column not in fm:
            raise PlanFrameSchemaError(f"Cannot move missing column: {column}")
        anchor = before if before is not None else after
        if anchor not in fm:
            raise PlanFrameSchemaError(f"Cannot move relative to missing column: {anchor}")
        if anchor == column:
            return self

        names = [n for n in self.names() if n != column]
        idx = names.index(anchor)
        insert_at = idx if before is not None else idx + 1
        names.insert(insert_at, column)
        return Schema(fields=tuple(fm[n] for n in names))

    def unique(self) -> Schema:
        return self

    def duplicated(self, *, out_name: str = "duplicated") -> Schema:
        return Schema(fields=(Field(name=out_name, dtype=bool),))

    def drop_nulls(self) -> Schema:
        return self

    def drop_nulls_all(self) -> Schema:
        return self

    def fill_null(self) -> Schema:
        return self

    def explode(self, column: str) -> Schema:
        self.get(column)  # validate
        return self

    def unnest(self, column: str, *, fields: tuple[str, ...]) -> Schema:
        if not fields:
            raise PlanFrameSchemaError("unnest requires non-empty fields")
        fm = self.field_map()
        if column not in fm:
            raise PlanFrameSchemaError(f"Cannot unnest missing column: {column}")
        if len(set(fields)) != len(fields):
            raise PlanFrameSchemaError("unnest fields must be unique")
        remaining = [f for f in self.fields if f.name != column]
        out_names = {f.name for f in remaining}
        for name in fields:
            if name in out_names:
                raise PlanFrameSchemaError(f"unnest would create duplicate column name: {name}")
            out_names.add(name)
            remaining.append(Field(name=name, dtype=object))
        return Schema(fields=tuple(remaining))

    def melt(
        self,
        *,
        id_vars: tuple[str, ...],
        value_vars: tuple[str, ...],
        variable_name: str,
        value_name: str,
    ) -> Schema:
        fm = self.field_map()
        missing = set(id_vars).difference(fm.keys()) | set(value_vars).difference(fm.keys())
        if missing:
            raise PlanFrameSchemaError(f"Cannot melt missing columns: {sorted(missing)}")
        out: list[Field] = [fm[c] for c in id_vars]
        out.append(Field(name=variable_name, dtype=str))
        out.append(Field(name=value_name, dtype=object))
        if len({f.name for f in out}) != len(out):
            raise PlanFrameSchemaError("melt would create duplicate column names")
        return Schema(fields=tuple(out))

    def join_merge(
        self,
        right: Schema,
        *,
        on: tuple[str, ...],
        suffix: str = "_right",
    ) -> Schema:
        if not on:
            raise PlanFrameSchemaError("join_merge requires non-empty on keys")

        left_map = self.field_map()
        right_map = right.field_map()

        missing_left = set(on).difference(left_map.keys())
        missing_right = set(on).difference(right_map.keys())
        if missing_left:
            raise PlanFrameSchemaError(f"Join keys missing on left: {sorted(missing_left)}")
        if missing_right:
            raise PlanFrameSchemaError(f"Join keys missing on right: {sorted(missing_right)}")

        out_fields: list[Field] = list(self.fields)
        out_names = {f.name for f in out_fields}

        for rf in right.fields:
            if rf.name in on:
                continue  # drop right join keys
            name = rf.name
            if name in out_names:
                name = f"{name}{suffix}"
            if name in out_names:
                raise PlanFrameSchemaError(f"Join suffix collision for column: {rf.name!r}")
            out_names.add(name)
            out_fields.append(Field(name=name, dtype=rf.dtype))

        return Schema(fields=tuple(out_fields))
 
