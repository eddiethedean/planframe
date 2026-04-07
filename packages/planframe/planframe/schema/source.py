from __future__ import annotations

import dataclasses
from typing import Any, get_type_hints

from planframe.backend.errors import PlanFrameSchemaError
from planframe.schema.ir import Field, Schema


def schema_from_type(schema_type: type[Any]) -> Schema:
    """Build Schema IR from a dataclass type or a Pydantic model type."""

    if dataclasses.is_dataclass(schema_type):
        hints = get_type_hints(schema_type)
        fields: list[Field] = []
        for f in dataclasses.fields(schema_type):
            if f.name not in hints:
                raise PlanFrameSchemaError(f"Missing type annotation for dataclass field: {f.name}")
            fields.append(Field(name=f.name, dtype=hints[f.name]))
        return Schema(fields=tuple(fields))

    # Pydantic v1/v2 reflection without importing private APIs.
    # v2: model_fields; v1: __fields__
    if hasattr(schema_type, "model_fields"):
        mf = getattr(schema_type, "model_fields")
        fields = [Field(name=name, dtype=info.annotation) for name, info in mf.items()]
        return Schema(fields=tuple(fields))
    if hasattr(schema_type, "__fields__"):
        ff = getattr(schema_type, "__fields__")
        fields = [Field(name=name, dtype=info.outer_type_) for name, info in ff.items()]
        return Schema(fields=tuple(fields))

    raise PlanFrameSchemaError(
        "Unsupported schema type. Expected a dataclass type or a Pydantic model type."
    )
 
