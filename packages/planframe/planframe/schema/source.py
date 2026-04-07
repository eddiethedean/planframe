from __future__ import annotations

import dataclasses
from typing import Any, get_type_hints

from planframe.backend.errors import PlanFrameSchemaError
from planframe.schema.ir import Field, Schema


def schema_from_type(schema_type: type[Any]) -> Schema:
    """Build Schema IR from a dataclass type or a Pydantic model type."""

    # PlanFrameModel-style: plain class with annotations only.
    # NOTE: This must come before the dataclass branch because subclasses of our
    # core `Frame` inherit dataclass internals and will otherwise be mistaken for
    # schema dataclasses.
    if getattr(schema_type, "__planframe_model__", False):
        # Use only annotations defined directly on the class; avoid inheriting annotations
        # from base `Frame`/dataclass internals.
        raw = dict(getattr(schema_type, "__dict__", {}).get("__annotations__", {}))
        hints = get_type_hints(schema_type, include_extras=True)
        hints = {k: hints[k] for k in raw if k in hints}
        if not hints:
            raise PlanFrameSchemaError("PlanFrameModel schema must have type annotations")
        fields = [Field(name=name, dtype=tp) for name, tp in hints.items()]
        return Schema(fields=tuple(fields))

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
        mf = schema_type.model_fields
        fields = [Field(name=name, dtype=info.annotation) for name, info in mf.items()]
        return Schema(fields=tuple(fields))
    if hasattr(schema_type, "__fields__"):
        ff = schema_type.__fields__
        fields = [Field(name=name, dtype=info.outer_type_) for name, info in ff.items()]
        return Schema(fields=tuple(fields))

    raise PlanFrameSchemaError(
        "Unsupported schema type. Expected a dataclass type or a Pydantic model type."
    )
