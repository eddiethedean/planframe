from __future__ import annotations

import dataclasses
from typing import Any, Literal

from pydantic import BaseModel, create_model

from planframe.schema.ir import Schema


def materialize_dataclass(name: str, schema: Schema) -> type[Any]:
    namespace: dict[str, Any] = {"__annotations__": {}}
    for f in schema.fields:
        namespace["__annotations__"][f.name] = f.dtype
    cls = type(name, (), namespace)
    return dataclasses.dataclass(frozen=True)(cls)


def materialize_pydantic(name: str, schema: Schema) -> type[BaseModel]:
    # Pydantic's `create_model` accepts field definitions as kwargs: name=(type, default).
    field_definitions: dict[str, Any] = {f.name: (f.dtype, ...) for f in schema.fields}
    return create_model(name, **field_definitions)


def materialize_model(
    name: str, schema: Schema, *, kind: Literal["dataclass", "pydantic"]
) -> type[Any]:
    if kind == "dataclass":
        return materialize_dataclass(name, schema)
    return materialize_pydantic(name, schema)
