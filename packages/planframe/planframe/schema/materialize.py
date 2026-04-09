from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any, Literal, cast

from pydantic import BaseModel, create_model

from planframe.schema.ir import Schema


def _schema_cache_key(schema: Schema) -> tuple[tuple[str, str], ...]:
    # Schema.dtypes may not be hashable; use repr() for a stable key.
    return tuple((f.name, repr(f.dtype)) for f in schema.fields)


_MODEL_CACHE: dict[tuple[str, str, tuple[tuple[str, str], ...]], type[Any]] = {}


def materialize_dataclass(name: str, schema: Schema) -> type[Any]:
    namespace: dict[str, Any] = {"__annotations__": {}}
    for f in schema.fields:
        namespace["__annotations__"][f.name] = f.dtype
    cls = type(name, (), namespace)
    deco = cast(Callable[[type[Any]], type[Any]], dataclasses.dataclass(frozen=True))
    return deco(cls)


def materialize_pydantic(name: str, schema: Schema) -> type[BaseModel]:
    # Pydantic's `create_model` accepts field definitions as kwargs: name=(type, default).
    field_definitions: dict[str, Any] = {f.name: (f.dtype, ...) for f in schema.fields}
    return create_model(name, **field_definitions)


def materialize_model(
    name: str, schema: Schema, *, kind: Literal["dataclass", "pydantic"]
) -> type[Any]:
    cache_key = (kind, name, _schema_cache_key(schema))
    cached = _MODEL_CACHE.get(cache_key)
    if cached is not None:
        return cached

    if kind == "dataclass":
        out = materialize_dataclass(name, schema)
    else:
        out = materialize_pydantic(name, schema)
    _MODEL_CACHE[cache_key] = out
    return out
