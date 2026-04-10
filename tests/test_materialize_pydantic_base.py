from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from planframe.schema.ir import Field, Schema
from planframe.schema.materialize import materialize_pydantic


def test_materialize_pydantic_can_inherit_base() -> None:
    class MyBase(BaseModel):
        model_config = ConfigDict(extra="forbid")

    schema = Schema(fields=(Field(name="id", dtype=int), Field(name="name", dtype=str)))
    Model = materialize_pydantic("Row", schema, base=MyBase)

    assert issubclass(Model, MyBase)
    out = Model(id=1, name="a")
    assert out.id == 1
