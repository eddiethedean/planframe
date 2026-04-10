# Optional Pydantic bridge for `materialize_model`

PlanFrame’s `materialize_model(...)` boundary produces an exact row model from the derived schema.

If you already have a Pydantic `BaseModel` you want to use as the “single source of truth” for validation (or you want to reuse a shared base class / config), you can bridge in a few simple ways.

## 1) Generate a Pydantic model directly from a `Frame`

```python
RowModel = pf.materialize_model("RowModel", kind="pydantic")
rows = pf.to_dicts()
validated = [RowModel(**r) for r in rows]
```

This uses PlanFrame’s derived schema as the authoritative field set and types.

## 2) Generate a Pydantic model that inherits your own base class

PlanFrame exposes `materialize_pydantic(..., base=...)` for cases where you want custom validation config, mixins, or shared methods on your Pydantic models.

```python
from pydantic import BaseModel, ConfigDict

from planframe.schema.materialize import materialize_pydantic


class MyBase(BaseModel):
    model_config = ConfigDict(extra="forbid")


RowModel = materialize_pydantic("RowModel", pf.schema(), base=MyBase)
```

## 3) Validate PlanFrame rows against an existing Pydantic model

If you already have a Pydantic model type, validate PlanFrame’s row dicts using the Pydantic API:

```python
from pydantic import BaseModel


class User(BaseModel):
    id: int
    name: str


rows = pf.to_dicts()
validated = [User.model_validate(r) for r in rows]  # Pydantic v2
```

### Trade-offs

- Validating against a pre-existing model can be great for “single source of truth”, but it may not match PlanFrame’s derived schema exactly (missing/extra fields, different optionality).
- `materialize_model(..., kind="pydantic")` is best when you want PlanFrame’s schema to be authoritative at that boundary.

