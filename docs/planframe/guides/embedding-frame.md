# Embedding `Frame` in a host type (composition)

Many integrations *wrap* a PlanFrame `Frame` inside a host type instead of subclassing backend frames like `PolarsFrame`.

This guide documents Pyright-friendly patterns for doing that without copying PlanFrame’s generated stubs.

## Recommended pattern: immutable host + `inner: Frame[...]`

Use an immutable wrapper that carries the inner `Frame` and returns a new host on transforms.

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Self, TypeVar

from planframe import Frame

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


@dataclass(frozen=True)
class HostFrame(Generic[SchemaT, BackendFrameT, BackendExprT]):
    inner: Frame[SchemaT, BackendFrameT, BackendExprT]

    def select(self, *cols: str) -> Self:
        # Delegate to PlanFrame, then re-wrap.
        return type(self)(self.inner.select(*cols))

    def to_frame(self) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
        # Provide an explicit escape hatch back to the PlanFrame surface.
        return self.inner
```

### Notes

- The wrapper stays **lazy** because `Frame.select(...)` only updates the plan.
- Keep the host **immutable** (return a new instance) to match PlanFrame semantics and preserve reasoning about plans.
- Prefer a single, consistently-named attribute like **`inner`** so it’s obvious where the PlanFrame surface lives.

## Type helpers

PlanFrame ships a small protocol for “host embeds a frame”:

```python
from planframe.typing import HasInnerFrame
```

This is useful in helper functions that can accept either your host type or a raw `Frame`.

```python
from __future__ import annotations

from typing import TypeVar

from planframe import Frame
from planframe.typing import HasInnerFrame

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


def unwrap_frame(
    x: Frame[SchemaT, BackendFrameT, BackendExprT]
    | HasInnerFrame[SchemaT, BackendFrameT, BackendExprT],
) -> Frame[SchemaT, BackendFrameT, BackendExprT]:
    return x if isinstance(x, Frame) else x.inner
```

If you want to support both shapes without `isinstance(x, Frame)`, prefer a separate overload set or require `HasInnerFrame` explicitly.

## When to use `materialize_model`

Composition wrappers are often used in apps that want a “schema snapshot” at a boundary.

If your host type provides a higher-level API, consider exposing PlanFrame’s boundary:

- `inner.materialize_model("Output", kind="dataclass")`

This makes it easier for downstream users to get an explicit model type without needing full “Resolve”.

