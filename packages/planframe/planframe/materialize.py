"""Adapter-friendly materialization boundaries (columnar export + optional factory).

These mirror :meth:`planframe.frame.Frame.to_dict` / :meth:`~planframe.frame.Frame.ato_dict` with
the same :class:`~planframe.execution_options.ExecutionOptions` contract — use them when you want
a **stable import** (`from planframe.materialize import ...`) instead of calling ``Frame`` methods
directly. See the `Creating an adapter — Columnar boundary
<https://planframe.readthedocs.io/en/latest/planframe/guides/creating-an-adapter/#columnar-boundary-materialize>`__
section.

For **chunked** columnar export (optional adapter protocol, not wired here yet), see the
`Columnar streaming` design note in the PlanFrame docs and `AdapterColumnarStreamer` in
`planframe.backend.io`.

PlanFrame does not construct Pydantic/dataclass models here; supply a *factory* when needed.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from planframe.execution_options import ExecutionOptions
from planframe.frame import Frame

TOut = TypeVar("TOut")


def materialize_columns(
    frame: Frame[Any, Any, Any],
    *,
    options: ExecutionOptions | None = None,
) -> dict[str, list[object]]:
    """Return columnar data for *frame* (``dict[column_name, column_values]``).

    Thin wrapper around :meth:`planframe.frame.Frame.to_dict` — *options* are forwarded unchanged.
    """

    return frame.to_dict(options=options)


def materialize_into(
    frame: Frame[Any, Any, Any],
    factory: Callable[[dict[str, list[object]]], TOut],
    *,
    options: ExecutionOptions | None = None,
) -> TOut:
    """Materialize columns, then pass them to *factory*.

    *factory* can wrap Pydantic models, dataclasses, Arrow tables, or any custom type;
    PlanFrame stays agnostic to the output shape.
    """

    return factory(materialize_columns(frame, options=options))


async def amaterialize_columns(
    frame: Frame[Any, Any, Any],
    *,
    options: ExecutionOptions | None = None,
) -> dict[str, list[object]]:
    """Async columnar materialization (same as :meth:`~planframe.frame.Frame.ato_dict`)."""

    return await frame.ato_dict(options=options)


async def amaterialize_into(
    frame: Frame[Any, Any, Any],
    factory: Callable[[dict[str, list[object]]], TOut],
    *,
    options: ExecutionOptions | None = None,
) -> TOut:
    """Like :func:`materialize_into`, using the async ``to_dict`` path."""

    return factory(await amaterialize_columns(frame, options=options))


__all__ = [
    "amaterialize_columns",
    "amaterialize_into",
    "materialize_columns",
    "materialize_into",
]
