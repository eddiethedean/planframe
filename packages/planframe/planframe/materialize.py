"""Adapter-friendly materialization boundaries (columnar export + optional factory).

Use these helpers when building adapters or host wrappers so ``Frame → columns`` and
:class:`~planframe.execution_options.ExecutionOptions` forwarding stay consistent.
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

    Delegates to :meth:`planframe.frame.Frame.to_dict` and forwards *options* unchanged.
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
    """Async columnar materialization (``Frame.ato_dict`` / ``to_dict_async``)."""

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
