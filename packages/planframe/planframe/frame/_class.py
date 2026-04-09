"""Concrete :class:`Frame` type composed from mixins."""

from __future__ import annotations

from typing import Generic, TypeVar

from planframe.frame._mixin_core import FramePlanMixin
from planframe.frame._mixin_io import FrameIOMixin
from planframe.frame._mixin_ops import FrameOpsMixin

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


class Frame(
    FramePlanMixin, FrameOpsMixin, FrameIOMixin, Generic[SchemaT, BackendFrameT, BackendExprT]
):
    """Typed, lazy dataframe transformation plan (see package docs)."""

    __slots__ = ("_data", "_adapter", "_plan", "_schema", "_compile_ctx")
