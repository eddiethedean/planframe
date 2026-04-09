"""A tiny pandas-like Series wrapper over PlanFrame Expr.

This is intentionally minimal: it's an expression container (not data) so it can
participate in `PandasLikeFrame.assign(...)`, `query`-style filters, etc.
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar, cast

from planframe.expr.api import (
    Col,
    Expr,
    add,
    and_,
    eq,
    ge,
    gt,
    le,
    lit,
    lt,
    mul,
    ne,
    not_,
    or_,
    sub,
    truediv,
)

T = TypeVar("T")


def _to_expr(value: object) -> Expr[Any]:
    if isinstance(value, Series):
        return cast(Expr[Any], value._expr)
    if isinstance(value, Expr):
        return cast(Expr[Any], value)
    return lit(value)


class Series(Generic[T]):
    """Expression-like Series wrapper."""

    __slots__ = ("_expr", "name")

    def __init__(self, expr: Expr[T], *, name: str | None = None) -> None:
        self._expr = expr
        self.name = name

    @property
    def expr(self) -> Expr[T]:
        return self._expr

    # Common pandas-ish comparison / boolean ops
    def __eq__(self, other: object) -> Any:  # noqa: D105
        return Series(eq(cast(Expr[object], self._expr), _to_expr(other)))

    def __ne__(self, other: object) -> Any:  # noqa: D105
        return Series(ne(cast(Expr[object], self._expr), _to_expr(other)))

    def __lt__(self, other: object) -> Series[bool]:
        return Series(lt(cast(Expr[object], self._expr), _to_expr(other)))

    def __le__(self, other: object) -> Series[bool]:
        return Series(le(cast(Expr[object], self._expr), _to_expr(other)))

    def __gt__(self, other: object) -> Series[bool]:
        return Series(gt(cast(Expr[object], self._expr), _to_expr(other)))

    def __ge__(self, other: object) -> Series[bool]:
        return Series(ge(cast(Expr[object], self._expr), _to_expr(other)))

    def __and__(self, other: object) -> Series[bool]:
        rhs = _to_expr(other)
        return Series(and_(cast(Expr[bool], self._expr), cast(Expr[bool], rhs)))

    def __or__(self, other: object) -> Series[bool]:
        rhs = _to_expr(other)
        return Series(or_(cast(Expr[bool], self._expr), cast(Expr[bool], rhs)))

    def __invert__(self) -> Series[bool]:
        return Series(not_(cast(Expr[bool], self._expr)))

    # Arithmetic
    def __add__(self, other: object) -> Series:
        return Series(add(cast(Expr[object], self._expr), _to_expr(other)))

    def __radd__(self, other: object) -> Series:
        return Series(add(_to_expr(other), cast(Expr[object], self._expr)))

    def __sub__(self, other: object) -> Series:
        return Series(sub(cast(Expr[object], self._expr), _to_expr(other)))

    def __rsub__(self, other: object) -> Series:
        return Series(sub(_to_expr(other), cast(Expr[object], self._expr)))

    def __mul__(self, other: object) -> Series:
        return Series(mul(cast(Expr[object], self._expr), _to_expr(other)))

    def __rmul__(self, other: object) -> Series:
        return Series(mul(_to_expr(other), cast(Expr[object], self._expr)))

    def __truediv__(self, other: object) -> Series:
        return Series(truediv(cast(Expr[object], self._expr), _to_expr(other)))

    def __rtruediv__(self, other: object) -> Series:
        return Series(truediv(_to_expr(other), cast(Expr[object], self._expr)))

    def __bool__(self) -> bool:
        raise TypeError("Series truth value is ambiguous; use it inside a filter on the Frame.")

    def rename(self, name: str) -> Series[T]:
        return Series(self._expr, name=name)


def series_from_key(key: str | Expr[Any] | Series[Any]) -> Series[Any]:
    if isinstance(key, Series):
        return key
    if isinstance(key, Expr):
        if isinstance(key, Col):
            return Series(key, name=key.name)
        return Series(key)
    return Series(Col(name=key), name=key)


__all__ = ["Series", "series_from_key"]
