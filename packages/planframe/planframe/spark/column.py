"""PySpark-flavored `Column` wrapper over PlanFrame `Expr` nodes."""

from __future__ import annotations

from typing import Any, Generic, TypeVar, cast

from planframe.expr.api import (
    Expr,
    abs_,
    add,
    and_,
    between,
    ceil,
    contains,
    ends_with,
    eq,
    exp,
    floor,
    ge,
    gt,
    is_not_null,
    is_null,
    isin,
    le,
    log,
    lt,
    mul,
    ne,
    not_,
    or_,
    pow_,
    round_,
    starts_with,
    sub,
    truediv,
    xor,
)
from planframe.expr.api import (
    lit as lit_expr,
)
from planframe.expr.api import (
    sqrt as expr_sqrt,
)

T = TypeVar("T")


def lit_value(other: object) -> Expr[Any]:
    if isinstance(other, Column):
        return cast(Expr[Any], other._expr)
    if isinstance(other, Expr):
        return cast(Expr[Any], other)
    return lit_expr(other)


class Column(Generic[T]):
    """Expression column with PySpark-style operators (PlanFrame IR underneath)."""

    __slots__ = ("_expr", "_alias")

    def __init__(self, expr: Expr[T], *, alias: str | None = None) -> None:
        self._expr = expr
        self._alias = alias

    @property
    def expr(self) -> Expr[T]:
        return self._expr

    @property
    def alias_name(self) -> str | None:
        return self._alias

    def alias(self, name: str) -> Column:
        return Column(self._expr, alias=name)

    def isNull(self) -> Column[bool]:  # noqa: N802
        return Column(is_null(cast(Expr[object], self._expr)))

    def isNotNull(self) -> Column[bool]:  # noqa: N802
        return Column(is_not_null(cast(Expr[object], self._expr)))

    def isin(self, *other: object) -> Column[bool]:  # noqa: A003
        return Column(isin(cast(Expr[object], self._expr), *other))

    def __add__(self, other: object) -> Column:
        return Column(add(cast(Expr[object], self._expr), lit_value(other)))

    def __radd__(self, other: object) -> Column:
        return Column(add(lit_value(other), cast(Expr[object], self._expr)))

    def __sub__(self, other: object) -> Column:
        return Column(sub(cast(Expr[object], self._expr), lit_value(other)))

    def __rsub__(self, other: object) -> Column:
        return Column(sub(lit_value(other), cast(Expr[object], self._expr)))

    def __mul__(self, other: object) -> Column:
        return Column(mul(cast(Expr[object], self._expr), lit_value(other)))

    def __rmul__(self, other: object) -> Column:
        return Column(mul(lit_value(other), cast(Expr[object], self._expr)))

    def __truediv__(self, other: object) -> Column:
        return Column(truediv(cast(Expr[object], self._expr), lit_value(other)))

    def __rtruediv__(self, other: object) -> Column:
        return Column(truediv(lit_value(other), cast(Expr[object], self._expr)))

    def __pow__(self, other: object) -> Column:
        return Column(pow_(cast(Expr[object], self._expr), lit_value(other)))

    def __rpow__(self, other: object) -> Column:
        return Column(pow_(lit_value(other), cast(Expr[object], self._expr)))

    def __eq__(self, other: object) -> Any:  # noqa: D105
        return Column(eq(cast(Expr[object], self._expr), lit_value(other)))

    def __ne__(self, other: object) -> Any:  # noqa: D105
        return Column(ne(cast(Expr[object], self._expr), lit_value(other)))

    def __lt__(self, other: object) -> Column[bool]:
        return Column(lt(cast(Expr[object], self._expr), lit_value(other)))

    def __le__(self, other: object) -> Column[bool]:
        return Column(le(cast(Expr[object], self._expr), lit_value(other)))

    def __gt__(self, other: object) -> Column[bool]:
        return Column(gt(cast(Expr[object], self._expr), lit_value(other)))

    def __ge__(self, other: object) -> Column[bool]:
        return Column(ge(cast(Expr[object], self._expr), lit_value(other)))

    def __and__(self, other: object) -> Column[bool]:
        rhs = other if isinstance(other, Column) else Column(cast(Expr[bool], lit_value(other)))
        return Column(and_(cast(Expr[bool], self._expr), cast(Expr[bool], rhs._expr)))

    def __rand__(self, other: object) -> Column[bool]:
        lhs = other if isinstance(other, Column) else Column(cast(Expr[bool], lit_value(other)))
        return Column(and_(cast(Expr[bool], lhs._expr), cast(Expr[bool], self._expr)))

    def __or__(self, other: object) -> Column[bool]:
        rhs = other if isinstance(other, Column) else Column(cast(Expr[bool], lit_value(other)))
        return Column(or_(cast(Expr[bool], self._expr), cast(Expr[bool], rhs._expr)))

    def __ror__(self, other: object) -> Column[bool]:
        lhs = other if isinstance(other, Column) else Column(cast(Expr[bool], lit_value(other)))
        return Column(or_(cast(Expr[bool], lhs._expr), cast(Expr[bool], self._expr)))

    def __invert__(self) -> Column[bool]:
        return Column(not_(cast(Expr[bool], self._expr)))

    def __xor__(self, other: object) -> Column[bool]:
        rhs = other if isinstance(other, Column) else Column(cast(Expr[bool], lit_value(other)))
        return Column(xor(cast(Expr[bool], self._expr), cast(Expr[bool], rhs._expr)))

    def __rxor__(self, other: object) -> Column[bool]:
        lhs = other if isinstance(other, Column) else Column(cast(Expr[bool], lit_value(other)))
        return Column(xor(cast(Expr[bool], lhs._expr), cast(Expr[bool], self._expr)))

    def __bool__(self) -> bool:
        raise TypeError("Column truth value is undefined; chain filters on a SparkFrame instead")

    def between(  # noqa: A003
        self,
        lower: object,
        upper: object,
        *,
        closed: str = "both",
    ) -> Column[bool]:
        return Column(
            between(
                cast(Expr[object], self._expr),
                lit_value(lower),
                lit_value(upper),
                closed=closed,
            )
        )

    def abs(self) -> Column:  # noqa: A003
        return Column(abs_(cast(Expr[object], self._expr)))

    def sqrt(self) -> Column:  # noqa: A003
        return Column(expr_sqrt(cast(Expr[object], self._expr)))

    def exp(self) -> Column:  # noqa: A003
        return Column(exp(cast(Expr[object], self._expr)))

    def log(self) -> Column:  # noqa: A003
        return Column(log(cast(Expr[object], self._expr)))

    def pow(self, other: object) -> Column:
        return Column(pow_(cast(Expr[object], self._expr), lit_value(other)))

    def round(self, scale: int | None = None) -> Column:  # noqa: A003
        return Column(round_(cast(Expr[object], self._expr), ndigits=scale))

    def floor(self) -> Column:  # noqa: A003
        return Column(floor(cast(Expr[object], self._expr)))

    def ceil(self) -> Column:  # noqa: A003
        return Column(ceil(cast(Expr[object], self._expr)))

    def contains(self, other: str) -> Column[bool]:  # noqa: A003
        return Column(contains(cast(Expr[object], self._expr), other))

    def startswith(self, other: str) -> Column[bool]:
        return Column(starts_with(cast(Expr[object], self._expr), other))

    def endswith(self, other: str) -> Column[bool]:
        return Column(ends_with(cast(Expr[object], self._expr), other))

    def substr(self, start: int, length: int) -> Column:
        raise NotImplementedError(
            "Column.substr is not implemented for PlanFrame yet; use core string IR if added"
        )


def unwrap_expr(col_or_expr: Column[Any] | Expr[Any]) -> Expr[Any]:
    if isinstance(col_or_expr, Column):
        return col_or_expr.expr
    return col_or_expr


__all__ = ["Column", "lit_value", "unwrap_expr"]
