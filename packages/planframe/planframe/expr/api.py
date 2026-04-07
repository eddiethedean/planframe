from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from planframe.backend.errors import PlanFrameExpressionError

T = TypeVar("T")


class Expr(Generic[T]):
    """Typed expression IR base."""


@dataclass(frozen=True, slots=True)
class Col(Expr[T]):
    name: str


@dataclass(frozen=True, slots=True)
class Lit(Expr[T]):
    value: T


@dataclass(frozen=True, slots=True)
class Add(Expr[object]):
    left: Expr[object]
    right: Expr[object]


@dataclass(frozen=True, slots=True)
class Eq(Expr[bool]):
    left: Expr[object]
    right: Expr[object]

@dataclass(frozen=True, slots=True)
class Ne(Expr[bool]):
    left: Expr[object]
    right: Expr[object]


@dataclass(frozen=True, slots=True)
class Lt(Expr[bool]):
    left: Expr[object]
    right: Expr[object]


@dataclass(frozen=True, slots=True)
class Le(Expr[bool]):
    left: Expr[object]
    right: Expr[object]


@dataclass(frozen=True, slots=True)
class Gt(Expr[bool]):
    left: Expr[object]
    right: Expr[object]


@dataclass(frozen=True, slots=True)
class Ge(Expr[bool]):
    left: Expr[object]
    right: Expr[object]


@dataclass(frozen=True, slots=True)
class Sub(Expr[object]):
    left: Expr[object]
    right: Expr[object]


@dataclass(frozen=True, slots=True)
class Mul(Expr[object]):
    left: Expr[object]
    right: Expr[object]


@dataclass(frozen=True, slots=True)
class TrueDiv(Expr[object]):
    left: Expr[object]
    right: Expr[object]


@dataclass(frozen=True, slots=True)
class IsNull(Expr[bool]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class IsNotNull(Expr[bool]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class IsIn(Expr[bool]):
    value: Expr[object]
    options: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class And(Expr[bool]):
    left: Expr[bool]
    right: Expr[bool]


@dataclass(frozen=True, slots=True)
class Or(Expr[bool]):
    left: Expr[bool]
    right: Expr[bool]


@dataclass(frozen=True, slots=True)
class Not(Expr[bool]):
    value: Expr[bool]


@dataclass(frozen=True, slots=True)
class Xor(Expr[bool]):
    left: Expr[bool]
    right: Expr[bool]


@dataclass(frozen=True, slots=True)
class Abs(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class Round(Expr[object]):
    value: Expr[object]
    ndigits: int | None


@dataclass(frozen=True, slots=True)
class Floor(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class Ceil(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class Coalesce(Expr[object]):
    values: tuple[Expr[object], ...]


@dataclass(frozen=True, slots=True)
class IfElse(Expr[object]):
    cond: Expr[bool]
    then_value: Expr[object]
    else_value: Expr[object]


def col(name: str) -> Col[object]:
    return Col(name=name)


def lit(value: T) -> Lit[T]:
    return Lit(value=value)


def add(left: Expr[object], right: Expr[object]) -> Add:
    return Add(left=left, right=right)


def eq(left: Expr[object], right: Expr[object]) -> Eq:
    return Eq(left=left, right=right)

def ne(left: Expr[object], right: Expr[object]) -> Ne:
    return Ne(left=left, right=right)


def lt(left: Expr[object], right: Expr[object]) -> Lt:
    return Lt(left=left, right=right)


def le(left: Expr[object], right: Expr[object]) -> Le:
    return Le(left=left, right=right)


def gt(left: Expr[object], right: Expr[object]) -> Gt:
    return Gt(left=left, right=right)


def ge(left: Expr[object], right: Expr[object]) -> Ge:
    return Ge(left=left, right=right)


def sub(left: Expr[object], right: Expr[object]) -> Sub:
    return Sub(left=left, right=right)


def mul(left: Expr[object], right: Expr[object]) -> Mul:
    return Mul(left=left, right=right)


def truediv(left: Expr[object], right: Expr[object]) -> TrueDiv:
    return TrueDiv(left=left, right=right)


def is_null(value: Expr[object]) -> IsNull:
    return IsNull(value=value)


def is_not_null(value: Expr[object]) -> IsNotNull:
    return IsNotNull(value=value)


def isin(value: Expr[object], *options: object) -> IsIn:
    return IsIn(value=value, options=options)


def and_(left: Expr[bool], right: Expr[bool]) -> And:
    return And(left=left, right=right)


def or_(left: Expr[bool], right: Expr[bool]) -> Or:
    return Or(left=left, right=right)


def not_(value: Expr[bool]) -> Not:
    return Not(value=value)


def xor(left: Expr[bool], right: Expr[bool]) -> Xor:
    return Xor(left=left, right=right)


def abs_(value: Expr[object]) -> Abs:
    return Abs(value=value)


def round_(value: Expr[object], ndigits: int | None = None) -> Round:
    return Round(value=value, ndigits=ndigits)


def floor(value: Expr[object]) -> Floor:
    return Floor(value=value)


def ceil(value: Expr[object]) -> Ceil:
    return Ceil(value=value)


def coalesce(*values: Expr[object]) -> Coalesce:
    if not values:
        raise PlanFrameExpressionError("coalesce requires at least one value")
    return Coalesce(values=tuple(values))


def if_else(cond: Expr[bool], then_value: Expr[object], else_value: Expr[object]) -> IfElse:
    return IfElse(cond=cond, then_value=then_value, else_value=else_value)


def _assert_bool(expr: Expr[object]) -> Expr[bool]:
    if isinstance(expr, (Eq, Ne, Lt, Le, Gt, Ge, IsNull, IsNotNull, IsIn, And, Or, Not, Xor)):
        return expr  # type: ignore[return-value]
    raise PlanFrameExpressionError(f"Expected boolean Expr, got: {type(expr).__name__}")


def infer_dtype(expr: Expr[Any]) -> Any:
    """Best-effort runtime dtype inference for schema evolution.

    This is intentionally conservative in MVP; higher-fidelity typing comes from stubs/overloads.
    """

    if isinstance(expr, Lit):
        return type(expr.value)
    if isinstance(expr, (Eq, Ne, Lt, Le, Gt, Ge, IsNull, IsNotNull, IsIn, And, Or, Not, Xor)):
        return bool
    if isinstance(expr, (Add, Sub, Mul, TrueDiv)):
        return object
    if isinstance(expr, (Abs, Round, Floor, Ceil, Coalesce, IfElse)):
        return object
    if isinstance(expr, Col):
        return Any
    return Any
 
