from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeAlias, TypeGuard, TypeVar, cast

from ..backend.errors import PlanFrameExpressionError

T = TypeVar("T")


class Expr(Generic[T]):
    """Typed expression IR base."""

    def alias(self, name: str) -> Alias[T]:
        if not name:
            raise ValueError("alias name must be non-empty")
        return Alias(expr=self, name=name)

    def __lt__(self, other: object) -> Expr[bool]:
        return lt(cast(Expr[object], self), _coerce_expr(other))

    def __le__(self, other: object) -> Expr[bool]:
        return le(cast(Expr[object], self), _coerce_expr(other))

    def __gt__(self, other: object) -> Expr[bool]:
        return gt(cast(Expr[object], self), _coerce_expr(other))

    def __ge__(self, other: object) -> Expr[bool]:
        return ge(cast(Expr[object], self), _coerce_expr(other))


@dataclass(frozen=True, slots=True)
class Alias(Expr[T]):
    expr: Expr[T]
    name: str


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


@dataclass(frozen=True, slots=True)
class Over(Expr[object]):
    value: Expr[object]
    partition_by: tuple[str, ...]
    order_by: tuple[str, ...] | None


@dataclass(frozen=True, slots=True)
class Between(Expr[bool]):
    value: Expr[object]
    low: Expr[object]
    high: Expr[object]
    closed: str = "both"


@dataclass(frozen=True, slots=True)
class Clip(Expr[object]):
    value: Expr[object]
    lower: Expr[object] | None
    upper: Expr[object] | None


@dataclass(frozen=True, slots=True)
class Pow(Expr[object]):
    base: Expr[object]
    exponent: Expr[object]


@dataclass(frozen=True, slots=True)
class Exp(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class Log(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class StrContains(Expr[bool]):
    value: Expr[object]
    pattern: str
    literal: bool = False


@dataclass(frozen=True, slots=True)
class StrStartsWith(Expr[bool]):
    value: Expr[object]
    prefix: str


@dataclass(frozen=True, slots=True)
class StrEndsWith(Expr[bool]):
    value: Expr[object]
    suffix: str


@dataclass(frozen=True, slots=True)
class StrLower(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class StrUpper(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class StrLen(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class StrReplace(Expr[object]):
    value: Expr[object]
    pattern: str
    replacement: str
    literal: bool = False


@dataclass(frozen=True, slots=True)
class StrStrip(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class StrSplit(Expr[object]):
    value: Expr[object]
    by: str


@dataclass(frozen=True, slots=True)
class DtYear(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class DtMonth(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class DtDay(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class Sqrt(Expr[object]):
    value: Expr[object]


@dataclass(frozen=True, slots=True)
class IsFinite(Expr[bool]):
    value: Expr[object]


AggOpLiteral = Literal["count", "sum", "mean", "min", "max", "n_unique"]


@dataclass(frozen=True, slots=True)
class AggExpr(Expr[object]):
    """Apply an aggregation *op* to *inner* inside :meth:`~planframe.groupby.GroupedFrame.agg`."""

    op: AggOpLiteral
    inner: Expr[object]


def col(name: str) -> Col[object]:
    return Col(name=name)


def lit(value: T) -> Lit[T]:
    return Lit(value=value)


def _coerce_expr(value: object) -> Expr[object]:
    if isinstance(value, Expr):
        return cast(Expr[object], value)
    return cast(Expr[object], lit(value))


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


def over(
    value: Expr[object], *, partition_by: tuple[str, ...], order_by: tuple[str, ...] | None = None
) -> Over:
    if not partition_by:
        raise PlanFrameExpressionError("over requires non-empty partition_by")
    if order_by is not None and not order_by:
        raise PlanFrameExpressionError("over order_by must be non-empty when provided")
    return Over(value=value, partition_by=partition_by, order_by=order_by)


def between(
    value: Expr[object],
    low: Expr[object],
    high: Expr[object],
    *,
    closed: str = "both",
) -> Between:
    if closed not in {"both", "left", "right", "none"}:
        raise PlanFrameExpressionError("between closed must be one of: both, left, right, none")
    return Between(value=value, low=low, high=high, closed=closed)


def clip(
    value: Expr[object], *, lower: Expr[object] | None = None, upper: Expr[object] | None = None
) -> Clip:
    if lower is None and upper is None:
        raise PlanFrameExpressionError("clip requires at least one of lower= or upper=")
    return Clip(value=value, lower=lower, upper=upper)


def pow_(base: Expr[object], exponent: Expr[object]) -> Pow:
    return Pow(base=base, exponent=exponent)


def exp(value: Expr[object]) -> Exp:
    return Exp(value=value)


def log(value: Expr[object]) -> Log:
    return Log(value=value)


def contains(value: Expr[object], pattern: str, *, literal: bool = False) -> StrContains:
    return StrContains(value=value, pattern=pattern, literal=literal)


def starts_with(value: Expr[object], prefix: str) -> StrStartsWith:
    return StrStartsWith(value=value, prefix=prefix)


def ends_with(value: Expr[object], suffix: str) -> StrEndsWith:
    return StrEndsWith(value=value, suffix=suffix)


def lower(value: Expr[object]) -> StrLower:
    return StrLower(value=value)


def upper(value: Expr[object]) -> StrUpper:
    return StrUpper(value=value)


def length(value: Expr[object]) -> StrLen:
    return StrLen(value=value)


def replace(
    value: Expr[object], pattern: str, replacement: str, *, literal: bool = False
) -> StrReplace:
    return StrReplace(value=value, pattern=pattern, replacement=replacement, literal=literal)


def strip(value: Expr[object]) -> StrStrip:
    return StrStrip(value=value)


def split(value: Expr[object], by: str) -> StrSplit:
    return StrSplit(value=value, by=by)


def year(value: Expr[object]) -> DtYear:
    return DtYear(value=value)


def month(value: Expr[object]) -> DtMonth:
    return DtMonth(value=value)


def day(value: Expr[object]) -> DtDay:
    return DtDay(value=value)


def sqrt(value: Expr[object]) -> Sqrt:
    return Sqrt(value=value)


def is_finite(value: Expr[object]) -> IsFinite:
    return IsFinite(value=value)


def agg_count(inner: Expr[object]) -> AggExpr:
    return AggExpr(op="count", inner=inner)


def agg_sum(inner: Expr[object]) -> AggExpr:
    return AggExpr(op="sum", inner=inner)


def agg_mean(inner: Expr[object]) -> AggExpr:
    return AggExpr(op="mean", inner=inner)


def agg_min(inner: Expr[object]) -> AggExpr:
    return AggExpr(op="min", inner=inner)


def agg_max(inner: Expr[object]) -> AggExpr:
    return AggExpr(op="max", inner=inner)


def agg_n_unique(inner: Expr[object]) -> AggExpr:
    return AggExpr(op="n_unique", inner=inner)


BoolExpr: TypeAlias = (
    Eq
    | Ne
    | Lt
    | Le
    | Gt
    | Ge
    | IsNull
    | IsNotNull
    | IsIn
    | And
    | Or
    | Not
    | Xor
    | Between
    | StrContains
    | StrStartsWith
    | StrEndsWith
    | IsFinite
)


def is_bool_expr(expr: Expr[object]) -> TypeGuard[BoolExpr]:
    return isinstance(
        expr,
        (
            Eq,
            Ne,
            Lt,
            Le,
            Gt,
            Ge,
            IsNull,
            IsNotNull,
            IsIn,
            And,
            Or,
            Not,
            Xor,
            Between,
            StrContains,
            StrStartsWith,
            StrEndsWith,
            IsFinite,
        ),
    )


def _assert_bool(expr: Expr[object]) -> Expr[bool]:
    if is_bool_expr(expr):
        return cast(Expr[bool], expr)
    raise PlanFrameExpressionError(f"Expected boolean Expr, got: {type(expr).__name__}")


def infer_dtype(expr: Expr[Any]) -> object:
    """Best-effort runtime dtype inference for schema evolution.

    This is intentionally conservative in MVP; higher-fidelity typing comes from stubs/overloads.
    """

    if isinstance(expr, Lit):
        return type(expr.value)
    if isinstance(
        expr,
        (
            Eq,
            Ne,
            Lt,
            Le,
            Gt,
            Ge,
            IsNull,
            IsNotNull,
            IsIn,
            And,
            Or,
            Not,
            Xor,
            Between,
            StrContains,
            StrStartsWith,
            StrEndsWith,
            IsFinite,
        ),
    ):
        return bool
    if isinstance(expr, (Add, Sub, Mul, TrueDiv)):
        return object
    if isinstance(
        expr,
        (
            Abs,
            Round,
            Floor,
            Ceil,
            Coalesce,
            IfElse,
            Clip,
            Pow,
            Exp,
            Log,
            StrLower,
            StrUpper,
            StrReplace,
            StrStrip,
            StrSplit,
            Over,
            Sqrt,
        ),
    ):
        return object
    if isinstance(expr, (StrLen, DtYear, DtMonth, DtDay)):
        return object
    if isinstance(expr, AggExpr):
        if expr.op in {"count", "n_unique"}:
            return int
        if expr.op == "mean":
            return float
        return object
    if isinstance(expr, Col):
        return Any
    return Any
