"""Pandas-like `DataFrame` method aliases on top of :class:`planframe.frame.Frame`.

This is an API skin: it does not implement pandas' eager semantics, indexes, or mutation.
All operations build PlanFrame plans and execute through an adapter at `collect()`.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, Generic, Literal, TypeVar, cast, overload

from typing_extensions import LiteralString, Self

from planframe.expr.api import Expr, col, eq, ge, gt, le, lit, lt, ne
from planframe.frame import Frame
from planframe.groupby import GroupedFrame
from planframe.plan.join_options import JoinOptions

from .series import Series, series_from_key

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


def _names(x: str | Sequence[str] | None) -> tuple[str, ...] | None:
    if x is None:
        return None
    if isinstance(x, str):
        return (x,)
    return tuple(x)


def _expr(x: object) -> Expr[Any]:
    if isinstance(x, Series):
        return x.expr
    if isinstance(x, Expr):
        return x
    return lit(x)


_QUERY_SIMPLE_RE = re.compile(
    r"""^\s*
    (?P<col>[A-Za-z_][A-Za-z0-9_]*)
    \s*(?P<op>==|!=|<=|>=|<|>)\s*
    (?P<lit>
        True|False|None
        |-?\d+(?:\.\d+)?
        |'(?:[^'\\]|\\.)*'
        |\"(?:[^\"\\]|\\.)*\"
    )
    \s*$""",
    re.VERBOSE,
)


def _parse_query(expr: str) -> Expr[bool]:
    m = _QUERY_SIMPLE_RE.match(expr)
    if m is None:
        raise ValueError(
            "query(str) only supports simple expressions of the form "
            "`col op literal` where op is one of == != < <= > >= and literal is "
            "a number, string, bool, or None"
        )
    name = m.group("col")
    op = m.group("op")
    lit_s = m.group("lit")

    if lit_s == "True":
        rhs: object = True
    elif lit_s == "False":
        rhs = False
    elif lit_s == "None":
        rhs = None
    elif lit_s[:1] in {"'", '"'}:
        # Strip quotes; keep minimal escape support for common cases.
        body = lit_s[1:-1]
        rhs = body.encode("utf-8").decode("unicode_escape")
    elif "." in lit_s:
        rhs = float(lit_s)
    else:
        rhs = int(lit_s)

    left = col(name)
    right = lit(rhs)
    if op == "==":
        return eq(left, right)
    if op == "!=":
        return ne(left, right)
    if op == "<":
        return lt(left, right)
    if op == "<=":
        return le(left, right)
    if op == ">":
        return gt(left, right)
    return ge(left, right)


class PandasLikeFrame(
    Frame[SchemaT, BackendFrameT, BackendExprT], Generic[SchemaT, BackendFrameT, BackendExprT]
):
    """Mixin-style pandas-flavored API; combine with a concrete backend frame."""

    # ---- selection ----
    @overload
    def __getitem__(self, key: str) -> Series[Any]: ...

    @overload
    def __getitem__(self, key: Sequence[str]) -> PandasLikeFrame[Any, Any, Any]: ...

    @overload
    def __getitem__(self, key: Series[bool] | Expr[bool]) -> PandasLikeFrame[Any, Any, Any]: ...

    def __getitem__(
        self, key: str | Sequence[str] | Series[bool] | Expr[bool]
    ) -> Series[Any] | PandasLikeFrame[Any, Any, Any]:
        # Note: returns expression wrappers / frames, not data.
        if isinstance(key, str):
            self.schema().get(key)
            return series_from_key(key)
        if isinstance(key, (Series, Expr)):
            # Typed boolean indexing sugar: df[mask] -> df.query(mask)
            return self.query(cast(Any, key))
        cols = tuple(key)
        if not cols:
            raise ValueError("Column selection requires a non-empty list of column names")
        return cast(PandasLikeFrame[Any, Any, Any], super().select(*cols))

    @property
    def columns(self) -> list[str]:  # pandas-like
        return list(self.schema().names())

    # ---- core verbs ----
    def assign(self, **columns: object) -> PandasLikeFrame[Any, Any, Any]:
        """Pandas-like `assign`, lowered to repeated `with_column`."""

        out: Frame[Any, BackendFrameT, BackendExprT] = self
        for name, value in columns.items():
            # Bypass any backend-bound overrides (e.g. PandasFrame blocking core verbs).
            out = Frame.with_columns(out, exprs={cast(LiteralString, name): _expr(value)})
        return cast(PandasLikeFrame[Any, Any, Any], out)

    def sort_values(
        self,
        by: str | Sequence[str],
        *,
        ascending: bool | Sequence[bool] = True,
        na_position: Literal["first", "last"] = "last",
    ) -> PandasLikeFrame[Any, Any, Any]:
        keys = tuple(by) if not isinstance(by, str) else (by,)
        nulls_last = na_position == "last"
        if not keys:
            raise ValueError("sort_values requires non-empty by=")
        if isinstance(ascending, Sequence) and not isinstance(ascending, (str, bytes)):
            asc = tuple(ascending)
            if len(asc) != len(keys):
                raise ValueError("ascending must match the number of sort keys")
            descending: bool | tuple[bool, ...] = tuple(not x for x in asc)
        else:
            descending = not cast(bool, ascending)
        return cast(
            PandasLikeFrame[Any, Any, Any],
            super().sort(*keys, descending=descending, nulls_last=nulls_last),
        )

    def groupby(
        self,
        by: str | Sequence[str],
        *,
        sort: bool = False,
        dropna: bool = True,
    ) -> object:
        """Pandas-like `groupby`, lowered to core `group_by`.\n\n        Note: PlanFrame does not implement pandas index semantics; this returns a PlanFrame\n        grouped object with `.agg(...)`.\n"""

        _ = sort, dropna
        keys = (by,) if isinstance(by, str) else tuple(by)
        if not keys:
            raise ValueError("groupby requires non-empty by=")
        grouped: GroupedFrame[Any, BackendFrameT, BackendExprT] = super().group_by(*keys)
        return _PandasGroupBy(owner=self, grouped=grouped)

    @overload
    def drop(self, *columns: str, strict: bool = True) -> PandasLikeFrame[Any, Any, Any]: ...

    @overload
    def drop(
        self,
        labels: Sequence[str] | None = None,
        *,
        columns: Sequence[str] | str | None = None,
        axis: Literal[0, 1, "index", "columns"] = "columns",
        errors: Literal["ignore", "raise"] = "raise",
    ) -> PandasLikeFrame[Any, Any, Any]: ...

    def drop(  # type: ignore[override]
        self,
        *args: object,
        strict: bool = True,
        columns: Sequence[str] | str | None = None,
        axis: Literal[0, 1, "index", "columns"] = "columns",
        errors: Literal["ignore", "raise"] = "raise",
    ) -> PandasLikeFrame[Any, Any, Any]:
        # PlanFrame-style drop(*cols) remains supported for compatibility.
        if args and all(isinstance(a, str) for a in args) and columns is None:
            return cast(
                PandasLikeFrame[Any, Any, Any],
                super().drop(*cast(tuple[str, ...], args), strict=strict),
            )
        if not args and columns is None:
            return cast(PandasLikeFrame[Any, Any, Any], self)

        if axis in (0, "index"):
            raise NotImplementedError(
                "row-wise drop by index is not supported (no index semantics)."
            )
        labels = args[0] if args else None
        cols = columns if columns is not None else labels
        if cols is None:
            raise ValueError("drop requires columns= (or labels=) for axis='columns'")
        col_tuple = (cols,) if isinstance(cols, str) else tuple(cast(Sequence[str], cols))
        strict2 = errors != "ignore"
        return cast(PandasLikeFrame[Any, Any, Any], super().drop(*col_tuple, strict=strict2))

    def rename(  # type: ignore[override]
        self,
        mapping: Mapping[str, str] | None = None,
        *,
        strict: bool = True,
        **named: str,
    ) -> Self:
        return cast(Self, super().rename(mapping=mapping, strict=strict, **named))

    def rename_pandas(
        self,
        *,
        columns: Mapping[str, str] | None = None,
        errors: Literal["ignore", "raise"] = "raise",
    ) -> Self:
        """Pandas-style rename(columns=..., errors=...)."""

        if not columns:
            return self
        strict2 = errors != "ignore"
        return cast(Self, super().rename(mapping=dict(columns), strict=strict2))

    def dropna(
        self,
        *,
        axis: Literal[0, 1, "index", "columns"] = 0,
        how: Literal["any", "all"] = "any",
        thresh: int | None = None,
        subset: Sequence[str] | str | None = None,
    ) -> PandasLikeFrame[Any, Any, Any]:
        if axis in (1, "columns"):
            raise NotImplementedError("dropna(axis='columns') is not supported in PlanFrame.")
        sub = _names(subset) or ()
        return cast(
            PandasLikeFrame[Any, Any, Any],
            super().drop_nulls(subset=sub or None, how=how, threshold=thresh),
        )

    def melt(
        self,
        *,
        id_vars: Sequence[str] | str,
        value_vars: Sequence[str] | str,
        var_name: str = "variable",
        value_name: str = "value",
    ) -> PandasLikeFrame[Any, Any, Any]:
        """Pandas-like `melt`, lowered to core `unpivot`."""

        ids = (id_vars,) if isinstance(id_vars, str) else tuple(id_vars)
        vals = (value_vars,) if isinstance(value_vars, str) else tuple(value_vars)
        if not ids:
            raise ValueError("melt requires non-empty id_vars=")
        if not vals:
            raise ValueError("melt requires non-empty value_vars=")
        return cast(
            PandasLikeFrame[Any, Any, Any],
            super().unpivot(
                index=ids,
                on=vals,
                variable_name=var_name,
                value_name=value_name,
            ),
        )

    @overload
    def query(self, expr: str) -> PandasLikeFrame[Any, Any, Any]: ...

    @overload
    def query(self, expr: Series[bool] | Expr[bool]) -> PandasLikeFrame[Any, Any, Any]: ...

    def query(self, expr: str | Series[bool] | Expr[bool]) -> PandasLikeFrame[Any, Any, Any]:
        """Pandas-like `query`.

        Supported forms:

        - Typed: `Series[bool]` / `Expr[bool]`
        - String (tiny subset): `col op literal` (no boolean ops, no functions, no parens)
        """

        if isinstance(expr, str):
            pred = _parse_query(expr)
        else:
            pred = expr.expr if isinstance(expr, Series) else expr
        return cast(PandasLikeFrame[Any, Any, Any], super().filter(pred))

    def filter(  # noqa: A003
        self,
        *predicates: Series[bool] | Expr[bool],
        items: Sequence[str] | None = None,
        like: str | None = None,
        regex: str | None = None,
    ) -> PandasLikeFrame[Any, Any, Any]:
        """Row filter (PlanFrame) or column filter (pandas-style), depending on arguments.

        - `df.filter(predicate)` behaves like PlanFrame `Frame.filter`.
        - `df.filter(items=...|like=...|regex=...)` behaves like pandas column selection.
        """

        if predicates:
            if any(x is not None for x in (items, like, regex)):
                raise ValueError(
                    "filter row predicates cannot be combined with items=, like=, or regex="
                )
            pred_exprs = tuple(p.expr if isinstance(p, Series) else p for p in predicates)
            return cast(PandasLikeFrame[Any, Any, Any], super().filter(*pred_exprs))

        provided = sum(x is not None for x in (items, like, regex))
        if provided != 1:
            raise ValueError(
                "filter requires row predicates or exactly one of: items=, like=, regex="
            )
        if items is not None:
            cols = tuple(items)
            if not cols:
                raise ValueError("filter(items=...) must be non-empty")
            return cast(PandasLikeFrame[Any, Any, Any], super().select(*cols))
        if like is not None:
            cols2 = tuple(n for n in self.schema().names() if like in n)
            return cast(PandasLikeFrame[Any, Any, Any], super().select(*cols2))
        rx = re.compile(cast(str, regex))
        cols3 = tuple(n for n in self.schema().names() if rx.search(n))
        return cast(PandasLikeFrame[Any, Any, Any], super().select(*cols3))

    def fillna(
        self,
        value: object | Mapping[str, object] | None = None,
        *,
        subset: Sequence[str] | str | None = None,
    ) -> PandasLikeFrame[Any, Any, Any]:
        if isinstance(value, Mapping):
            return cast(PandasLikeFrame[Any, Any, Any], super().fill_null_many(dict(value)))
        if value is None:
            raise ValueError("fillna requires value= (or a dict mapping)")
        sub = _names(subset)
        if sub is None:
            return cast(PandasLikeFrame[Any, Any, Any], super().fill_null(value))
        return cast(PandasLikeFrame[Any, Any, Any], super().fill_null(value, *sub))

    def astype(
        self,
        dtype: Mapping[str, object],
        *,
        errors: Literal["raise", "ignore"] = "raise",
    ) -> PandasLikeFrame[Any, Any, Any]:
        strict = errors != "ignore"
        return cast(PandasLikeFrame[Any, Any, Any], super().cast_many(dtype, strict=strict))

    def eval(self, **columns: object) -> PandasLikeFrame[Any, Any, Any]:  # noqa: A003
        # Typed eval: alias to assign; no string expressions.
        return self.assign(**columns)

    def head(self, n: int = 5) -> PandasLikeFrame[Any, Any, Any]:
        return cast(PandasLikeFrame[Any, Any, Any], super().head(n))

    def tail(self, n: int = 5) -> PandasLikeFrame[Any, Any, Any]:
        return cast(PandasLikeFrame[Any, Any, Any], super().tail(n))

    def nlargest(
        self,
        n: int,
        columns: str | Sequence[str],
        *,
        keep: Literal["first", "last", "all"] = "first",
    ) -> PandasLikeFrame[Any, Any, Any]:
        if keep == "all":
            raise NotImplementedError("nlargest(keep='all') is not supported in PlanFrame.")
        by = (columns,) if isinstance(columns, str) else tuple(columns)
        if not by:
            raise ValueError("nlargest requires non-empty columns=")
        # pandas: descending sort then head
        return self.sort_values(by, ascending=False).head(n)

    def nsmallest(
        self,
        n: int,
        columns: str | Sequence[str],
        *,
        keep: Literal["first", "last", "all"] = "first",
    ) -> PandasLikeFrame[Any, Any, Any]:
        if keep == "all":
            raise NotImplementedError("nsmallest(keep='all') is not supported in PlanFrame.")
        by = (columns,) if isinstance(columns, str) else tuple(columns)
        if not by:
            raise ValueError("nsmallest requires non-empty columns=")
        # pandas: ascending sort then head
        return self.sort_values(by, ascending=True).head(n)

    def drop_duplicates(
        self,
        *subset: Any,
        keep: Literal["first", "last"] = "first",
        maintain_order: bool = True,
        **kwargs: Any,
    ) -> PandasLikeFrame[Any, Any, Any]:
        # Keep compatibility with core `Frame.drop_duplicates(*subset, keep=..., maintain_order=...)`
        # while also accepting pandas-style `subset=[...]` keyword.
        if "subset" in kwargs:
            if subset:
                raise TypeError(
                    "drop_duplicates(): pass either positional subset or subset=, not both"
                )
            subset = tuple(cast(Sequence[str], kwargs.pop("subset") or ()))
        if kwargs:
            raise TypeError(f"Unexpected drop_duplicates() kwargs: {sorted(kwargs)}")
        if not subset:
            return cast(PandasLikeFrame[Any, Any, Any], super().unique(keep=keep))
        return cast(
            PandasLikeFrame[Any, Any, Any],
            super().drop_duplicates(*subset, keep=keep, maintain_order=maintain_order),
        )

    def merge(
        self,
        right: Frame[Any, BackendFrameT, BackendExprT],
        *,
        how: Literal["inner", "left", "right", "outer", "cross"] = "inner",
        on: str | Sequence[str] | None = None,
        left_on: str | Sequence[str] | None = None,
        right_on: str | Sequence[str] | None = None,
        suffixes: tuple[str, str] = ("_x", "_y"),
        options: JoinOptions | None = None,
    ) -> PandasLikeFrame[Any, Any, Any]:
        how2: Literal["inner", "left", "right", "full", "cross"] = (
            "full" if how == "outer" else cast(Any, how)
        )

        if how2 == "cross":
            return cast(
                PandasLikeFrame[Any, Any, Any],
                super().join(right, how="cross", suffix=suffixes[1], options=options),
            )

        if on is not None:
            keys = _names(on)
            assert keys is not None
            return cast(
                PandasLikeFrame[Any, Any, Any],
                super().join(
                    right,
                    on=cast(Any, tuple(keys)),
                    how=cast(Any, how2),
                    suffix=suffixes[1],
                    options=options,
                ),
            )
        if left_on is None or right_on is None:
            raise ValueError("merge requires on= or both left_on= and right_on=")
        lk = _names(left_on)
        rk = _names(right_on)
        assert lk is not None and rk is not None
        return cast(
            PandasLikeFrame[Any, Any, Any],
            super().join(
                right,
                left_on=cast(Any, tuple(lk)),
                right_on=cast(Any, tuple(rk)),
                how=cast(Any, how2),
                suffix=suffixes[1],
                options=options,
            ),
        )


__all__ = ["PandasLikeFrame", "Series"]


class _PandasGroupBy(Generic[SchemaT, BackendFrameT, BackendExprT]):
    __slots__ = ("_owner", "_grouped")

    def __init__(
        self,
        *,
        owner: PandasLikeFrame[SchemaT, BackendFrameT, BackendExprT],
        grouped: GroupedFrame[Any, BackendFrameT, BackendExprT],
    ) -> None:
        self._owner = owner
        self._grouped = grouped

    def agg(self, **named_aggs: object) -> PandasLikeFrame[Any, Any, Any]:
        out = self._grouped.agg(**cast(Any, named_aggs))
        # Re-wrap the core `Frame` result into the same runtime type as the owner so
        # pandas-style chaining (e.g. `.sort_values`) continues to work.
        return cast(
            PandasLikeFrame[Any, Any, Any],
            type(self._owner)(
                _data=out._data,  # type: ignore[attr-defined]
                _adapter=out._adapter,  # type: ignore[attr-defined]
                _plan=out._plan,  # type: ignore[attr-defined]
                _schema=out._schema,  # type: ignore[attr-defined]
            ),
        )
