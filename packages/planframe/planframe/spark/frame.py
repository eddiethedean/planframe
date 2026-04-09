"""PySpark-like `DataFrame` method aliases on top of :class:`planframe.frame.Frame`."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Generic, Literal, TypeVar, cast

from typing_extensions import LiteralString, Self

from planframe.expr.api import Col, Expr
from planframe.frame import Frame
from planframe.plan.join_options import JoinOptions

from .column import Column, unwrap_expr
from .groupby import GroupedData

SchemaT = TypeVar("SchemaT")
BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


def _norm_select_arg(
    c: str | Column[Any] | Expr[Any],
) -> str | tuple[str, Expr[Any]]:
    if isinstance(c, str):
        return c
    if isinstance(c, Column):
        if c.alias_name is not None:
            return (c.alias_name, c.expr)
        if isinstance(c.expr, Col):
            return c.expr.name
        raise TypeError(
            "Computed Column in select must use Column.alias(name); "
            "plain column refs can use col('x') or a str."
        )
    if isinstance(c, Expr):
        if isinstance(c, Col):
            return c.name
        raise TypeError(
            "Bare Expr in select must be a column reference (col(...)) or use core Frame.select tuples"
        )
    raise TypeError(f"Unsupported select() argument: {type(c).__name__}")


def _drop_col_names(*cols: str | Column[Any] | Expr[Any]) -> tuple[str, ...]:
    names: list[str] = []
    for c in cols:
        if isinstance(c, str):
            names.append(c)
        elif isinstance(c, Column):
            if isinstance(c.expr, Col):
                names.append(c.expr.name)
            else:
                raise TypeError("drop() only accepts column name strings or col('name') references")
        elif isinstance(c, Expr) and isinstance(c, Col):
            names.append(c.name)
        else:
            raise TypeError("drop() only accepts column name strings or col('name') references")
    return tuple(names)


def _bool_expr(pred: Column[bool] | Expr[bool]) -> Expr[bool]:
    e = unwrap_expr(pred)
    return cast(Expr[bool], e)


def _sort_keys(
    *cols: str | Column[Any] | Expr[Any],
) -> tuple[str | Expr[Any], ...]:
    out: list[str | Expr[Any]] = []
    for c in cols:
        if isinstance(c, str):
            out.append(c)
        else:
            out.append(unwrap_expr(c))
    return tuple(out)


class SparkFrame(
    Frame[SchemaT, BackendFrameT, BackendExprT], Generic[SchemaT, BackendFrameT, BackendExprT]
):
    """Mixin-style subclass adding PySpark naming; combine with a concrete backend frame.

    Example::

        from planframe.spark import SparkFrame


        class Users(PolarsFrame, SparkFrame):
            id: int
    """

    # --- PySpark naming ---

    def __getitem__(self, key: str) -> Column[Any]:
        # Column reference sugar: df["x"].
        self.schema().get(key)
        return Column(Col(name=key))

    def __getattr__(self, name: str) -> Any:
        # Column reference sugar: df.x, only when the name is a schema column.
        # Fall back to normal AttributeError for real attributes.
        try:
            self.schema().get(name)
        except Exception:  # noqa: BLE001
            raise AttributeError(name) from None
        return Column(Col(name=name))

    def withColumns(
        self, colsMap: Mapping[str, Column[Any] | Expr[Any]]
    ) -> SparkFrame[SchemaT, BackendFrameT, BackendExprT]:  # noqa: N802, N803
        out: Frame[Any, BackendFrameT, BackendExprT] = self
        for name, expr in colsMap.items():
            out = out.with_column(cast(LiteralString, name), unwrap_expr(expr))
        return cast(SparkFrame[SchemaT, BackendFrameT, BackendExprT], out)

    def hint(self, *hints: str, **kv: object) -> SparkFrame[SchemaT, BackendFrameT, BackendExprT]:
        # Implemented via core Hint plan node (added in plan/nodes.py).
        from planframe.plan.nodes import Hint  # avoid cycle at import time

        return cast(
            SparkFrame[SchemaT, BackendFrameT, BackendExprT],
            type(self)(
                _data=self._data,
                _adapter=self._adapter,
                _plan=Hint(self._plan, hints=tuple(hints), kv=dict(kv)),
                _schema=self._schema,
            ),
        )

    def withColumn(
        self, colName: str, col: Column[Any] | Expr[Any]
    ) -> SparkFrame[SchemaT, BackendFrameT, BackendExprT]:  # noqa: N802, N803
        return cast(
            SparkFrame[SchemaT, BackendFrameT, BackendExprT],
            super().with_column(colName, unwrap_expr(col)),
        )

    def withColumnRenamed(  # noqa: N802
        self, existing: str, new: str
    ) -> SparkFrame[SchemaT, BackendFrameT, BackendExprT]:  # noqa: N803
        return cast(
            SparkFrame[SchemaT, BackendFrameT, BackendExprT],
            super().rename(strict=True, **{existing: new}),
        )

    def select(self, *columns: Any) -> SparkFrame[SchemaT, BackendFrameT, BackendExprT]:
        args = tuple(_norm_select_arg(c) for c in columns)
        return cast(SparkFrame[SchemaT, BackendFrameT, BackendExprT], super().select(*args))

    def drop(
        self, *cols: str | Column[Any] | Expr[Any], strict: bool = True
    ) -> SparkFrame[SchemaT, BackendFrameT, BackendExprT]:  # type: ignore[override]
        names = _drop_col_names(*cols)
        return cast(
            SparkFrame[SchemaT, BackendFrameT, BackendExprT], super().drop(*names, strict=strict)
        )

    def where(self, condition: Any) -> SparkFrame[SchemaT, BackendFrameT, BackendExprT]:
        if not isinstance(condition, (Column, Expr)):
            raise TypeError("where() expects a Spark Column or planframe Expr[bool]")
        return cast(
            SparkFrame[SchemaT, BackendFrameT, BackendExprT], super().filter(_bool_expr(condition))
        )

    def filter(self, predicate: Expr[bool]) -> Self:  # noqa: A003
        return cast(Self, self.where(predicate))

    def orderBy(  # noqa: N802
        self,
        *cols: str | Column[Any] | Expr[Any],
        ascending: bool | Sequence[bool] = True,
    ) -> SparkFrame[Any, Any, Any]:
        keys = _sort_keys(*cols)
        if isinstance(ascending, bool):
            return cast(
                SparkFrame[Any, Any, Any],
                super().sort(*keys, descending=not ascending, nulls_last=False),
            )
        asc = tuple(ascending)
        if len(asc) != len(keys):
            raise ValueError("ascending sequence must match number of sort columns")
        descending = tuple(not x for x in asc)
        return cast(
            SparkFrame[Any, Any, Any],
            super().sort(*keys, descending=descending, nulls_last=False),
        )

    def sortWithinPartitions(
        self, *cols: str | Column[Any] | Expr[Any], ascending: bool = True
    ) -> SparkFrame[Any, Any, Any]:  # noqa: N802
        raise NotImplementedError(
            "sortWithinPartitions is Spark partitioning behavior; PlanFrame has no partition concept here."
        )

    def distinct(self) -> SparkFrame[Any, Any, Any]:
        return cast(SparkFrame[Any, Any, Any], super().unique())

    def dropDuplicates(
        self, subset: list[str] | tuple[str, ...] | None = None
    ) -> SparkFrame[Any, Any, Any]:  # noqa: N802
        if subset is None:
            return cast(SparkFrame[Any, Any, Any], super().unique())
        return cast(SparkFrame[Any, Any, Any], super().drop_duplicates(*tuple(subset)))

    def sample(
        self,
        n: int | None = None,
        *,
        frac: float | None = None,
        with_replacement: bool = False,
        shuffle: bool = True,
        seed: int | None = None,
        **kwargs: Any,
    ) -> SparkFrame[SchemaT, BackendFrameT, BackendExprT]:
        # Spark-style aliases: withReplacement=, fraction=.
        if "withReplacement" in kwargs:
            with_replacement = bool(kwargs.pop("withReplacement"))
        if "fraction" in kwargs:
            frac = cast(float, kwargs.pop("fraction"))
        if kwargs:
            raise TypeError(f"Unexpected sample() kwargs: {sorted(kwargs)}")
        if frac is None:
            raise ValueError("sample() requires frac= (or Spark-style fraction=)")
        return cast(
            SparkFrame[SchemaT, BackendFrameT, BackendExprT],
            super().sample(
                n=n, frac=frac, with_replacement=with_replacement, shuffle=shuffle, seed=seed
            ),
        )

    def groupBy(self, *cols: str | Expr[Any] | Column[Any]) -> GroupedData[Any, Any, Any]:  # noqa: N802
        keys = _sort_keys(*cols)
        return GroupedData(super().group_by(*keys))

    def union(self, other: Frame[Any, BackendFrameT, BackendExprT]) -> SparkFrame[Any, Any, Any]:  # noqa: A003
        """PySpark ``union`` preserves duplicates (SQL UNION ALL)."""
        return cast(SparkFrame[Any, Any, Any], super().concat_vertical(other))

    def unionAll(self, other: Frame[Any, BackendFrameT, BackendExprT]) -> SparkFrame[Any, Any, Any]:  # noqa: N802
        return self.union(other)

    def unionByName(  # noqa: N802
        self,
        other: Frame[Any, BackendFrameT, BackendExprT],
        allowMissingColumns: bool = False,  # noqa: N803
    ) -> SparkFrame[Any, Any, Any]:
        if allowMissingColumns:
            raise NotImplementedError(
                "unionByName(allowMissingColumns=True) is not supported in PlanFrame yet."
            )
        names = self.schema().names()
        if set(names) != set(other.schema().names()):
            raise ValueError(
                "unionByName requires identical logical columns; align schemas or use core transforms."
            )
        if other.schema().names() != names:
            other2 = other.select(*cast(Any, names))
            return cast(SparkFrame[Any, Any, Any], super().concat_vertical(other2))
        return cast(SparkFrame[Any, Any, Any], super().concat_vertical(other))

    def intersect(
        self, other: Frame[Any, BackendFrameT, BackendExprT]
    ) -> SparkFrame[Any, Any, Any]:  # noqa: A003
        raise NotImplementedError("intersect is not implemented in PlanFrame core.")

    def subtract(self, other: Frame[Any, BackendFrameT, BackendExprT]) -> SparkFrame[Any, Any, Any]:
        raise NotImplementedError("subtract (EXCEPT) is not implemented in PlanFrame core.")

    def crossJoin(
        self, other: Frame[Any, BackendFrameT, BackendExprT]
    ) -> SparkFrame[Any, Any, Any]:  # noqa: N802
        return cast(
            SparkFrame[Any, Any, Any],
            super().join(other, how="cross"),
        )

    def join(  # type: ignore[override]
        self,
        other: Frame[Any, BackendFrameT, BackendExprT],
        on: str
        | Sequence[str]
        | Column[Any]
        | Expr[Any]
        | Sequence[Column[Any] | Expr[Any] | str]
        | None = None,
        how: Literal[
            "inner",
            "left",
            "right",
            "full",
            "full_outer",
            "left_semi",
            "left_anti",
            "semi",
            "anti",
            "cross",
        ] = "inner",
        *,
        left_on: Sequence[str | Expr[Any] | Column[Any]] | None = None,
        right_on: Sequence[str | Expr[Any] | Column[Any]] | None = None,
        suffix: str = "_right",
        options: JoinOptions | None = None,
    ) -> SparkFrame[Any, Any, Any]:
        how_map = {
            "outer": "full",
            "full_outer": "full",
            "left_semi": "semi",
            "left_anti": "anti",
        }
        how2 = how_map.get(how, how)
        if how2 not in {"inner", "left", "right", "full", "semi", "anti", "cross"}:
            raise ValueError(f"Unsupported join how={how!r}")

        if how2 == "cross":
            return cast(
                SparkFrame[Any, Any, Any],
                super().join(other, how="cross", suffix=suffix, options=options),
            )

        def _to_on_tuple(
            keys: str
            | Sequence[str]
            | Column[Any]
            | Expr[Any]
            | Sequence[Column[Any] | Expr[Any] | str],
        ) -> tuple[str | Expr[Any], ...]:
            if isinstance(keys, str):
                return (keys,)
            if isinstance(keys, (Column, Expr)):
                return (unwrap_expr(keys),)
            out: list[str | Expr[Any]] = []
            for k in keys:
                if isinstance(k, str):
                    out.append(k)
                else:
                    out.append(unwrap_expr(k))
            return tuple(out)

        if left_on is not None or right_on is not None:
            if left_on is None or right_on is None:
                raise ValueError("join(): left_on and right_on must be provided together")
            lk = tuple(unwrap_expr(c) if isinstance(c, Column) else c for c in left_on)
            rk = tuple(unwrap_expr(c) if isinstance(c, Column) else c for c in right_on)
            return cast(
                SparkFrame[Any, Any, Any],
                super().join(
                    other,
                    left_on=cast(Any, lk),
                    right_on=cast(Any, rk),
                    how=how2,
                    suffix=suffix,
                    options=options,
                ),
            )

        if on is None:
            raise ValueError("join() requires on= or both left_on= and right_on=")

        on_t = _to_on_tuple(on)
        return cast(
            SparkFrame[Any, Any, Any],
            super().join(other, on=cast(Any, on_t), how=how2, suffix=suffix, options=options),
        )

    @property
    def columns(self) -> list[str]:  # noqa: A003
        return list(self.schema().names())

    def toDF(self, *names: str) -> SparkFrame[Any, Any, Any]:  # noqa: N802
        cur = self.schema().names()
        if len(names) != len(cur):
            raise ValueError("toDF requires one name per existing column")
        mapping = dict(zip(cur, names, strict=True))
        return cast(SparkFrame[Any, Any, Any], super().rename(strict=True, **mapping))

    def fillna(  # noqa: N802
        self,
        value: object | Mapping[str, object] | None = None,
        subset: Sequence[str] | str | None = None,
    ) -> SparkFrame[Any, Any, Any]:
        if isinstance(value, Mapping):
            return cast(SparkFrame[Any, Any, Any], super().fill_null_many(dict(value)))
        if value is None:
            raise ValueError("fillna value must be provided unless passing a dict mapping")
        if subset is None:
            return cast(SparkFrame[Any, Any, Any], super().fill_null(value))
        sub = (subset,) if isinstance(subset, str) else tuple(subset)
        return cast(SparkFrame[Any, Any, Any], super().fill_null(value, *sub))

    @property
    def na(self) -> _SparkFrameNaLike:
        return _SparkFrameNaLike(self)

    def dropna(  # noqa: N802
        self,
        how: Literal["any", "all"] = "any",
        thresh: int | None = None,
        subset: Sequence[str] | str | None = None,
    ) -> SparkFrame[Any, Any, Any]:
        if subset is None:
            cols: tuple[str, ...] = ()
        elif isinstance(subset, str):
            cols = (subset,)
        else:
            cols = tuple(subset)
        return cast(
            SparkFrame[Any, Any, Any],
            super().drop_nulls(*cols, how=how, threshold=thresh),
        )

    def count(self) -> int:
        return len(self.to_dicts())

    def take(self, num: int) -> list[dict[str, object]]:
        return self.limit(num).to_dicts()

    def limit(self, n: int) -> SparkFrame[SchemaT, BackendFrameT, BackendExprT]:
        return cast(SparkFrame[SchemaT, BackendFrameT, BackendExprT], super().limit(n))

    def repartition(self, *args: Any, **kwargs: Any) -> SparkFrame[Any, Any, Any]:  # noqa: ARG002
        raise NotImplementedError(
            "repartition is not applicable to PlanFrame; use ExecutionOptions on backends that support hints."
        )

    def coalesce(self, _numPartitions: int) -> SparkFrame[Any, Any, Any]:  # noqa: N803
        raise NotImplementedError("coalesce (Spark partitions) is not applicable to PlanFrame.")

    def cache(self) -> SparkFrame[Any, Any, Any]:  # noqa: A003
        raise NotImplementedError("cache() is not implemented for PlanFrame plans.")

    def persist(self, *args: Any, **kwargs: Any) -> SparkFrame[Any, Any, Any]:  # noqa: ARG002
        raise NotImplementedError("persist() is not implemented for PlanFrame plans.")

    def unpersist(self, *args: Any, **kwargs: Any) -> SparkFrame[Any, Any, Any]:  # noqa: ARG002
        raise NotImplementedError("unpersist() is not implemented for PlanFrame plans.")

    def selectExpr(self, *expr: str) -> SparkFrame[Any, Any, Any]:  # noqa: N802
        raise NotImplementedError(
            "selectExpr requires a SQL expression parser; use select() with Column/Expr instead."
        )

    def show(self, n: int = 20, truncate: bool = True, vertical: bool = False) -> None:  # noqa: ARG002
        rows = self.limit(n).to_dicts()
        if vertical:
            for i, r in enumerate(rows):
                print(f"- record {i}")
                for k, v in r.items():
                    print(f"    {k}: {v}")
            return
        print(rows if not truncate else rows)


class _SparkFrameNaLike:
    __slots__ = ("_frame",)

    def __init__(self, frame: SparkFrame[Any, Any, Any]) -> None:
        self._frame = frame

    def fill(
        self,
        value: object | Mapping[str, object] | None = None,
        subset: Sequence[str] | str | None = None,
    ) -> SparkFrame[Any, Any, Any]:  # noqa: A003
        return self._frame.fillna(value, subset=subset)

    def drop(
        self,
        how: Literal["any", "all"] = "any",
        thresh: int | None = None,
        subset: Sequence[str] | str | None = None,
    ) -> SparkFrame[Any, Any, Any]:  # noqa: A003
        return self._frame.dropna(how=how, thresh=thresh, subset=subset)


__all__ = ["SparkFrame"]
