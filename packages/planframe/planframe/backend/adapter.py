from __future__ import annotations

from typing import Any, Generic, Protocol, TypeVar

BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


class BackendAdapter(Protocol, Generic[BackendFrameT, BackendExprT]):
    """Backend execution protocol.

    Core PlanFrame code must not import backend libraries. Adapters translate PlanFrame
    operations + expression IR into backend-native operations.
    """

    name: str

    def select(self, df: BackendFrameT, columns: tuple[str, ...]) -> BackendFrameT: ...

    def drop(self, df: BackendFrameT, columns: tuple[str, ...]) -> BackendFrameT: ...

    def rename(self, df: BackendFrameT, mapping: dict[str, str]) -> BackendFrameT: ...

    def with_column(self, df: BackendFrameT, name: str, expr: BackendExprT) -> BackendFrameT: ...

    def cast(self, df: BackendFrameT, name: str, dtype: Any) -> BackendFrameT: ...

    def filter(self, df: BackendFrameT, predicate: BackendExprT) -> BackendFrameT: ...

    def sort(
        self,
        df: BackendFrameT,
        columns: tuple[str, ...],
        *,
        descending: bool = False,
    ) -> BackendFrameT: ...

    def unique(
        self,
        df: BackendFrameT,
        subset: tuple[str, ...] | None,
        *,
        keep: str = "first",
    ) -> BackendFrameT: ...

    def duplicated(
        self,
        df: BackendFrameT,
        subset: tuple[str, ...] | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> BackendFrameT: ...

    def group_by_agg(
        self,
        df: BackendFrameT,
        *,
        keys: tuple[str, ...],
        named_aggs: dict[str, tuple[str, str]],
    ) -> BackendFrameT: ...

    def drop_nulls(
        self,
        df: BackendFrameT,
        subset: tuple[str, ...] | None,
    ) -> BackendFrameT: ...

    def fill_null(
        self,
        df: BackendFrameT,
        value: Any,
        subset: tuple[str, ...] | None,
    ) -> BackendFrameT: ...

    def melt(
        self,
        df: BackendFrameT,
        *,
        id_vars: tuple[str, ...],
        value_vars: tuple[str, ...],
        variable_name: str,
        value_name: str,
    ) -> BackendFrameT: ...

    def join(
        self,
        left: BackendFrameT,
        right: BackendFrameT,
        *,
        on: tuple[str, ...],
        how: str = "inner",
        suffix: str = "_right",
    ) -> BackendFrameT: ...

    def slice(
        self,
        df: BackendFrameT,
        *,
        offset: int,
        length: int | None,
    ) -> BackendFrameT: ...

    def head(self, df: BackendFrameT, n: int) -> BackendFrameT: ...

    def tail(self, df: BackendFrameT, n: int) -> BackendFrameT: ...

    def concat_vertical(self, left: BackendFrameT, right: BackendFrameT) -> BackendFrameT: ...

    def pivot(
        self,
        df: BackendFrameT,
        *,
        index: tuple[str, ...],
        on: str,
        values: str,
        agg: str = "first",
        on_columns: tuple[str, ...] | None = None,
        separator: str = "_",
    ) -> BackendFrameT: ...

    def compile_expr(self, expr: Any) -> BackendExprT: ...

    def collect(self, df: BackendFrameT) -> BackendFrameT: ...
 
