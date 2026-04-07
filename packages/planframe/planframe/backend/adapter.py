from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

BackendFrameT = TypeVar("BackendFrameT")
BackendExprT = TypeVar("BackendExprT")


class BaseAdapter(ABC, Generic[BackendFrameT, BackendExprT]):
    """Backend execution base class.

    Core PlanFrame code must not import backend libraries. Adapters translate PlanFrame
    operations + expression IR into backend-native operations.
    """

    name: str

    @abstractmethod
    def select(self, df: BackendFrameT, columns: tuple[str, ...]) -> BackendFrameT: ...

    @abstractmethod
    def drop(
        self, df: BackendFrameT, columns: tuple[str, ...], *, strict: bool = True
    ) -> BackendFrameT:
        """Remove columns named in *columns* from *df*.

        When *strict* is True (default), implementations must raise if any name is missing.
        When *strict* is False, implementations must ignore names that are not present in *df*.
        """
        ...

    @abstractmethod
    def rename(self, df: BackendFrameT, mapping: dict[str, str]) -> BackendFrameT: ...

    @abstractmethod
    def with_column(self, df: BackendFrameT, name: str, expr: BackendExprT) -> BackendFrameT: ...

    @abstractmethod
    def cast(self, df: BackendFrameT, name: str, dtype: Any) -> BackendFrameT: ...

    @abstractmethod
    def filter(self, df: BackendFrameT, predicate: BackendExprT) -> BackendFrameT: ...

    @abstractmethod
    def sort(
        self,
        df: BackendFrameT,
        columns: tuple[str, ...],
        *,
        descending: tuple[bool, ...],
        nulls_last: tuple[bool, ...],
    ) -> BackendFrameT:
        """Sort *df* by *columns* (first key is most significant).

        *descending* and *nulls_last* must have the same length as *columns*.
        Each position applies to the sort key at that index (per-key direction and null placement).
        """
        ...

    @abstractmethod
    def unique(
        self,
        df: BackendFrameT,
        subset: tuple[str, ...] | None,
        *,
        keep: str = "first",
        maintain_order: bool = False,
    ) -> BackendFrameT: ...

    @abstractmethod
    def duplicated(
        self,
        df: BackendFrameT,
        subset: tuple[str, ...] | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> BackendFrameT: ...

    @abstractmethod
    def group_by_agg(
        self,
        df: BackendFrameT,
        *,
        keys: tuple[str, ...],
        named_aggs: dict[str, tuple[str, str]],
    ) -> BackendFrameT: ...

    @abstractmethod
    def drop_nulls(
        self,
        df: BackendFrameT,
        subset: tuple[str, ...] | None,
    ) -> BackendFrameT: ...

    @abstractmethod
    def fill_null(
        self,
        df: BackendFrameT,
        value: Any,
        subset: tuple[str, ...] | None,
    ) -> BackendFrameT: ...

    @abstractmethod
    def melt(
        self,
        df: BackendFrameT,
        *,
        id_vars: tuple[str, ...],
        value_vars: tuple[str, ...],
        variable_name: str,
        value_name: str,
    ) -> BackendFrameT: ...

    @abstractmethod
    def join(
        self,
        left: BackendFrameT,
        right: BackendFrameT,
        *,
        on: tuple[str, ...],
        how: str = "inner",
        suffix: str = "_right",
    ) -> BackendFrameT: ...

    @abstractmethod
    def slice(
        self,
        df: BackendFrameT,
        *,
        offset: int,
        length: int | None,
    ) -> BackendFrameT: ...

    @abstractmethod
    def head(self, df: BackendFrameT, n: int) -> BackendFrameT: ...

    @abstractmethod
    def tail(self, df: BackendFrameT, n: int) -> BackendFrameT: ...

    @abstractmethod
    def concat_vertical(self, left: BackendFrameT, right: BackendFrameT) -> BackendFrameT: ...

    @abstractmethod
    def concat_horizontal(self, left: BackendFrameT, right: BackendFrameT) -> BackendFrameT: ...

    @abstractmethod
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

    @abstractmethod
    def write_parquet(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: dict[str, Any] | None = None,
    ) -> None: ...

    @abstractmethod
    def write_csv(
        self,
        df: BackendFrameT,
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: dict[str, Any] | None = None,
    ) -> None: ...

    @abstractmethod
    def write_ndjson(
        self, df: BackendFrameT, path: str, *, storage_options: dict[str, Any] | None = None
    ) -> None: ...

    @abstractmethod
    def write_ipc(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "uncompressed",
        storage_options: dict[str, Any] | None = None,
    ) -> None: ...

    @abstractmethod
    def write_database(
        self,
        df: BackendFrameT,
        *,
        table_name: str,
        connection: Any,
        if_table_exists: str = "fail",
        engine: str | None = None,
    ) -> None: ...

    @abstractmethod
    def write_excel(self, df: BackendFrameT, path: str, *, worksheet: str = "Sheet1") -> None: ...

    @abstractmethod
    def write_delta(
        self,
        df: BackendFrameT,
        target: str,
        *,
        mode: str = "error",
        storage_options: dict[str, Any] | None = None,
    ) -> None: ...

    @abstractmethod
    def write_avro(
        self,
        df: BackendFrameT,
        path: str,
        *,
        compression: str = "uncompressed",
        name: str = "",
    ) -> None: ...

    @abstractmethod
    def explode(self, df: BackendFrameT, column: str) -> BackendFrameT: ...

    @abstractmethod
    def unnest(self, df: BackendFrameT, column: str) -> BackendFrameT: ...

    @abstractmethod
    def drop_nulls_all(
        self, df: BackendFrameT, subset: tuple[str, ...] | None
    ) -> BackendFrameT: ...

    @abstractmethod
    def sample(
        self,
        df: BackendFrameT,
        *,
        n: int | None = None,
        frac: float | None = None,
        with_replacement: bool = False,
        shuffle: bool = False,
        seed: int | None = None,
    ) -> BackendFrameT: ...

    @abstractmethod
    def compile_expr(self, expr: Any) -> BackendExprT: ...

    @abstractmethod
    def collect(self, df: BackendFrameT) -> BackendFrameT: ...

    @abstractmethod
    def to_dicts(self, df: BackendFrameT) -> list[dict[str, object]]: ...

    @abstractmethod
    def to_dict(self, df: BackendFrameT) -> dict[str, list[object]]: ...


# Backwards-compatible name for older imports.
BackendAdapter = BaseAdapter
