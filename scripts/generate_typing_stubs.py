from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _ruff_format_pyi(path: Path, content: str) -> str:
    """Match `ruff format` so ``ruff format --check`` passes on generated stubs."""

    stdin_fn = f"--stdin-filename={path.relative_to(REPO_ROOT)}"
    ruff_exe = shutil.which("ruff")
    cmd = (
        [ruff_exe, "format", "-", stdin_fn]
        if ruff_exe
        else [sys.executable, "-m", "ruff", "format", "-", stdin_fn]
    )
    proc = subprocess.run(
        cmd,
        input=content,
        text=True,
        capture_output=True,
        cwd=REPO_ROOT,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr or "ruff format failed")
    return proc.stdout


def _write_if_changed(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding="utf-8") if path.exists() else None
    if existing == content:
        return
    path.write_text(content, encoding="utf-8")


def _differs(path: Path, content: str) -> bool:
    existing = path.read_text(encoding="utf-8") if path.exists() else None
    return existing != content


def _render_frame_pyi(*, max_arity: int = 10) -> str:
    lines: list[str] = []
    a = lines.append

    a("from __future__ import annotations")
    a("")
    a("from collections.abc import Sequence")
    a("from typing import Any, Generic, Literal, TypeVar, overload")
    a("")
    a("from typing_extensions import LiteralString, Self")
    a("")
    a("from planframe.backend.adapter import BackendAdapter")
    a("from planframe.expr.api import Expr")
    a("from planframe.groupby import GroupedFrame")
    a("from planframe.plan.join_options import JoinOptions")
    a("from planframe.plan.nodes import PlanNode")
    a("from planframe.schema.ir import Schema")
    a("from planframe.typing._schema_types import JoinedSchema")
    a("from planframe.typing.scalars import Scalar")
    a("from planframe.typing.storage import StorageOptions")
    a("")
    a('SchemaT = TypeVar("SchemaT")')
    a('BackendFrameT = TypeVar("BackendFrameT")')
    a('BackendExprT = TypeVar("BackendExprT")')
    a('OtherSchemaT = TypeVar("OtherSchemaT")')
    a('T = TypeVar("T")')
    a("")
    a("")
    a("class Frame(Generic[SchemaT, BackendFrameT, BackendExprT]):")
    a("    _data: BackendFrameT")
    a("    _adapter: BackendAdapter[BackendFrameT, BackendExprT]")
    a("    _plan: PlanNode")
    a("    _schema: Schema")
    a("")
    a("    def __init__(")
    a("        self,")
    a("        _data: BackendFrameT,")
    a("        _adapter: BackendAdapter[BackendFrameT, BackendExprT],")
    a("        _plan: PlanNode,")
    a("        _schema: Schema,")
    a("    ) -> None: ...")
    a("")
    a("    @classmethod")
    a("    def source(")
    a("        cls,")
    a("        data: BackendFrameT,")
    a("        *,")
    a("        adapter: BackendAdapter[BackendFrameT, BackendExprT],")
    a("        schema: type[SchemaT],")
    a("    ) -> Self: ...")
    a("")
    a("    def schema(self) -> Schema: ...")
    a("    def plan(self) -> PlanNode: ...")
    a("    def optimize(self, *, level: Literal[0, 1, 2] = ...) -> Self: ...")
    a("")
    a("    # NOTE: Pyright's behavior around LiteralString vs str can be permissive.")
    a("    # Overloads here are intended to encourage literal call sites and improve IDE help.")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def select(self, {params}) -> Self: ...")
    a("    @overload")
    a("    def select(self, *columns: LiteralString) -> Self: ...")
    a("    @overload")
    a("    def select(self, *columns: LiteralString | tuple[str, Expr[Any]]) -> Self: ...")
    a("    def select(self, *columns: Any) -> Self: ...")
    a("")
    a("    def select_prefix(self, prefix: str) -> Self: ...")
    a("    def select_suffix(self, suffix: str) -> Self: ...")
    a("    def select_regex(self, pattern: str) -> Self: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def select_exclude(self, {params}) -> Self: ...")
    a("    @overload")
    a("    def select_exclude(self, *columns: LiteralString) -> Self: ...")
    a("    @overload")
    a("    def select_exclude(self, *columns: str) -> Self: ...")
    a("    def select_exclude(self, *columns: Any) -> Self: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def drop(self, {params}, *, strict: bool = True) -> Self: ...")
    a("    @overload")
    a("    def drop(self, *columns: LiteralString, strict: bool = True) -> Self: ...")
    a("    @overload")
    a("    def drop(self, *columns: str, strict: bool = True) -> Self: ...")
    a("    def drop(self, *columns: Any, strict: bool = True) -> Self: ...")
    a("")
    a("    def drop_prefix(self, prefix: str) -> Self: ...")
    a("    def drop_suffix(self, suffix: str) -> Self: ...")
    a("    def drop_regex(self, pattern: str) -> Self: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def reorder_columns(self, {params}) -> Self: ...")
    a("    @overload")
    a("    def reorder_columns(self, *columns: LiteralString) -> Self: ...")
    a("    @overload")
    a("    def reorder_columns(self, *columns: str) -> Self: ...")
    a("    def reorder_columns(self, *columns: Any) -> Self: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def select_first(self, {params}) -> Self: ...")
    a("    @overload")
    a("    def select_first(self, *columns: LiteralString) -> Self: ...")
    a("    @overload")
    a("    def select_first(self, *columns: str) -> Self: ...")
    a("    def select_first(self, *columns: Any) -> Self: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def select_last(self, {params}) -> Self: ...")
    a("    @overload")
    a("    def select_last(self, *columns: LiteralString) -> Self: ...")
    a("    @overload")
    a("    def select_last(self, *columns: str) -> Self: ...")
    a("    def select_last(self, *columns: Any) -> Self: ...")
    a("")
    a("    @overload")
    a("    def move(")
    a("        self,")
    a("        column: LiteralString,")
    a("        *,")
    a("        before: LiteralString | None = ...,")
    a("        after: LiteralString | None = ...,")
    a("    ) -> Self: ...")
    a("    @overload")
    a("    def move(")
    a("        self,")
    a("        column: str,")
    a("        *,")
    a("        before: str | None = ...,")
    a("        after: str | None = ...,")
    a("    ) -> Self: ...")
    a("    def move(")
    a("        self,")
    a("        column: Any,")
    a("        *,")
    a("        before: Any | None = ...,")
    a("        after: Any | None = ...,")
    a("    ) -> Self: ...")
    a("")
    a("    def rename(self, *, strict: bool = ..., **mapping: str) -> Self: ...")
    a("")
    a("    @overload")
    a("    def rename_prefix(self, prefix: str, *subset: LiteralString) -> Self: ...")
    a("    @overload")
    a("    def rename_suffix(self, suffix: str, *subset: LiteralString) -> Self: ...")
    a("    @overload")
    a("    def rename_replace(self, old: str, new: str, *subset: LiteralString) -> Self: ...")
    a("    @overload")
    a("    def rename_prefix(self, prefix: str, *subset: str) -> Self: ...")
    a("    @overload")
    a("    def rename_suffix(self, suffix: str, *subset: str) -> Self: ...")
    a("    @overload")
    a("    def rename_replace(self, old: str, new: str, *subset: str) -> Self: ...")
    a("    def rename_prefix(self, prefix: str, *subset: Any) -> Self: ...")
    a("    def rename_suffix(self, suffix: str, *subset: Any) -> Self: ...")
    a("    def rename_replace(self, old: str, new: str, *subset: Any) -> Self: ...")
    a("")
    a("    def with_column(")
    a("        self,")
    a("        name: LiteralString,")
    a("        expr: Expr[T],")
    a("    ) -> Self: ...")
    a("")
    a("    @overload")
    a("    def cast(self, name: LiteralString, dtype: type[T]) -> Self: ...")
    a("    @overload")
    a("    def cast(self, name: LiteralString, dtype: object) -> Self: ...")
    a("    def filter(self, predicate: Expr[bool]) -> Self: ...")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__k{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a("    def sort(")
        a("        self,")
        a(f"        {params},")
        a("        descending: bool | Sequence[bool] = ...,")
        a("        nulls_last: bool | Sequence[bool] = ...,")
        a("    ) -> Self: ...")
    a("    @overload")
    a("    def sort(")
    a("        self,")
    a("        *keys: LiteralString,")
    a("        descending: bool | Sequence[bool] = ...,")
    a("        nulls_last: bool | Sequence[bool] = ...,")
    a("    ) -> Self: ...")
    a("    @overload")
    a("    def sort(")
    a("        self,")
    a("        *keys: LiteralString | Expr[Any],")
    a("        descending: bool | Sequence[bool] = ...,")
    a("        nulls_last: bool | Sequence[bool] = ...,")
    a("    ) -> Self: ...")
    a("    def sort(")
    a("        self,")
    a("        *keys: Any,")
    a("        descending: bool | Sequence[bool] = ...,")
    a("        nulls_last: bool | Sequence[bool] = ...,")
    a("    ) -> Self: ...")
    a("    @overload")
    a("    def unique(")
    a("        self,")
    a("        *subset: LiteralString,")
    a('        keep: Literal["first", "last"] = ...,')
    a("        maintain_order: bool = ...,")
    a("    ) -> Self: ...")
    a("    @overload")
    a("    def unique(")
    a("        self,")
    a("        *subset: str,")
    a('        keep: Literal["first", "last"] = ...,')
    a("        maintain_order: bool = ...,")
    a("    ) -> Self: ...")
    a(
        "    def unique(self, *subset: Any, keep: Any = ..., maintain_order: bool = ...) -> Self: ..."
    )
    a("    @overload")
    a("    def drop_duplicates(")
    a("        self,")
    a("        *subset: LiteralString,")
    a('        keep: Literal["first", "last"] = ...,')
    a("        maintain_order: bool = ...,")
    a("    ) -> Self: ...")
    a("    @overload")
    a("    def drop_duplicates(")
    a("        self,")
    a("        *subset: str,")
    a('        keep: Literal["first", "last"] = ...,')
    a("        maintain_order: bool = ...,")
    a("    ) -> Self: ...")
    a(
        '    def drop_duplicates(self, *subset: Any, keep: Literal["first", "last"] = ..., maintain_order: bool = ...) -> Self: ...'
    )
    a("    @overload")
    a("    def duplicated(")
    a("        self,")
    a("        *subset: LiteralString,")
    a('        keep: Literal["first", "last"] | bool = ...,')
    a("        out_name: str = ...,")
    a("    ) -> Self: ...")
    a("    @overload")
    a("    def duplicated(")
    a("        self,")
    a("        *subset: str,")
    a('        keep: Literal["first", "last"] | bool = ...,')
    a("        out_name: str = ...,")
    a("    ) -> Self: ...")
    a(
        '    def duplicated(self, *subset: Any, keep: Literal["first", "last"] | bool = ..., out_name: str = ...) -> Self: ...'
    )
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__gk{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a("    def group_by(")
        a("        self,")
        a(f"        {params},")
        a("    ) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    @overload")
    a(
        "    def group_by(self, *keys: LiteralString) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ..."
    )
    a("    @overload")
    a(
        "    def group_by(self, *keys: LiteralString | Expr[Any]) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ..."
    )
    a(
        "    def group_by(self, *keys: Any) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ..."
    )
    a("    @overload")
    a(
        '    def drop_nulls(self, *subset: LiteralString, how: Literal["any", "all"] = ..., threshold: int | None = ...) -> Self: ...'
    )
    a(
        "    @overload\n"
        '    def drop_nulls(self, *subset: str, how: Literal["any", "all"] = ..., threshold: int | None = ...) -> Self: ...'
    )
    a(
        "    def drop_nulls(self, *subset: Any, how: Any = ..., threshold: int | None = ...) -> Self: ..."
    )
    a("    @overload")
    a("    def fill_null(self, value: Scalar, *subset: LiteralString) -> Self: ...")
    a("    @overload")
    a("    def fill_null(self, value: Expr[Any], *subset: LiteralString) -> Self: ...")
    a("    @overload")
    a("    def fill_null(")
    a("        self,")
    a("        value: None = ...,")
    a("        *subset: LiteralString,")
    a("        strategy: str,")
    a("    ) -> Self: ...")
    a("    @overload")
    a("    def fill_null(self, value: Scalar, *subset: str) -> Self: ...")
    a("    @overload")
    a("    def fill_null(self, value: Expr[Any], *subset: str) -> Self: ...")
    a("    @overload")
    a("    def fill_null(")
    a("        self,")
    a("        value: None = ...,")
    a("        *subset: str,")
    a("        strategy: str,")
    a("    ) -> Self: ...")
    a("    @overload")
    a("    def melt(")
    a("        self,")
    a("        *,")
    a("        id_vars: tuple[LiteralString, ...],")
    a("        value_vars: tuple[LiteralString, ...],")
    a("        variable_name: str = ...,")
    a("        value_name: str = ...,")
    a("    ) -> Self: ...")
    a("    @overload")
    a("    def melt(")
    a("        self,")
    a("        *,")
    a("        id_vars: tuple[str, ...],")
    a("        value_vars: tuple[str, ...],")
    a("        variable_name: str = ...,")
    a("        value_name: str = ...,")
    a("    ) -> Self: ...")
    a(
        "    def melt(self, *, id_vars: tuple[Any, ...], value_vars: tuple[Any, ...], variable_name: str = ..., value_name: str = ...) -> Self: ..."
    )
    a("")
    a("    def slice(self, offset: int, length: int | None = ...) -> Self: ...")
    a("    def limit(self, n: int) -> Self: ...")
    a("    def head(self, n: int) -> Self: ...")
    a("    def tail(self, n: int) -> Self: ...")
    a(
        "    def concat_vertical(self, other: Frame[SchemaT, BackendFrameT, BackendExprT]) -> Self: ..."
    )
    a(
        "    def concat_horizontal(self, other: Frame[SchemaT, BackendFrameT, BackendExprT]) -> Self: ..."
    )
    a(
        "    def union_distinct(self, other: Frame[SchemaT, BackendFrameT, BackendExprT]) -> Self: ..."
    )
    a("    @overload")
    a("    def explode(self, column: LiteralString) -> Self: ...")
    a("    @overload")
    a("    def explode(self, column: str) -> Self: ...")
    a("    def explode(self, column: Any) -> Self: ...")
    a(
        "    @overload\n"
        "    def unnest(self, column: LiteralString, *, fields: tuple[LiteralString, ...]) -> Self: ..."
    )
    a("    @overload")
    a("    def unnest(self, column: str, *, fields: tuple[str, ...]) -> Self: ...")
    a("    def unnest(self, column: Any, *, fields: tuple[Any, ...]) -> Self: ...")
    a("    @overload")
    a("    def drop_nulls_all(self, *subset: LiteralString) -> Self: ...")
    a("    @overload")
    a("    def drop_nulls_all(self, *subset: str) -> Self: ...")
    a("    def drop_nulls_all(self, *subset: Any) -> Self: ...")
    a("    @overload")
    a("    def pivot(")
    a("        self,")
    a("        *,")
    a("        index: tuple[LiteralString, ...],")
    a("        on: LiteralString,")
    a("        values: LiteralString,")
    a(
        '        agg: Literal["first", "last", "sum", "mean", "min", "max", "count", "len", "median"] = ...,'
    )
    a("        on_columns: tuple[str, ...] | None = ...,")
    a("        separator: str = ...,")
    a("    ) -> Self: ...")
    a("    @overload")
    a("    def pivot(")
    a("        self,")
    a("        *,")
    a("        index: tuple[str, ...],")
    a("        on: str,")
    a("        values: str,")
    a(
        '        agg: Literal["first", "last", "sum", "mean", "min", "max", "count", "len", "median"] = ...,'
    )
    a("        on_columns: tuple[str, ...] | None = ...,")
    a("        separator: str = ...,")
    a("    ) -> Self: ...")
    a(
        "    def pivot(self, *, index: tuple[Any, ...], on: Any, values: Any, agg: Any = ..., on_columns: tuple[str, ...] | None = ..., separator: str = ...) -> Self: ..."
    )
    a("    def sample(")
    a("        self,")
    a("        n: int | None = ...,")
    a("        *,")
    a("        frac: float | None = ...,")
    a("        with_replacement: bool = ...,")
    a("        shuffle: bool = ...,")
    a("        seed: int | None = ...,")
    a("    ) -> Self: ...")
    a("")
    a("    @overload")
    a("    def join(")
    a("        self,")
    a("        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],")
    a("        *,")
    a('        how: Literal["cross"],')
    a("        suffix: str = ...,")
    a("        options: JoinOptions | None = ...,")
    a("    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...")
    join_how = 'Literal["inner", "left", "right", "full", "semi", "anti"]'
    a("    @overload")
    a("    def join(")
    a("        self,")
    a("        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],")
    a("        *,")
    a("        on: tuple[LiteralString],")
    a(f"        how: {join_how} = ...,")
    a("        suffix: str = ...,")
    a("        options: JoinOptions | None = ...,")
    a("    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...")
    for n in range(2, max_arity + 1):
        on_tuple = ", ".join(["LiteralString"] * n)
        a("    @overload")
        a("    def join(")
        a("        self,")
        a("        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],")
        a("        *,")
        a(f"        on: tuple[{on_tuple}],")
        a(f"        how: {join_how} = ...,")
        a("        suffix: str = ...,")
        a("        options: JoinOptions | None = ...,")
        a("    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...")
    for n in range(1, max_arity + 1):
        lr = ", ".join(["LiteralString"] * n)
        a("    @overload")
        a("    def join(")
        a("        self,")
        a("        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],")
        a("        *,")
        a(f"        left_on: tuple[{lr}],")
        a(f"        right_on: tuple[{lr}],")
        a(f"        how: {join_how} = ...,")
        a("        suffix: str = ...,")
        a("        options: JoinOptions | None = ...,")
        a("    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...")
    a("    @overload")
    a("    def join(")
    a("        self,")
    a("        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],")
    a("        *,")
    a("        on: tuple[LiteralString | Expr[Any], ...],")
    a(f"        how: {join_how} = ...,")
    a("        suffix: str = ...,")
    a("        options: JoinOptions | None = ...,")
    a("    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...")
    a("    @overload")
    a("    def join(")
    a("        self,")
    a("        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],")
    a("        *,")
    a("        left_on: tuple[LiteralString | Expr[Any], ...],")
    a("        right_on: tuple[LiteralString | Expr[Any], ...],")
    a(f"        how: {join_how} = ...,")
    a("        suffix: str = ...,")
    a("        options: JoinOptions | None = ...,")
    a("    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...")
    a("    def join(")
    a("        self,")
    a("        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],")
    a("        *,")
    a("        on: tuple[Any, ...] | None = ...,")
    a("        left_on: tuple[Any, ...] | None = ...,")
    a("        right_on: tuple[Any, ...] | None = ...,")
    a("        how: Any = ...,")
    a("        suffix: str = ...,")
    a("        options: JoinOptions | None = ...,")
    a("    ) -> Frame[Any, BackendFrameT, BackendExprT]: ...")
    a("    @overload")
    a("    def collect(self) -> BackendFrameT: ...")
    a("")
    a("    @overload")
    a(
        '    def collect(self, *, kind: Literal["dataclass", "pydantic"], name: str = ...) -> list[Any]: ...'
    )
    a("    @overload")
    a("    async def acollect(self) -> BackendFrameT: ...")
    a("")
    a("    @overload")
    a(
        '    async def acollect(self, *, kind: Literal["dataclass", "pydantic"], name: str = ...) -> list[Any]: ...'
    )
    a("    def to_dicts(self) -> list[dict[str, object]]: ...")
    a("    def to_dict(self) -> dict[str, list[object]]: ...")
    a("    async def ato_dicts(self) -> list[dict[str, object]]: ...")
    a("    async def ato_dict(self) -> dict[str, list[object]]: ...")
    a("    def write_parquet(")
    a("        self,")
    a("        path: str,")
    a("        *,")
    a(
        '        compression: Literal["uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4"] = ...,'
    )
    a("        row_group_size: int | None = ...,")
    a("        partition_by: tuple[LiteralString, ...] | None = ...,")
    a("        storage_options: StorageOptions | None = ...,")
    a("    ) -> None: ...")
    a("    def write_csv(")
    a("        self,")
    a("        path: str,")
    a("        *,")
    a("        separator: str = ...,")
    a("        include_header: bool = ...,")
    a("        storage_options: StorageOptions | None = ...,")
    a("    ) -> None: ...")
    a(
        "    def write_ndjson(self, path: str, *, storage_options: StorageOptions | None = ...) -> None: ..."
    )
    a("    def write_ipc(")
    a("        self,")
    a("        path: str,")
    a("        *,")
    a('        compression: Literal["uncompressed", "lz4", "zstd"] = ...,')
    a("        storage_options: StorageOptions | None = ...,")
    a("    ) -> None: ...")
    a("    def write_database(")
    a("        self,")
    a("        table_name: str,")
    a("        *,")
    a("        connection: object,")
    a('        if_table_exists: Literal["fail", "replace", "append"] = ...,')
    a("        engine: str | None = ...,")
    a("    ) -> None: ...")
    a("    def write_excel(self, path: str, *, worksheet: str = ...) -> None: ...")
    a("    def write_delta(")
    a("        self,")
    a("        target: str,")
    a("        *,")
    a('        mode: Literal["error", "append", "overwrite", "ignore", "merge"] = ...,')
    a("        storage_options: StorageOptions | None = ...,")
    a("    ) -> None: ...")
    a("    def write_avro(")
    a("        self,")
    a("        path: str,")
    a("        *,")
    a('        compression: Literal["uncompressed", "snappy", "deflate"] = ...,')
    a("        name: str = ...,")
    a("    ) -> None: ...")
    a("")
    a("    def materialize_model(")
    a("        self,")
    a("        name: str,")
    a("        *,")
    a('        kind: Literal["dataclass", "pydantic"] = ...,')
    a("    ) -> type[Any]: ...")
    a("")

    return "\n".join(lines)


def _render_schema_types_pyi() -> str:
    return "\n".join(
        [
            "from __future__ import annotations",
            "",
            "from typing import Generic, TypeVar",
            "",
            'LeftSchemaT = TypeVar("LeftSchemaT")',
            'RightSchemaT = TypeVar("RightSchemaT")',
            "",
            "class JoinedSchema(Generic[LeftSchemaT, RightSchemaT]):",
            "    ...",
            "",
        ]
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--max-arity", type=int, default=10)
    args = parser.parse_args(argv)

    frame_pyi_path = REPO_ROOT / "packages" / "planframe" / "planframe" / "frame.pyi"
    frame_pyi = _ruff_format_pyi(frame_pyi_path, _render_frame_pyi(max_arity=args.max_arity))
    schema_types_pyi_path = (
        REPO_ROOT / "packages" / "planframe" / "planframe" / "typing" / "_schema_types.pyi"
    )
    schema_types_pyi = _ruff_format_pyi(schema_types_pyi_path, _render_schema_types_pyi())

    if args.check:
        changed: list[str] = []
        if _differs(frame_pyi_path, frame_pyi):
            changed.append(str(frame_pyi_path.relative_to(REPO_ROOT)))
        if _differs(schema_types_pyi_path, schema_types_pyi):
            changed.append(str(schema_types_pyi_path.relative_to(REPO_ROOT)))
        if changed:
            print("Typing stubs are out of date. Re-run:")
            print("  python scripts/generate_typing_stubs.py")
            print("Changed:")
            for p in changed:
                print(f"  - {p}")
            return 1
        return 0

    _write_if_changed(frame_pyi_path, frame_pyi)
    _write_if_changed(schema_types_pyi_path, schema_types_pyi)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
