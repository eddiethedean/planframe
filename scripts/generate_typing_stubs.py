from __future__ import annotations

import argparse
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


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
    a("from typing_extensions import LiteralString, Self")
    a("")
    a("from planframe.backend.adapter import BackendAdapter")
    a("from planframe.expr.api import Expr")
    a("from planframe.groupby import GroupedFrame")
    a("from planframe.plan.nodes import PlanNode")
    a("from planframe.plan.join_options import JoinOptions")
    a("from planframe.schema.ir import Schema")
    a("from planframe.typing._schema_types import JoinedSchema")
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
    a("")
    a("    # NOTE: Pyright's behavior around LiteralString vs str can be permissive.")
    a("    # Overloads here are intended to encourage literal call sites and improve IDE help.")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def select(self, {params}) -> Self: ...")
    a("    def select(self, *columns: LiteralString) -> Self: ...")
    a("")
    a("    def select_prefix(self, prefix: str) -> Self: ...")
    a("    def select_suffix(self, suffix: str) -> Self: ...")
    a("    def select_regex(self, pattern: str) -> Self: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def select_exclude(self, {params}) -> Self: ...")
    a("    def select_exclude(self, *columns: LiteralString) -> Self: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def drop(self, {params}, *, strict: bool = True) -> Self: ...")
    a("    def drop(self, *columns: LiteralString, strict: bool = True) -> Self: ...")
    a("")
    a("    def drop_prefix(self, prefix: str) -> Self: ...")
    a("    def drop_suffix(self, suffix: str) -> Self: ...")
    a("    def drop_regex(self, pattern: str) -> Self: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def reorder_columns(self, {params}) -> Self: ...")
    a("    def reorder_columns(self, *columns: LiteralString) -> Self: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def select_first(self, {params}) -> Self: ...")
    a("    def select_first(self, *columns: LiteralString) -> Self: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def select_last(self, {params}) -> Self: ...")
    a("    def select_last(self, *columns: LiteralString) -> Self: ...")
    a("")
    a("    def move(")
    a("        self,")
    a("        column: LiteralString,")
    a("        *,")
    a("        before: LiteralString | None = ...,")
    a("        after: LiteralString | None = ...,")
    a("    ) -> Self: ...")
    a("")
    a("    def rename(self, **mapping: str) -> Self: ...")
    a("")
    a("    def rename_prefix(self, prefix: str, *subset: LiteralString) -> Self: ...")
    a("    def rename_suffix(self, suffix: str, *subset: LiteralString) -> Self: ...")
    a("    def rename_replace(self, old: str, new: str, *subset: LiteralString) -> Self: ...")
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
    a("    def cast(self, name: LiteralString, dtype: Any) -> Self: ...")
    a("    def filter(self, predicate: Expr[bool]) -> Self: ...")
    a("    def sort(")
    a("        self,")
    a("        *columns: LiteralString,")
    a("        descending: bool | Sequence[bool] = ...,")
    a("        nulls_last: bool | Sequence[bool] = ...,")
    a("    ) -> Self: ...")
    a("    def unique(")
    a("        self,")
    a("        *subset: LiteralString,")
    a('        keep: Literal["first", "last"] = ...,')
    a("        maintain_order: bool = ...,")
    a("    ) -> Self: ...")
    a("    def drop_duplicates(")
    a("        self,")
    a("        *subset: LiteralString,")
    a('        keep: Literal["first", "last"] = ...,')
    a("        maintain_order: bool = ...,")
    a("    ) -> Self: ...")
    a("    def duplicated(")
    a("        self,")
    a("        *subset: LiteralString,")
    a('        keep: Literal["first", "last"] | bool = ...,')
    a("        out_name: str = ...,")
    a("    ) -> Self: ...")
    a(
        "    def group_by(self, *keys: LiteralString) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ..."
    )
    a("    def drop_nulls(self, *subset: LiteralString) -> Self: ...")
    a("    def fill_null(self, value: Any, *subset: LiteralString) -> Self: ...")
    a("    def melt(")
    a("        self,")
    a("        *,")
    a("        id_vars: tuple[LiteralString, ...],")
    a("        value_vars: tuple[LiteralString, ...],")
    a("        variable_name: str = ...,")
    a("        value_name: str = ...,")
    a("    ) -> Self: ...")
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
    a("    def explode(self, column: LiteralString) -> Self: ...")
    a(
        "    def unnest(self, column: LiteralString, *, fields: tuple[LiteralString, ...]) -> Self: ..."
    )
    a("    def drop_nulls_all(self, *subset: LiteralString) -> Self: ...")
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
    for n in range(2, 6):
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
    for n in range(1, 6):
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
    a("    def collect(self) -> BackendFrameT: ...")
    a("")
    a("    @overload")
    a(
        '    def collect(self, *, kind: Literal["dataclass", "pydantic"], name: str = ...) -> list[Any]: ...'
    )
    a("    def to_dicts(self) -> list[dict[str, object]]: ...")
    a("    def to_dict(self) -> dict[str, list[object]]: ...")
    a("    def write_parquet(")
    a("        self,")
    a("        path: str,")
    a("        *,")
    a(
        '        compression: Literal["uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4"] = ...,'
    )
    a("        row_group_size: int | None = ...,")
    a("        partition_by: tuple[LiteralString, ...] | None = ...,")
    a("        storage_options: dict[str, Any] | None = ...,")
    a("    ) -> None: ...")
    a("    def write_csv(")
    a("        self,")
    a("        path: str,")
    a("        *,")
    a("        separator: str = ...,")
    a("        include_header: bool = ...,")
    a("        storage_options: dict[str, Any] | None = ...,")
    a("    ) -> None: ...")
    a(
        "    def write_ndjson(self, path: str, *, storage_options: dict[str, Any] | None = ...) -> None: ..."
    )
    a("    def write_ipc(")
    a("        self,")
    a("        path: str,")
    a("        *,")
    a('        compression: Literal["uncompressed", "lz4", "zstd"] = ...,')
    a("        storage_options: dict[str, Any] | None = ...,")
    a("    ) -> None: ...")
    a("    def write_database(")
    a("        self,")
    a("        table_name: str,")
    a("        *,")
    a("        connection: Any,")
    a('        if_table_exists: Literal["fail", "replace", "append"] = ...,')
    a("        engine: str | None = ...,")
    a("    ) -> None: ...")
    a("    def write_excel(self, path: str, *, worksheet: str = ...) -> None: ...")
    a("    def write_delta(")
    a("        self,")
    a("        target: str,")
    a("        *,")
    a('        mode: Literal["error", "append", "overwrite", "ignore", "merge"] = ...,')
    a("        storage_options: dict[str, Any] | None = ...,")
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
    frame_pyi = _render_frame_pyi(max_arity=args.max_arity)
    schema_types_pyi_path = (
        REPO_ROOT / "packages" / "planframe" / "planframe" / "typing" / "_schema_types.pyi"
    )
    schema_types_pyi = _render_schema_types_pyi()

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
