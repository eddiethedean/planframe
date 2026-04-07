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
    a("from typing import Any, Generic, Literal, TypeVar, overload")
    a("from typing_extensions import LiteralString")
    a("")
    a("from planframe.backend.adapter import BackendAdapter")
    a("from planframe.expr.api import Expr")
    a("from planframe.groupby import GroupedFrame")
    a("from planframe.plan.nodes import PlanNode")
    a("from planframe.schema.ir import Schema")
    a("from planframe.typing._schema_types import JoinedSchema")
    a("")
    a("SchemaT = TypeVar(\"SchemaT\")")
    a("BackendFrameT = TypeVar(\"BackendFrameT\")")
    a("BackendExprT = TypeVar(\"BackendExprT\")")
    a("OtherSchemaT = TypeVar(\"OtherSchemaT\")")
    a("T = TypeVar(\"T\")")
    a("")
    a("")
    a("class Frame(Generic[SchemaT, BackendFrameT, BackendExprT]):")
    a("    _data: BackendFrameT")
    a("    _adapter: BackendAdapter[BackendFrameT, BackendExprT]")
    a("    _plan: PlanNode")
    a("    _schema: Schema")
    a("")
    a("    @classmethod")
    a("    def source(")
    a("        cls,")
    a("        data: BackendFrameT,")
    a("        *,")
    a("        adapter: BackendAdapter[BackendFrameT, BackendExprT],")
    a("        schema: type[SchemaT],")
    a("    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    a("    def schema(self) -> Schema: ...")
    a("    def plan(self) -> PlanNode: ...")
    a("")
    a("    # NOTE: Pyright's behavior around LiteralString vs str can be permissive.")
    a("    # Overloads here are intended to encourage literal call sites and improve IDE help.")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def select(self, {params}) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def select(self, *columns: LiteralString) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    a("    def select_prefix(self, prefix: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def select_suffix(self, suffix: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def select_regex(self, pattern: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def select_exclude(self, {params}) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def select_exclude(self, *columns: LiteralString) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def drop(self, {params}) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def drop(self, *columns: LiteralString) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    a("    def drop_prefix(self, prefix: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def drop_suffix(self, suffix: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def drop_regex(self, pattern: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def reorder_columns(self, {params}) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def reorder_columns(self, *columns: LiteralString) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def select_first(self, {params}) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def select_first(self, *columns: LiteralString) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    for n in range(1, max_arity + 1):
        params = ", ".join([f"__c{i}: LiteralString" for i in range(1, n + 1)])
        a("    @overload")
        a(f"    def select_last(self, {params}) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def select_last(self, *columns: LiteralString) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    a("    def move(")
    a("        self,")
    a("        column: LiteralString,")
    a("        *,")
    a("        before: LiteralString | None = ...,")
    a("        after: LiteralString | None = ...,")
    a("    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    a("    def rename(self, **mapping: str) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    a("    def rename_prefix(self, prefix: str, *subset: LiteralString) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def rename_suffix(self, suffix: str, *subset: LiteralString) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def rename_replace(self, old: str, new: str, *subset: LiteralString) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    a("    def with_column(")
    a("        self,")
    a("        name: LiteralString,")
    a("        expr: Expr[T],")
    a("    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    a("    @overload")
    a("    def cast(self, name: LiteralString, dtype: type[T]) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    @overload")
    a("    def cast(self, name: LiteralString, dtype: Any) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def filter(self, predicate: Expr[bool]) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def sort(self, *columns: LiteralString, descending: bool = ...) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def unique(")
    a("        self,")
    a("        *subset: LiteralString,")
    a("        keep: Literal[\"first\", \"last\"] = ...,")
    a("    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def duplicated(")
    a("        self,")
    a("        *subset: LiteralString,")
    a("        keep: Literal[\"first\", \"last\"] | bool = ...,")
    a("        out_name: str = ...,")
    a("    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def group_by(self, *keys: LiteralString) -> GroupedFrame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def drop_nulls(self, *subset: LiteralString) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def fill_null(self, value: Any, *subset: LiteralString) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def melt(")
    a("        self,")
    a("        *,")
    a("        id_vars: tuple[LiteralString, ...],")
    a("        value_vars: tuple[LiteralString, ...],")
    a("        variable_name: str = ...,")
    a("        value_name: str = ...,")
    a("    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    a("    def slice(self, offset: int, length: int | None = ...) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def limit(self, n: int) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def head(self, n: int) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def tail(self, n: int) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def concat_vertical(self, other: Frame[SchemaT, BackendFrameT, BackendExprT]) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("    def pivot(")
    a("        self,")
    a("        *,")
    a("        index: tuple[LiteralString, ...],")
    a("        on: LiteralString,")
    a("        values: LiteralString,")
    a("        agg: Literal[\"first\", \"last\", \"sum\", \"mean\", \"min\", \"max\", \"count\", \"len\", \"median\"] = ...,")
    a("        on_columns: tuple[str, ...] | None = ...,")
    a("        separator: str = ...,")
    a("    ) -> Frame[SchemaT, BackendFrameT, BackendExprT]: ...")
    a("")
    a("    @overload")
    a("    def join(")
    a("        self,")
    a("        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],")
    a("        *,")
    a("        on: tuple[LiteralString],")
    a("        how: Literal[\"inner\", \"left\", \"right\", \"full\", \"semi\", \"anti\", \"cross\"] = ...,")
    a("        suffix: str = ...,")
    a("    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...")
    for n in range(2, 6):
        on_tuple = ", ".join(["LiteralString"] * n)
        a("    @overload")
        a("    def join(")
        a("        self,")
        a("        other: Frame[OtherSchemaT, BackendFrameT, BackendExprT],")
        a("        *,")
        a(f"        on: tuple[{on_tuple}],")
        a("        how: Literal[\"inner\", \"left\", \"right\", \"full\", \"semi\", \"anti\", \"cross\"] = ...,")
        a("        suffix: str = ...,")
        a("    ) -> Frame[JoinedSchema[SchemaT, OtherSchemaT], BackendFrameT, BackendExprT]: ...")
    a("    def collect(self) -> BackendFrameT: ...")
    a("")
    a("    def materialize_model(")
    a("        self,")
    a("        name: str,")
    a("        *,")
    a("        kind: Literal[\"dataclass\", \"pydantic\"] = ...,")
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
            "LeftSchemaT = TypeVar(\"LeftSchemaT\")",
            "RightSchemaT = TypeVar(\"RightSchemaT\")",
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

