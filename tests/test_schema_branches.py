from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict

import pytest
from pydantic import BaseModel

from planframe.backend.errors import PlanFrameSchemaError
from planframe.expr import add, coalesce, col, lit
from planframe.expr.api import Expr
from planframe.plan.nodes import JoinKeyColumn, JoinKeyExpr
from planframe.schema.ir import Field, Schema, collect_col_names_in_expr
from planframe.schema.materialize import materialize_model
from planframe.schema.source import schema_from_type


def test_schema_from_type_planframe_model_requires_annotations() -> None:
    class M:
        __planframe_model__ = True

    with pytest.raises(PlanFrameSchemaError, match="must have type annotations"):
        schema_from_type(M)


def test_schema_from_type_planframe_model_ignores_inherited_annotations() -> None:
    class Base:
        __planframe_model__ = True
        base_only: int

    class Child(Base):
        child_only: str

    s = schema_from_type(Child)
    assert s.names() == ("child_only",)


def test_schema_from_type_dataclass_missing_annotation_errors() -> None:
    @dataclass
    class DC:
        ok: int
        missing: int  # will be deleted from __annotations__

    del DC.__annotations__["missing"]

    with pytest.raises(PlanFrameSchemaError, match="Missing type annotation"):
        schema_from_type(DC)


def test_schema_from_type_pydantic_v2_like_model_fields_branch() -> None:
    class Info:
        def __init__(self, annotation: object) -> None:
            self.annotation = annotation

    class P2:
        model_fields = {"a": Info(int), "b": Info(str)}

    s = schema_from_type(P2)  # type: ignore[arg-type]
    assert s.names() == ("a", "b")


def test_schema_from_type_pydantic_v1_like___fields__branch() -> None:
    class Info:
        def __init__(self, outer_type_: object) -> None:
            self.outer_type_ = outer_type_

    class P1:
        __fields__ = {"a": Info(int), "b": Info(str)}

    s = schema_from_type(P1)  # type: ignore[arg-type]
    assert s.names() == ("a", "b")


def test_schema_from_type_unsupported_type_errors() -> None:
    with pytest.raises(PlanFrameSchemaError, match="Unsupported schema type"):
        schema_from_type(int)  # type: ignore[arg-type]


def test_materialize_model_pydantic_kind_creates_model() -> None:
    schema = Schema(fields=(Field(name="id", dtype=int), Field(name="name", dtype=str)))
    Model = materialize_model("Out", schema, kind="pydantic")
    assert issubclass(Model, BaseModel)
    inst = Model(id=1, name="a")
    assert inst.id == 1


def test_schema_ir_unnest_branches() -> None:
    class SStruct(TypedDict):
        x: int
        y: int

    schema = Schema(
        fields=(
            Field(name="id", dtype=int),
            Field(name="s", dtype=SStruct),
            Field(name="a", dtype=int),
        )
    )

    with pytest.raises(PlanFrameSchemaError, match="requires at least one column"):
        schema.unnest(())

    with pytest.raises(PlanFrameSchemaError, match="columns must be unique"):
        schema.unnest(("s", "s"))

    with pytest.raises(PlanFrameSchemaError, match="Cannot unnest missing columns"):
        schema.unnest(("missing",))

    schema2 = Schema(fields=(Field(name="s", dtype=dict),))
    with pytest.raises(PlanFrameSchemaError, match="requires schema field names"):
        schema2.unnest(("s",))

    schema3 = Schema(fields=(Field(name="s", dtype=SStruct), Field(name="x", dtype=int)))
    with pytest.raises(PlanFrameSchemaError, match="duplicate column name"):
        schema3.unnest(("s",))


def test_schema_ir_melt_duplicate_names_and_missing_columns() -> None:
    schema = Schema(
        fields=(Field(name="id", dtype=int), Field(name="a", dtype=int), Field(name="b", dtype=int))
    )

    with pytest.raises(PlanFrameSchemaError, match="Cannot melt missing columns"):
        schema.melt(id_vars=("id",), value_vars=("missing",), variable_name="k", value_name="v")

    with pytest.raises(PlanFrameSchemaError, match="duplicate column names"):
        schema.melt(id_vars=("id",), value_vars=("a",), variable_name="id", value_name="v")


def test_schema_ir_join_merge_validation_and_suffix_collision() -> None:
    left = Schema(fields=(Field(name="id", dtype=int), Field(name="x", dtype=int)))
    right = Schema(fields=(Field(name="id", dtype=int), Field(name="x", dtype=int)))

    with pytest.raises(PlanFrameSchemaError, match="requires non-empty join keys"):
        left.join_merge(right, left_on=(), right_on=())

    with pytest.raises(PlanFrameSchemaError, match="must have the same length"):
        left.join_merge(
            right,
            left_on=(JoinKeyColumn("id"),),
            right_on=(JoinKeyColumn("id"), JoinKeyColumn("x")),
        )

    # Suffix collision: right has `x` which collides with left `x`, and suffix produces `x_right`
    # which collides again because left already contains `x_right`.
    left2 = Schema(
        fields=(
            Field(name="id", dtype=int),
            Field(name="x", dtype=int),
            Field(name="x_right", dtype=int),
        )
    )
    with pytest.raises(PlanFrameSchemaError, match="suffix collision"):
        left2.join_merge(
            right,
            left_on=(JoinKeyColumn("id"),),
            right_on=(JoinKeyColumn("id"),),
            suffix="_right",
        )

    # Missing join keys on left/right
    with pytest.raises(PlanFrameSchemaError, match="Join keys missing on left"):
        left.join_merge(right, left_on=(JoinKeyColumn("missing"),), right_on=(JoinKeyColumn("id"),))
    with pytest.raises(PlanFrameSchemaError, match="Join keys missing on right"):
        left.join_merge(right, left_on=(JoinKeyColumn("id"),), right_on=(JoinKeyColumn("missing"),))

    with pytest.raises(PlanFrameSchemaError, match="references unknown columns on left"):
        left.join_merge(
            right,
            left_on=(JoinKeyExpr(expr=add(col("missing"), lit(1))),),
            right_on=(JoinKeyColumn("id"),),
        )

    # join_merge_cross suffix collision
    with pytest.raises(PlanFrameSchemaError, match="suffix collision"):
        left2.join_merge_cross(right, suffix="_right")


def test_collect_col_names_in_expr_nested_and_tuple_fields() -> None:
    # Hit non-Col path with nested Expr and tuple fields.
    expr = add(col("a"), add(col("b"), lit(1)))
    assert collect_col_names_in_expr(expr) == frozenset({"a", "b"})

    # Defensive non-dataclass Expr node
    class Plain(Expr[object]):
        pass

    assert collect_col_names_in_expr(Plain()) == frozenset()

    # Tuple field containing Exprs (Coalesce.values)
    expr2 = coalesce(col("a"), col("b"))
    assert collect_col_names_in_expr(expr2) == frozenset({"a", "b"})


def test_schema_get_and_select_errors() -> None:
    schema = Schema(fields=(Field(name="id", dtype=int),))
    with pytest.raises(PlanFrameSchemaError, match="Unknown column"):
        schema.get("missing")
    with pytest.raises(PlanFrameSchemaError, match="Cannot select missing column"):
        schema.select(("missing",))

    with pytest.raises(PlanFrameSchemaError, match="Cannot cast missing column"):
        schema.cast("missing", int)


def test_schema_project_and_drop_rename_errors() -> None:
    schema = Schema(fields=(Field(name="a", dtype=int),))
    from planframe.plan.nodes import ProjectExpr, ProjectPick

    with pytest.raises(PlanFrameSchemaError, match="Cannot project missing column"):
        schema.project((ProjectPick(column="missing"),))

    with pytest.raises(PlanFrameSchemaError, match="project repeats"):
        schema.project((ProjectPick(column="a"), ProjectPick(column="a")))

    with pytest.raises(PlanFrameSchemaError, match="project repeats"):
        schema.project((ProjectExpr(name="x", expr=lit(1)), ProjectExpr(name="x", expr=lit(2))))

    with pytest.raises(PlanFrameSchemaError, match="Cannot drop missing columns"):
        schema.drop(("missing",), strict=True)

    with pytest.raises(PlanFrameSchemaError, match="Cannot rename missing columns"):
        schema.rename({"missing": "x"}, strict=True)

    with pytest.raises(PlanFrameSchemaError, match="Cannot exclude missing columns"):
        schema.select_exclude(("missing",))


def test_schema_reorder_and_select_first_last_and_move_branches() -> None:
    schema = Schema(
        fields=(Field(name="a", dtype=int), Field(name="b", dtype=int), Field(name="c", dtype=int))
    )

    with pytest.raises(PlanFrameSchemaError, match="Cannot reorder with duplicate"):
        schema.reorder_columns(("a", "a", "b"))
    with pytest.raises(PlanFrameSchemaError, match="Cannot reorder with missing columns"):
        schema.reorder_columns(("missing",))
    with pytest.raises(PlanFrameSchemaError, match="reorder_columns must include every column"):
        schema.reorder_columns(("a", "b"))

    with pytest.raises(PlanFrameSchemaError, match="Cannot select_first missing"):
        schema.select_first(("missing",))
    with pytest.raises(PlanFrameSchemaError, match="Cannot select_first with duplicate"):
        schema.select_first(("a", "a"))

    with pytest.raises(PlanFrameSchemaError, match="Cannot select_last missing"):
        schema.select_last(("missing",))
    with pytest.raises(PlanFrameSchemaError, match="Cannot select_last with duplicate"):
        schema.select_last(("a", "a"))

    with pytest.raises(PlanFrameSchemaError, match="move requires exactly one"):
        schema.move("a")
    with pytest.raises(PlanFrameSchemaError, match="Cannot move missing column"):
        schema.move("missing", before="a")
    with pytest.raises(PlanFrameSchemaError, match="Cannot move relative to missing"):
        schema.move("a", before="missing")
    assert schema.move("a", before="a") is schema
