from __future__ import annotations

from dataclasses import dataclass

import polars as pl
import pytest

from planframe.expr import abs_, add, and_, ceil, coalesce, col, eq, floor, gt, if_else, is_not_null, lit, mul, round_, xor
from planframe_polars import from_polars


@dataclass(frozen=True)
class UserSchema:
    id: int
    name: str
    age: int


def test_select_drop_rename_with_column_filter_collect() -> None:
    lf = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]}).lazy()
    pf = from_polars(lf, schema=UserSchema)

    out = (
        pf.select("id", "name", "age")
        .drop("name")
        .rename(age="years")
        .with_column("years_plus_one", add(col("years"), lit(1)))
        .filter(eq(col("id"), lit(1)))
    )

    assert out.schema().names() == ("id", "years", "years_plus_one")

    collected = out.collect()
    assert isinstance(collected, pl.DataFrame)
    assert collected.columns == ["id", "years", "years_plus_one"]
    assert collected.height == 1


def test_materialize_model_dataclass() -> None:
    lf = pl.DataFrame({"id": [1], "name": ["a"], "age": [10]}).lazy()
    pf = from_polars(lf, schema=UserSchema)
    out = pf.select("id", "age").with_column("age_plus_one", add(col("age"), lit(1)))

    Model = out.materialize_model("Out", kind="dataclass")
    assert Model.__name__ == "Out"
    assert set(Model.__annotations__.keys()) == {"id", "age", "age_plus_one"}


def test_schema_convenience_ops_affect_column_order_and_names() -> None:
    lf = pl.DataFrame({"id": [1], "name": ["a"], "age": [10]}).lazy()
    pf = from_polars(lf, schema=UserSchema)

    out = (
        pf.select_exclude("name")
        .select_first("age")
        .rename_prefix("x_", "id")
        .move("x_id", after="age")
    )

    assert out.schema().names() == ("age", "x_id")

    collected = out.collect()
    assert collected.columns == ["age", "x_id"]


def test_extended_expressions_compile_and_filter() -> None:
    lf = pl.DataFrame({"id": [1, 2, 3], "age": [10, 20, None]}).lazy()
    pf = from_polars(lf, schema=UserSchema)

    out = (
        pf.select("id", "age")
        .filter(and_(gt(col("age"), lit(10)), is_not_null(col("age"))))
        .with_column("age_times_two", mul(col("age"), lit(2)))
    )

    collected = out.collect()
    assert collected.columns == ["id", "age", "age_times_two"]
    assert collected["id"].to_list() == [2]


def test_more_expressions_abs_round_floor_ceil_coalesce_if_else_xor() -> None:
    lf = pl.DataFrame({"id": [1, 2], "x": [-1.2, 3.4], "a": [None, 5], "b": [10, None]}).lazy()

    @dataclass(frozen=True)
    class S:
        id: int
        x: float
        a: int | None
        b: int | None

    pf = from_polars(lf, schema=S)
    out = pf.select("id", "x", "a", "b").with_column("ax", abs_(col("x")))
    out = out.with_column("rx", round_(col("x"), 0))
    out = out.with_column("fx", floor(col("x")))
    out = out.with_column("cx", ceil(col("x")))
    out = out.with_column("c", coalesce(col("a"), col("b")))
    out = out.with_column("flag", xor(eq(col("id"), lit(1)), eq(col("id"), lit(2))))
    out = out.with_column("picked", if_else(eq(col("id"), lit(1)), lit("one"), lit("other")))

    collected = out.collect()
    assert collected.columns == ["id", "x", "a", "b", "ax", "rx", "fx", "cx", "c", "flag", "picked"]


def test_sort_unique_duplicated() -> None:
    lf = pl.DataFrame({"id": [2, 1, 1], "name": ["b", "a", "a"], "age": [20, 10, 10]}).lazy()
    pf = from_polars(lf, schema=UserSchema)

    sorted_pf = pf.sort("id")
    assert sorted_pf.collect()["id"].to_list() == [1, 1, 2]

    uniq = pf.unique("id", keep="first").sort("id").collect()
    assert uniq["id"].to_list() == [1, 2]

    dups = pf.duplicated("id").collect()
    assert dups.columns == ["duplicated"]
    assert dups["duplicated"].dtype == pl.Boolean


def test_group_by_agg() -> None:
    lf = pl.DataFrame({"id": [1, 1, 2], "age": [10, 20, 30], "name": ["a", "b", "c"]}).lazy()
    pf = from_polars(lf, schema=UserSchema)

    out = pf.group_by("id").agg(total_age=("sum", "age"), n=("count", "name")).sort("id")
    collected = out.collect()
    assert collected.columns == ["id", "total_age", "n"]
    assert collected["n"].to_list() == [2, 1]


def test_sort_descending() -> None:
    lf = pl.DataFrame({"id": [2, 1, 3], "name": ["b", "a", "c"], "age": [20, 10, 30]}).lazy()
    pf = from_polars(lf, schema=UserSchema)
    out = pf.sort("id", descending=True).collect()
    assert out["id"].to_list() == [3, 2, 1]


def test_unique_no_subset_keeps_one_row_per_full_row() -> None:
    lf = pl.DataFrame({"id": [1, 1, 1], "name": ["a", "a", "b"], "age": [10, 10, 10]}).lazy()
    pf = from_polars(lf, schema=UserSchema)
    out = pf.unique().collect()
    # rows are (1,a,10) and (1,b,10)
    assert out.height == 2


def test_duplicated_keep_false_not_supported() -> None:
    lf = pl.DataFrame({"id": [1, 1, 2], "name": ["a", "b", "c"], "age": [10, 20, 30]}).lazy()
    pf = from_polars(lf, schema=UserSchema)
    import pytest

    from planframe.backend.errors import PlanFrameExecutionError

    with pytest.raises(PlanFrameExecutionError):
        pf.duplicated("id", keep=False).collect()


def test_drop_nulls_fill_null_and_melt() -> None:
    lf = pl.DataFrame({"id": [1, 2], "a": [None, 5], "b": [10, 20]}).lazy()

    @dataclass(frozen=True)
    class S:
        id: int
        a: int | None
        b: int

    pf = from_polars(lf, schema=S)

    filled = pf.fill_null(0, "a")
    out = filled.drop_nulls("a")
    collected = out.collect()
    assert collected["a"].to_list() == [0, 5]

    melted = pf.melt(id_vars=("id",), value_vars=("a", "b"), variable_name="k", value_name="v")
    m = melted.collect()
    assert m.columns == ["id", "k", "v"]


def test_drop_nulls_all_columns_and_fill_null_all_columns() -> None:
    lf = pl.DataFrame({"id": [1, None], "a": [None, 5]}).lazy()

    @dataclass(frozen=True)
    class S2:
        id: int | None
        a: int | None

    pf = from_polars(lf, schema=S2)
    filled = pf.fill_null(0).collect()
    assert filled["id"].to_list() == [1, 0]
    assert filled["a"].to_list() == [0, 5]

    dropped = pf.drop_nulls().collect()
    # Only row with no nulls remains; both rows have a null, so empty.
    assert dropped.height == 0


def test_join_inner_key_drop_and_collision_suffixing() -> None:
    left_lf = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]}).lazy()
    right_lf = pl.DataFrame({"id": [1, 1], "name": ["x", "y"], "city": ["NY", "SF"]}).lazy()

    @dataclass(frozen=True)
    class Left:
        id: int
        name: str
        age: int

    @dataclass(frozen=True)
    class Right:
        id: int
        name: str
        city: str

    left = from_polars(left_lf, schema=Left)
    right = from_polars(right_lf, schema=Right)

    out = left.join(right, on=("id",), suffix="_right")
    assert out.schema().names() == ("id", "name", "age", "name_right", "city")

    collected = out.collect()
    assert collected.columns == ["id", "name", "age", "name_right", "city"]
    assert collected.height == 2


def test_row_ops_head_tail_slice_limit() -> None:
    lf = pl.DataFrame({"id": [1, 2, 3, 4], "name": ["a", "b", "c", "d"], "age": [10, 20, 30, 40]}).lazy()
    pf = from_polars(lf, schema=UserSchema)

    out = pf.head(3).slice(1, 2).tail(1).limit(1)
    collected = out.collect()
    assert collected["id"].to_list() == [3]


def test_row_ops_slice_length_none_and_offset_past_end() -> None:
    lf = pl.DataFrame({"id": [1, 2, 3]}).lazy()

    @dataclass(frozen=True)
    class S:
        id: int

    pf = from_polars(lf, schema=S)
    assert pf.slice(1, None).collect()["id"].to_list() == [2, 3]
    assert pf.slice(999, None).collect().height == 0


def test_row_ops_head_tail_zero() -> None:
    lf = pl.DataFrame({"id": [1, 2, 3]}).lazy()

    @dataclass(frozen=True)
    class S:
        id: int

    pf = from_polars(lf, schema=S)
    assert pf.head(0).collect().height == 0
    assert pf.tail(0).collect().height == 0


def test_pattern_select_and_drop() -> None:
    lf = pl.DataFrame({"id": [1], "x_a": [10], "x_b": [20], "y": [30]}).lazy()

    @dataclass(frozen=True)
    class S:
        id: int
        x_a: int
        x_b: int
        y: int

    pf = from_polars(lf, schema=S)

    out = pf.select_prefix("x_")
    assert out.schema().names() == ("x_a", "x_b")
    assert out.collect().columns == ["x_a", "x_b"]

    out2 = pf.drop_regex("^x_")
    assert out2.schema().names() == ("id", "y")
    assert out2.collect().columns == ["id", "y"]


def test_pattern_ops_select_regex_no_matches_returns_empty_schema() -> None:
    lf = pl.DataFrame({"id": [1], "x": [2]}).lazy()

    @dataclass(frozen=True)
    class S:
        id: int
        x: int

    pf = from_polars(lf, schema=S)
    out = pf.select_regex("^does_not_exist$")
    assert out.schema().names() == ()
    assert out.collect().columns == []


def test_concat_vertical() -> None:
    lf1 = pl.DataFrame({"id": [1], "name": ["a"], "age": [10]}).lazy()
    lf2 = pl.DataFrame({"id": [2], "name": ["b"], "age": [20]}).lazy()

    pf1 = from_polars(lf1, schema=UserSchema)
    pf2 = from_polars(lf2, schema=UserSchema)

    out = pf1.concat_vertical(pf2).sort("id")
    collected = out.collect()
    assert collected["id"].to_list() == [1, 2]


def test_concat_vertical_preserves_order_without_sort() -> None:
    lf1 = pl.DataFrame({"id": [2], "name": ["b"], "age": [20]}).lazy()
    lf2 = pl.DataFrame({"id": [1], "name": ["a"], "age": [10]}).lazy()
    pf1 = from_polars(lf1, schema=UserSchema)
    pf2 = from_polars(lf2, schema=UserSchema)
    collected = pf1.concat_vertical(pf2).collect()
    assert collected["id"].to_list() == [2, 1]


def test_pivot_with_lazyframe_requires_on_columns_and_is_deterministic() -> None:
    lf = pl.DataFrame({"id": [1, 1], "k": ["a", "b"], "v": [10, 20]}).lazy()

    @dataclass(frozen=True)
    class S:
        id: int
        k: str
        v: int

    pf = from_polars(lf, schema=S)
    out = pf.pivot(index=("id",), on="k", values="v", on_columns=("a", "b"), agg="first")
    collected = out.collect()
    assert collected.columns == ["id", "a", "b"]
    assert collected["a"].to_list() == [10]
    assert collected["b"].to_list() == [20]


def test_pivot_handles_missing_on_columns_as_nulls() -> None:
    lf = pl.DataFrame({"id": [1], "k": ["a"], "v": [10]}).lazy()

    @dataclass(frozen=True)
    class S:
        id: int
        k: str
        v: int

    pf = from_polars(lf, schema=S)
    collected = pf.pivot(index=("id",), on="k", values="v", on_columns=("a", "b")).collect()
    assert collected.columns == ["id", "a", "b"]
    assert collected["a"].to_list() == [10]
    assert collected["b"].to_list() == [None]


def test_pivot_lazy_without_on_columns_raises_execution_error() -> None:
    lf = pl.DataFrame({"id": [1, 1], "k": ["a", "b"], "v": [10, 20]}).lazy()

    @dataclass(frozen=True)
    class S:
        id: int
        k: str
        v: int

    pf = from_polars(lf, schema=S)
    from planframe.backend.errors import PlanFrameExecutionError

    with pytest.raises(PlanFrameExecutionError):
        pf.pivot(index=("id",), on="k", values="v", on_columns=None).collect()

