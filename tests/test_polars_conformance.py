from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict

import polars as pl
import pytest

from planframe.expr import (
    abs_,
    add,
    and_,
    between,
    ceil,
    clip,
    coalesce,
    col,
    contains,
    day,
    ends_with,
    eq,
    exp,
    floor,
    gt,
    if_else,
    is_not_null,
    length,
    lit,
    log,
    lower,
    month,
    mul,
    over,
    pow_,
    replace,
    round_,
    starts_with,
    xor,
    year,
)
from planframe_polars import PolarsFrame

pytestmark = pytest.mark.conformance


class _SStructAB(TypedDict):
    a: int | None
    b: int | None


class _SStructId(TypedDict):
    id: int


scan_parquet = PolarsFrame.scan_parquet
scan_parquet_dataset = PolarsFrame.scan_parquet_dataset
scan_csv = PolarsFrame.scan_csv
scan_ndjson = PolarsFrame.scan_ndjson
scan_ipc = PolarsFrame.scan_ipc
scan_delta = PolarsFrame.scan_delta
read_database = PolarsFrame.read_database
read_database_uri = PolarsFrame.read_database_uri
read_delta = PolarsFrame.read_delta
read_excel = PolarsFrame.read_excel
read_avro = PolarsFrame.read_avro


@dataclass(frozen=True)
class UserSchema:
    id: int
    name: str
    age: int


class User(PolarsFrame):
    id: int
    name: str
    age: int


def test_drop_strict_false_ignores_unknown_polars_columns() -> None:
    pf = User({"id": [1], "name": ["a"], "age": [10]})
    out = pf.select("id", "name", "age").drop("not_a_column", strict=False)
    df = out.collect()
    assert df.columns == ["id", "name", "age"]


def test_rename_strict_false_ignores_unknown_polars_columns() -> None:
    pf = User({"id": [1], "name": ["a"], "age": [10]})
    out = pf.select("id", "name", "age").rename(name="full_name", not_a_column="x", strict=False)
    df = out.collect()
    assert df.columns == ["id", "full_name", "age"]


def test_select_mixed_str_and_expr_polars() -> None:
    pf = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
    out = pf.select("id", ("twice_age", mul(col("age"), lit(2))))
    df = out.collect()
    assert df.columns == ["id", "twice_age"]
    assert df["id"].to_list() == [1, 2]
    assert df["twice_age"].to_list() == [20, 40]


def test_sort_expression_key_polars() -> None:
    pf = User({"id": [1, 2, 3], "name": ["a", "b", "c"], "age": [30, 10, 20]})
    out = pf.sort(add(col("id"), col("age")))
    df = out.collect()
    assert df["id"].to_list() == [2, 3, 1]


def test_sort_mixed_column_and_expr_polars() -> None:
    pf = User({"id": [1, 2], "name": ["b", "a"], "age": [10, 20]})
    out = pf.sort("name", add(col("id"), col("age")))
    df = out.collect()
    assert df["id"].to_list() == [2, 1]


def test_select_drop_rename_with_column_filter_collect() -> None:
    pf = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})

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

    rows = out.collect(kind="dataclass", name="OutRow")
    assert len(rows) == 1
    assert rows[0].id == 1
    assert rows[0].years == 10


def test_optimize_preserves_results_polars() -> None:
    pf = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
    out = pf.select("id", "name", "age").select("id", "age").rename(age="years").drop()

    unopt = out.collect()
    opt = out.optimize(level=1).collect()

    assert unopt.columns == opt.columns
    assert unopt.to_dicts() == opt.to_dicts()


def test_select_equivalence_with_column_polars() -> None:
    pf = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})

    a = pf.select("id", ("years_plus_one", add(col("age"), lit(1)))).collect()
    b = (
        pf.with_column("years_plus_one", add(col("age"), lit(1)))
        .select("id", "years_plus_one")
        .collect()
    )

    assert a.columns == b.columns
    assert a.to_dicts() == b.to_dicts()


def test_materialize_model_dataclass() -> None:
    pf = User({"id": [1], "name": ["a"], "age": [10]})
    out = pf.select("id", "age").with_column("age_plus_one", add(col("age"), lit(1)))

    Model = out.materialize_model("Out", kind="dataclass")
    assert Model.__name__ == "Out"
    assert set(Model.__annotations__.keys()) == {"id", "age", "age_plus_one"}


def test_constructor_fills_missing_columns_from_schema_defaults() -> None:
    class UserWithDefaults(PolarsFrame):
        id: int
        name: str
        age: int
        active: bool = True

    pf = UserWithDefaults([{"id": 1, "name": "a", "age": 10}, {"id": 2, "name": "b", "age": 20}])
    assert pf.select("active").collect()["active"].to_list() == [True, True]

    pf2 = UserWithDefaults({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
    assert pf2.select("active").collect()["active"].to_list() == [True, True]


def test_schema_convenience_ops_affect_column_order_and_names() -> None:
    pf = User({"id": [1], "name": ["a"], "age": [10]})

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
    pf = User({"id": [1, 2, 3], "name": ["a", "b", "c"], "age": [10, 20, None]})

    out = (
        pf.select("id", "age")
        .filter(and_(gt(col("age"), lit(10)), is_not_null(col("age"))))
        .with_column("age_times_two", mul(col("age"), lit(2)))
    )

    collected = out.collect()
    assert collected.columns == ["id", "age", "age_times_two"]
    assert collected["id"].to_list() == [2]


def test_more_expressions_abs_round_floor_ceil_coalesce_if_else_xor() -> None:
    data = {"id": [1, 2], "x": [-1.2, 3.4], "a": [None, 5], "b": [10, None]}

    class S(PolarsFrame):
        id: int
        x: float
        a: int | None
        b: int | None

    pf = S(data)
    out = pf.select("id", "x", "a", "b").with_column("ax", abs_(col("x")))
    out = out.with_column("rx", round_(col("x"), 0))
    out = out.with_column("fx", floor(col("x")))
    out = out.with_column("cx", ceil(col("x")))
    out = out.with_column("c", coalesce(col("a"), col("b")))
    out = out.with_column("flag", xor(eq(col("id"), lit(1)), eq(col("id"), lit(2))))
    out = out.with_column("picked", if_else(eq(col("id"), lit(1)), lit("one"), lit("other")))

    collected = out.collect()
    assert collected.columns == ["id", "x", "a", "b", "ax", "rx", "fx", "cx", "c", "flag", "picked"]


def test_string_datetime_math_window_expressions() -> None:
    from datetime import datetime

    data = {
        "id": [1, 1, 2],
        "s": ["Hello", "world", "HELLO"],
        "x": [1.0, 2.0, 3.0],
        "dt": [datetime(2026, 1, 2), datetime(2026, 1, 3), datetime(2025, 12, 31)],
    }

    class S(PolarsFrame):
        id: int
        s: str
        x: float
        dt: object

    pf = S(data)
    out = (
        pf.select("id", "s", "x", "dt")
        .with_column("has_hello", contains(lower(col("s")), "hello"))
        .with_column("sw_h", starts_with(col("s"), "H"))
        .with_column("ew_o", ends_with(col("s"), "o"))
        .with_column("s_len", length(col("s")))
        .with_column("s2", replace(col("s"), "l", "L", literal=True))
        .with_column("y", year(col("dt")))
        .with_column("m", month(col("dt")))
        .with_column("d", day(col("dt")))
        .with_column("btw", between(col("x"), lit(1.5), lit(3.0)))
        .with_column("clp", clip(col("x"), lower=lit(1.5)))
        .with_column("p", pow_(col("x"), lit(2)))
        .with_column("lg", log(exp(lit(1.0))))
        .with_column("x_max_by_id", over(col("x"), partition_by=("id",)))
    )

    df = out.collect()
    assert "x_max_by_id" in df.columns


def test_string_ops_nulls_and_literal_replace() -> None:
    data = {"s": ["a.a", None, "bbb"]}

    class S(PolarsFrame):
        s: str | None

    pf = S(data)
    out = (
        pf.with_column("c1", contains(col("s"), ".", literal=True))
        .with_column("c2", contains(col("s"), ".", literal=False))
        .with_column("r", replace(col("s"), ".", "_", literal=True))
        .with_column("ln", length(col("s")))
    )
    df = out.collect()
    assert df.columns == ["s", "c1", "c2", "r", "ln"]


def test_strip_split_sqrt_is_finite_exprs() -> None:
    data = {"s": ["  a,b  "], "x": [4.0], "y": [float("inf")]}

    class S(PolarsFrame):
        s: str
        x: float
        y: float

    pf = S(data)
    from planframe.expr import is_finite, split, sqrt, strip

    df = (
        pf.with_column("s2", strip(col("s")))
        .with_column("parts", split(strip(col("s")), ","))
        .with_column("r", sqrt(col("x")))
        .with_column("ok", is_finite(col("y")))
        .collect()
    )
    assert df.columns == ["s", "x", "y", "s2", "parts", "r", "ok"]


def test_window_over_partition_by_multiple_keys() -> None:
    data = {"g1": [1, 1, 1, 2], "g2": ["a", "a", "b", "a"], "x": [1, 2, 3, 4]}

    class S(PolarsFrame):
        g1: int
        g2: str
        x: int

    pf = S(data)
    # Note: `over()` applies window context; without an aggregation the values remain unchanged.
    df = (
        pf.with_column("x_over", over(col("x"), partition_by=("g1", "g2"), order_by=("x",)))
        .sort("x")
        .collect()
    )
    assert df["x_over"].to_list() == [1, 2, 3, 4]


def test_sort_unique_duplicated() -> None:
    pf = User({"id": [2, 1, 1], "name": ["b", "a", "a"], "age": [20, 10, 10]})

    sorted_pf = pf.sort("id")
    assert sorted_pf.collect()["id"].to_list() == [1, 1, 2]

    uniq = pf.unique("id", keep="first").sort("id").collect()
    assert uniq["id"].to_list() == [1, 2]

    dups = pf.duplicated("id").collect()
    assert dups.columns == ["duplicated"]
    assert dups["duplicated"].dtype == pl.Boolean


def test_group_by_agg() -> None:
    pf = User({"id": [1, 1, 2], "name": ["a", "b", "c"], "age": [10, 20, 30]})

    out = pf.group_by("id").agg(total_age=("sum", "age"), n=("count", "name")).sort("id")
    collected = out.collect()
    assert collected.columns == ["id", "total_age", "n"]
    assert collected["n"].to_list() == [2, 1]


def test_group_by_expression_key_polars() -> None:
    pf = User({"name": ["A", "a", "B"], "age": [1, 2, 10], "id": [1, 2, 3]})
    out = (
        pf.group_by(lower(col("name")))
        .agg(n=("count", "age"), total=("sum", "age"))
        .sort("__pf_g0")
    )
    collected = out.collect()
    assert collected.columns == ["__pf_g0", "n", "total"]
    assert collected["__pf_g0"].to_list() == ["a", "b"]
    assert collected["n"].to_list() == [2, 1]
    assert collected["total"].to_list() == [3, 10]


def test_group_by_agg_expression_polars() -> None:
    from planframe.expr import agg_sum, truediv

    pf = User({"id": [1, 1, 2], "age": [10, 20, 15], "name": ["a", "b", "c"]})
    out = pf.group_by("id").agg(s=agg_sum(truediv(col("age"), col("id")))).sort("id")
    collected = out.collect()
    assert collected["s"].to_list() == [30.0, 7.5]


def test_sort_descending() -> None:
    pf = User({"id": [2, 1, 3], "name": ["b", "a", "c"], "age": [20, 10, 30]})
    out = pf.sort("id", descending=True).collect()
    assert out["id"].to_list() == [3, 2, 1]


def test_sort_per_key_descending_and_nulls_last() -> None:
    pf = User({"id": [1, 1, 2, 2], "name": ["b", "a", "d", "c"], "age": [1, 2, 3, 4]})
    out = pf.sort("id", "name", descending=[False, True], nulls_last=[True, False]).collect()
    assert [(r["id"], r["name"]) for r in out.to_dicts()] == [
        (1, "b"),
        (1, "a"),
        (2, "d"),
        (2, "c"),
    ]


def test_unique_no_subset_keeps_one_row_per_full_row() -> None:
    pf = User({"id": [1, 1, 1], "name": ["a", "a", "b"], "age": [10, 10, 10]})
    out = pf.unique().collect()
    # rows are (1,a,10) and (1,b,10)
    assert out.height == 2


def test_duplicated_keep_false_marks_all_duplicates() -> None:
    pf = User({"id": [1, 1, 2, 3, 3], "name": ["a", "b", "c", "d", "e"], "age": [10, 20, 30, 1, 2]})
    out = pf.duplicated("id", keep=False).collect()
    assert out["duplicated"].to_list() == [True, True, False, True, True]


def test_drop_nulls_fill_null_and_melt() -> None:
    data = {"id": [1, 2], "a": [None, 5], "b": [10, 20]}

    class S(PolarsFrame):
        id: int
        a: int | None
        b: int

    pf = S(data)

    filled = pf.fill_null(0, "a")
    out = filled.drop_nulls("a")
    collected = out.collect()
    assert collected["a"].to_list() == [0, 5]

    # strategy-based fill (forward fill)
    ff = S({"id": [1, 2, 3], "a": [None, 5, None], "b": [10, 20, 30]})
    ff_out = ff.fill_null(None, "a", strategy="forward").collect()
    assert ff_out["a"].to_list() == [None, 5, 5]

    # expression fill value
    expr_fill = pf.fill_null(add(col("b"), lit(1)), "a").collect()
    assert expr_fill["a"].to_list() == [11, 5]

    melted = pf.melt(id_vars=("id",), value_vars=("a", "b"), variable_name="k", value_name="v")
    m = melted.collect()
    assert m.columns == ["id", "k", "v"]


def test_drop_nulls_all_columns_and_fill_null_all_columns() -> None:
    data = {"id": [1, None], "a": [None, 5]}

    class S2(PolarsFrame):
        id: int | None
        a: int | None

    pf = S2(data)
    filled = pf.fill_null(0).collect()
    assert filled["id"].to_list() == [1, 0]
    assert filled["a"].to_list() == [0, 5]

    dropped = pf.drop_nulls().collect()
    # Only row with no nulls remains; both rows have a null, so empty.
    assert dropped.height == 0


def test_join_inner_key_drop_and_collision_suffixing() -> None:
    left_data = {"id": [1, 2], "name": ["a", "b"], "age": [10, 20]}
    right_data = {"id": [1, 1], "name": ["x", "y"], "city": ["NY", "SF"]}

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

    class LeftFrame(PolarsFrame):
        id: int
        name: str
        age: int

    class RightFrame(PolarsFrame):
        id: int
        name: str
        city: str

    left = LeftFrame(left_data)
    right = RightFrame(right_data)

    out = left.join(right, on=("id",), suffix="_right")
    assert out.schema().names() == ("id", "name", "age", "name_right", "city")

    collected = out.collect()
    assert collected.columns == ["id", "name", "age", "name_right", "city"]
    assert collected.height == 2


def test_join_left_on_right_on_polars() -> None:
    @dataclass(frozen=True)
    class L:
        user_id: int
        x: int

    @dataclass(frozen=True)
    class R:
        id: int
        y: int

    class LF(PolarsFrame):
        user_id: int
        x: int

    class RF(PolarsFrame):
        id: int
        y: int

    left_pf = LF({"user_id": [1, 2], "x": [10, 20]})
    right_pf = RF({"id": [1, 3], "y": [100, 300]})
    out = left_pf.join(right_pf, left_on=("user_id",), right_on=("id",), how="inner").collect()
    assert out.to_dict(as_series=False) == {"user_id": [1], "x": [10], "y": [100]}


def test_join_expression_keys_polars() -> None:
    @dataclass(frozen=True)
    class L:
        id: int
        email: str

    @dataclass(frozen=True)
    class R:
        rid: int
        email_norm: str

    class LF(PolarsFrame):
        id: int
        email: str

    class RF(PolarsFrame):
        rid: int
        email_norm: str

    left_pf = LF({"id": [1, 2], "email": ["A@x.com", "b@y.com"]})
    right_pf = RF({"rid": [10, 20], "email_norm": ["a@x.com", "b@y.com"]})
    out = left_pf.join(
        right_pf,
        left_on=(lower(col("email")),),
        right_on=(lower(col("email_norm")),),
        how="inner",
    ).collect()
    assert out.height == 2
    assert set(out["id"].to_list()) == {1, 2}


def test_row_ops_head_tail_slice_limit() -> None:
    pf = User({"id": [1, 2, 3, 4], "name": ["a", "b", "c", "d"], "age": [10, 20, 30, 40]})

    out = pf.head(3).slice(1, 2).tail(1).limit(1)
    collected = out.collect()
    assert collected["id"].to_list() == [3]


def test_clip_subset_and_all_numeric_polars() -> None:
    pf = User({"id": [1, 2], "name": ["a", "b"], "age": [-1, 10]})

    df_subset = pf.clip(lower=0, upper=6, subset=("age",)).sort("id").collect()
    assert df_subset.to_dict(as_series=False) == {"id": [1, 2], "name": ["a", "b"], "age": [0, 6]}

    df_all = pf.clip(lower=0).sort("id").collect()
    assert df_all.to_dict(as_series=False) == {"id": [1, 2], "name": ["a", "b"], "age": [0, 10]}


def test_row_ops_slice_length_none_and_offset_past_end() -> None:
    data = {"id": [1, 2, 3]}

    class S(PolarsFrame):
        id: int

    pf = S(data)
    assert pf.slice(1, None).collect()["id"].to_list() == [2, 3]
    assert pf.slice(999, None).collect().height == 0


def test_row_ops_head_tail_zero() -> None:
    data = {"id": [1, 2, 3]}

    class S(PolarsFrame):
        id: int

    pf = S(data)
    assert pf.head(0).collect().height == 0
    assert pf.tail(0).collect().height == 0


def test_pattern_select_and_drop() -> None:
    data = {"id": [1], "x_a": [10], "x_b": [20], "y": [30]}

    class S(PolarsFrame):
        id: int
        x_a: int
        x_b: int
        y: int

    pf = S(data)

    out = pf.select_prefix("x_")
    assert out.schema().names() == ("x_a", "x_b")
    assert out.collect().columns == ["x_a", "x_b"]

    out2 = pf.drop_regex("^x_")
    assert out2.schema().names() == ("id", "y")
    assert out2.collect().columns == ["id", "y"]


def test_pattern_ops_select_regex_no_matches_returns_empty_schema() -> None:
    data = {"id": [1], "x": [2]}

    class S(PolarsFrame):
        id: int
        x: int

    pf = S(data)
    out = pf.select_regex("^does_not_exist$")
    assert out.schema().names() == ()
    assert out.collect().columns == []


def test_concat_vertical() -> None:
    pf1 = User({"id": [1], "name": ["a"], "age": [10]})
    pf2 = User({"id": [2], "name": ["b"], "age": [20]})

    out = pf1.concat_vertical(pf2).sort("id")
    collected = out.collect()
    assert collected["id"].to_list() == [1, 2]


def test_concat_vertical_preserves_order_without_sort() -> None:
    pf1 = User({"id": [2], "name": ["b"], "age": [20]})
    pf2 = User({"id": [1], "name": ["a"], "age": [10]})
    collected = pf1.concat_vertical(pf2).collect()
    assert collected["id"].to_list() == [2, 1]


def test_pivot_with_lazyframe_requires_on_columns_and_is_deterministic() -> None:
    data = {"id": [1, 1], "k": ["a", "b"], "v": [10, 20]}

    class S(PolarsFrame):
        id: int
        k: str
        v: int

    pf = S(data)
    out = pf.pivot(index=("id",), on="k", values="v", on_columns=("a", "b"), agg="first")
    collected = out.collect()
    assert collected.columns == ["id", "a", "b"]
    assert collected["a"].to_list() == [10]
    assert collected["b"].to_list() == [20]


def test_pivot_handles_missing_on_columns_as_nulls() -> None:
    data = {"id": [1], "k": ["a"], "v": [10]}

    class S(PolarsFrame):
        id: int
        k: str
        v: int

    pf = S(data)
    collected = pf.pivot(index=("id",), on="k", values="v", on_columns=("a", "b")).collect()
    assert collected.columns == ["id", "a", "b"]
    assert collected["a"].to_list() == [10]
    assert collected["b"].to_list() == [None]


def test_pivot_lazy_without_on_columns_raises_execution_error() -> None:
    data = {"id": [1, 1], "k": ["a", "b"], "v": [10, 20]}

    class S(PolarsFrame):
        id: int
        k: str
        v: int

    pf = S(data)
    from planframe.backend.errors import PlanFrameBackendError

    with pytest.raises(PlanFrameBackendError, match="requires on_columns"):
        pf.pivot(index=("id",), on="k", values="v", on_columns=None)


def test_io_write_parquet_and_scan_parquet(tmp_path: Any) -> None:
    path = tmp_path / "out.parquet"
    pf = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})

    pf.select("id", "age").write_parquet(str(path))
    out = PolarsFrame.scan_parquet(str(path), schema=UserSchema).select("id", "age").collect()
    assert out["id"].to_list() == [1, 2]


def test_io_write_csv_and_scan_csv(tmp_path: Any) -> None:
    path = tmp_path / "out.csv"

    class S(PolarsFrame):
        id: int
        age: int

    pf = S({"id": [1, 2], "age": [10, 20]})
    pf.write_csv(str(path))

    out = PolarsFrame.scan_csv(str(path), schema=S).sort("id").collect()
    assert out["age"].to_list() == [10, 20]


def test_io_write_ndjson_and_scan_ndjson(tmp_path: Any) -> None:
    path = tmp_path / "out.ndjson"

    class S(PolarsFrame):
        id: int
        age: int

    pf = S({"id": [1, 2], "age": [10, 20]})
    pf.write_ndjson(str(path))

    out = PolarsFrame.scan_ndjson(str(path), schema=S).sort("id").collect()
    assert out["age"].to_list() == [10, 20]


def test_io_write_ipc_and_scan_ipc(tmp_path: Any) -> None:
    path = tmp_path / "out.ipc"

    class S(PolarsFrame):
        id: int
        age: int

    pf = S({"id": [1, 2], "age": [10, 20]})
    pf.write_ipc(str(path))

    out = PolarsFrame.scan_ipc(str(path), schema=S).sort("id").collect()
    assert out["age"].to_list() == [10, 20]


def test_io_read_database_sqlite_dbapi(tmp_path: Any) -> None:
    import importlib.util
    import sqlite3

    # Polars DB reading may rely on optional engines; skip if unavailable.
    if importlib.util.find_spec("connectorx") is None:
        import pytest

        pytest.skip("connectorx not available; skipping database IO test")

    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INTEGER, age INTEGER)")
    cur.execute("INSERT INTO t VALUES (1, 10)")
    cur.execute("INSERT INTO t VALUES (2, 20)")
    conn.commit()

    class S(PolarsFrame):
        id: int
        age: int

    out = PolarsFrame.read_database(
        "SELECT id, age FROM t ORDER BY id", connection=conn, schema=S
    ).collect()
    assert out["age"].to_list() == [10, 20]


def test_io_parquet_dataset_partitioned_write_and_scan(tmp_path: Any) -> None:
    base = tmp_path / "ds"
    data = {"id": [1, 2, 3], "part": ["a", "a", "b"], "age": [10, 20, 30]}

    class S(PolarsFrame):
        id: int
        part: str
        age: int

    pf = S(data)
    pf.write_parquet(str(base), partition_by=("part",), compression="zstd")

    out = (
        PolarsFrame.scan_parquet_dataset(str(base / "**" / "*.parquet"), schema=S)
        .sort("id")
        .collect()
    )
    assert out["age"].to_list() == [10, 20, 30]


def test_io_excel_roundtrip_if_available(tmp_path: Any) -> None:
    import importlib.util

    if importlib.util.find_spec("xlsxwriter") is None:
        pytest.skip("xlsxwriter not available; skipping excel write test")
    if importlib.util.find_spec("fastexcel") is None:
        pytest.skip("fastexcel not available; skipping excel read test")

    path = tmp_path / "out.xlsx"
    data = {"id": [1, 2], "age": [10, 20]}

    class S(PolarsFrame):
        id: int
        age: int

    S(data, lazy=False).write_excel(str(path), worksheet="Sheet1")
    out = PolarsFrame.read_excel(str(path), schema=S, sheet_name="Sheet1").sort("id").collect()
    assert out["age"].to_list() == [10, 20]


def test_io_avro_roundtrip_if_available(tmp_path: Any) -> None:
    path = tmp_path / "out.avro"
    data = {"id": [1, 2], "age": [10, 20]}

    class S(PolarsFrame):
        id: int
        age: int

    S(data, lazy=False).write_avro(str(path), compression="uncompressed")
    out = PolarsFrame.read_avro(str(path), schema=S).sort("id").collect()
    assert out["age"].to_list() == [10, 20]


def test_io_delta_roundtrip_if_available(tmp_path: Any) -> None:
    import importlib.util

    if importlib.util.find_spec("deltalake") is None:
        pytest.skip("deltalake not available; skipping delta IO test")

    path = tmp_path / "delta_tbl"
    data = {"id": [1, 2], "age": [10, 20]}

    class S(PolarsFrame):
        id: int
        age: int

    S(data, lazy=False).write_delta(str(path), mode="overwrite")
    out = PolarsFrame.scan_delta(str(path), schema=S).sort("id").collect()
    assert out["age"].to_list() == [10, 20]


def test_concat_horizontal_union_distinct_explode_unnest_drop_nulls_all() -> None:
    data = {
        "id": [1, 1, 2],
        "x": [1, 1, 2],
        "lst": [[1, 2], [3], []],
        "s": [{"a": 1, "b": 2}, {"a": 3, "b": None}, {"a": None, "b": None}],
    }

    class S(PolarsFrame):
        id: int
        x: int
        lst: object
        s: _SStructAB

    pf = S(data)
    left = pf.select("id", "x")
    right = pf.select("id").rename(id="id2")
    out = left.concat_horizontal(right)
    assert out.schema().names() == ("id", "x", "id2")

    u = left.union_distinct(left).sort("id").collect()
    assert u.height == 2

    exploded = pf.explode("lst").select("id", "lst").collect()
    assert exploded.height >= 3

    unnested = pf.unnest("s").select("id", "a", "b").collect()
    assert set(unnested.columns) == {"id", "a", "b"}

    dropped = pf.unnest("s").drop_nulls_all("a", "b").collect()
    assert "a" in dropped.columns


def test_concat_horizontal_overlap_raises() -> None:
    data = {"id": [1], "x": [1]}

    class S(PolarsFrame):
        id: int
        x: int

    pf = S(data)
    import pytest

    from planframe.backend.errors import PlanFrameSchemaError

    with pytest.raises(PlanFrameSchemaError):
        pf.select("id").concat_horizontal(pf.select("id"))


def test_unnest_duplicate_field_raises() -> None:
    data = {"id": [1], "s": [{"a": 1}]}

    class S(PolarsFrame):
        id: int
        s: _SStructId

    pf = S(data)
    import pytest

    from planframe.backend.errors import PlanFrameSchemaError

    with pytest.raises(PlanFrameSchemaError):
        pf.unnest("s")


def test_to_dicts_and_to_dict_execute() -> None:
    data = {"id": [2, 1], "x": [10, 20]}

    class S(PolarsFrame):
        id: int
        x: int

    pf = S(data).sort("id")

    dicts = pf.to_dicts()
    assert dicts == [{"id": 1, "x": 20}, {"id": 2, "x": 10}]

    d = pf.to_dict()
    assert d == {"id": [1, 2], "x": [20, 10]}


def test_sort_nulls_last_and_unique_maintain_order() -> None:
    data = {"id": [2, None, 1, None], "x": [10, 99, 20, 42]}

    class S(PolarsFrame):
        id: int | None
        x: int

    pf = S(data)
    sorted_df = pf.sort("id", nulls_last=True).collect()
    assert sorted_df["id"].to_list() == [1, 2, None, None]

    data2 = {"id": [2, 1, 2, 1], "x": [10, 20, 11, 21]}

    class S2(PolarsFrame):
        id: int
        x: int

    pf2 = S2(data2)
    out = pf2.unique("id", keep="first", maintain_order=True).collect()
    assert out["id"].to_list() == [2, 1]


def test_sample_n_is_deterministic_with_seed() -> None:
    data = {"id": list(range(10))}

    class S(PolarsFrame):
        id: int

    pf = S(data, lazy=False)
    a = pf.sample(3, seed=123, shuffle=True).sort("id").collect()["id"].to_list()
    b = pf.sample(3, seed=123, shuffle=True).sort("id").collect()["id"].to_list()
    assert a == b


def test_sample_frac_zero_and_one() -> None:
    data = {"id": list(range(10))}

    class S(PolarsFrame):
        id: int

    pf = S(data, lazy=False)
    assert pf.sample(frac=0.0, seed=1, shuffle=True).collect().height == 0
    assert pf.sample(frac=1.0, seed=1, shuffle=True).collect().height == 10


def test_sample_on_lazy_source_raises_clear_error() -> None:
    from planframe.backend.errors import PlanFrameExecutionError

    data = {"id": list(range(10))}

    class S(PolarsFrame):
        id: int

    pf = S(data, lazy=True)
    with pytest.raises(PlanFrameExecutionError, match="Backend collect failed"):
        pf.sample(3, seed=1, shuffle=True).collect()


def test_drop_duplicates_alias_and_keep_last() -> None:
    data = {"id": [1, 2, 1, 2], "x": [10, 20, 11, 21]}

    class S(PolarsFrame):
        id: int
        x: int

    pf = S(data)
    out = pf.drop_duplicates("id", keep="last", maintain_order=True).collect()
    assert out["id"].to_list() == [1, 2]
    assert out["x"].to_list() == [11, 21]


def test_construction_via_generic_call_dict_of_lists() -> None:
    class S(PolarsFrame):
        id: int
        name: str
        age: int

    pf = S({"id": [1], "name": ["a"], "age": [10]})
    df = pf.select("id", "name").collect()
    assert df.to_dict(as_series=False) == {"id": [1], "name": ["a"]}


def test_construction_via_model_subclass_list_of_dicts() -> None:
    class User2(PolarsFrame):
        id: int
        name: str
        age: int

    pf = User2([{"id": 1, "name": "a", "age": 10}])
    df = pf.select("age").collect()
    assert df.to_dict(as_series=False) == {"age": [10]}
