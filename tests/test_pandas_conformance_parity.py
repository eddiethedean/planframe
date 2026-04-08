from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd
import pytest

from planframe.backend.errors import PlanFrameExecutionError, PlanFrameSchemaError
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
    is_finite,
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
    split,
    sqrt,
    starts_with,
    strip,
    xor,
    year,
)
from planframe_pandas import PandasFrame

pytestmark = pytest.mark.conformance


@dataclass(frozen=True)
class UserSchema:
    id: int
    name: str
    age: int


class User(PandasFrame):
    id: int
    name: str
    age: int


def test_drop_strict_false_ignores_unknown_pandas_columns() -> None:
    pf = User({"id": [1], "name": ["a"], "age": [10]})
    out = pf.select("id", "name", "age").drop("not_a_column", strict=False)
    df = out.collect()
    assert df.columns.tolist() == ["id", "name", "age"]


def test_rename_strict_false_ignores_unknown_pandas_columns() -> None:
    pf = User({"id": [1], "name": ["a"], "age": [10]})
    out = pf.select("id", "name", "age").rename(name="full_name", not_a_column="x", strict=False)
    df = out.collect()
    assert df.columns.tolist() == ["id", "full_name", "age"]


def test_select_mixed_str_and_expr_pandas() -> None:
    pf = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
    out = pf.select("id", ("twice_age", mul(col("age"), lit(2))))
    df = out.collect()
    assert df.columns.tolist() == ["id", "twice_age"]
    assert df["id"].to_list() == [1, 2]
    assert df["twice_age"].to_list() == [20, 40]


def test_sort_expression_key_pandas() -> None:
    pf = User({"id": [1, 2, 3], "name": ["a", "b", "c"], "age": [30, 10, 20]})
    out = pf.sort(add(col("id"), col("age")))
    df = out.collect()
    assert df["id"].to_list() == [2, 3, 1]


def test_sort_mixed_column_and_expr_pandas() -> None:
    pf = User({"id": [1, 2], "name": ["b", "a"], "age": [10, 20]})
    out = pf.sort("name", add(col("id"), col("age")))
    df = out.collect()
    assert df["id"].to_list() == [2, 1]


def test_select_drop_rename_with_column_filter_collect_pandas() -> None:
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
    assert isinstance(collected, pd.DataFrame)
    assert collected.columns.tolist() == ["id", "years", "years_plus_one"]
    assert len(collected) == 1

    rows = out.collect(kind="dataclass", name="OutRow")
    assert len(rows) == 1
    assert rows[0].id == 1
    assert rows[0].years == 10


def test_optimize_preserves_results_pandas() -> None:
    pf = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
    out = pf.select("id", "name", "age").select("id", "age").rename(age="years").drop()

    unopt = out.collect()
    opt = out.optimize(level=1).collect()

    assert unopt.columns.tolist() == opt.columns.tolist()
    assert unopt.to_dict(orient="records") == opt.to_dict(orient="records")


def test_constructor_fills_missing_columns_from_schema_defaults() -> None:
    class UserWithDefaults(PandasFrame):
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
    assert collected.columns.tolist() == ["age", "x_id"]


def test_extended_expressions_compile_and_filter() -> None:
    pf = User({"id": [1, 2, 3], "name": ["a", "b", "c"], "age": [10, 20, None]})
    out = (
        pf.select("id", "age")
        .filter(and_(gt(col("age"), lit(10)), is_not_null(col("age"))))
        .with_column("age_times_two", mul(col("age"), lit(2)))
    )
    collected = out.collect()
    assert collected.columns.tolist() == ["id", "age", "age_times_two"]
    assert collected["id"].to_list() == [2]


def test_more_expressions_abs_round_floor_ceil_coalesce_if_else_xor() -> None:
    data = {"id": [1, 2], "x": [-1.2, 3.4], "a": [None, 5], "b": [10, None]}

    class S(PandasFrame):
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
    assert collected.columns.tolist() == [
        "id",
        "x",
        "a",
        "b",
        "ax",
        "rx",
        "fx",
        "cx",
        "c",
        "flag",
        "picked",
    ]


def test_string_datetime_math_window_expressions() -> None:
    data = {
        "id": [1, 1, 2],
        "s": ["Hello", "world", "HELLO"],
        "x": [1.0, 2.0, 3.0],
        "dt": [datetime(2026, 1, 2), datetime(2026, 1, 3), datetime(2025, 12, 31)],
    }

    class S(PandasFrame):
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
        .with_column("x_over", over(col("x"), partition_by=("id",)))
    )
    df = out.collect()
    assert "x_over" in df.columns


def test_string_ops_nulls_and_literal_replace() -> None:
    data = {"s": ["a.a", None, "bbb"]}

    class S(PandasFrame):
        s: str | None

    pf = S(data)
    out = (
        pf.with_column("c1", contains(col("s"), ".", literal=True))
        .with_column("c2", contains(col("s"), ".", literal=False))
        .with_column("r", replace(col("s"), ".", "_", literal=True))
        .with_column("ln", length(col("s")))
    )
    df = out.collect()
    assert df.columns.tolist() == ["s", "c1", "c2", "r", "ln"]


def test_strip_split_sqrt_is_finite_exprs() -> None:
    data = {"s": ["  a,b  "], "x": [4.0], "y": [float("inf")]}

    class S(PandasFrame):
        s: str
        x: float
        y: float

    pf = S(data)
    df = (
        pf.with_column("s2", strip(col("s")))
        .with_column("parts", split(strip(col("s")), ","))
        .with_column("r", sqrt(col("x")))
        .with_column("ok", is_finite(col("y")))
        .collect()
    )
    assert df.columns.tolist() == ["s", "x", "y", "s2", "parts", "r", "ok"]


def test_window_over_partition_by_multiple_keys_is_passthrough() -> None:
    data = {"g1": [1, 1, 1, 2], "g2": ["a", "a", "b", "a"], "x": [1, 2, 3, 4]}

    class S(PandasFrame):
        g1: int
        g2: str
        x: int

    pf = S(data)
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
    assert dups.columns.tolist() == ["duplicated"]
    assert dups["duplicated"].dtype == bool


def test_group_by_expression_key_pandas() -> None:
    pf = User({"name": ["A", "a", "B"], "age": [1, 2, 10], "id": [1, 2, 3]})
    out = (
        pf.group_by(lower(col("name")))
        .agg(n=("count", "age"), total=("sum", "age"))
        .sort("__pf_g0")
    )
    collected = out.collect()
    assert collected.columns.tolist() == ["__pf_g0", "n", "total"]
    assert collected["__pf_g0"].to_list() == ["a", "b"]
    assert collected["n"].to_list() == [2, 1]
    assert collected["total"].to_list() == [3, 10]


def test_join_left_on_right_on() -> None:
    class LF(PandasFrame):
        user_id: int
        x: int

    class RF(PandasFrame):
        id: int
        y: int

    left_pf = LF({"user_id": [1, 2], "x": [10, 20]})
    right_pf = RF({"id": [1, 3], "y": [100, 300]})
    out = left_pf.join(right_pf, left_on=("user_id",), right_on=("id",), how="inner").collect()
    assert out.to_dict(orient="list") == {"user_id": [1], "x": [10], "y": [100]}


def test_join_expression_keys() -> None:
    class LF(PandasFrame):
        id: int
        email: str

    class RF(PandasFrame):
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
    assert len(out) == 2
    assert set(out["id"].to_list()) == {1, 2}


def test_row_ops_head_tail_slice_limit() -> None:
    pf = User({"id": [1, 2, 3, 4], "name": ["a", "b", "c", "d"], "age": [10, 20, 30, 40]})
    out = pf.head(3).slice(1, 2).tail(1).limit(1)
    collected = out.collect()
    assert collected["id"].to_list() == [3]


def test_row_ops_slice_length_none_and_offset_past_end() -> None:
    class S(PandasFrame):
        id: int

    pf = S({"id": [1, 2, 3]})
    assert pf.slice(1, None).collect()["id"].to_list() == [2, 3]
    assert len(pf.slice(999, None).collect()) == 0


def test_row_ops_head_tail_zero() -> None:
    class S(PandasFrame):
        id: int

    pf = S({"id": [1, 2, 3]})
    assert len(pf.head(0).collect()) == 0
    assert len(pf.tail(0).collect()) == 0


def test_pattern_select_and_drop() -> None:
    data = {"id": [1], "x_a": [10], "x_b": [20], "y": [30]}

    class S(PandasFrame):
        id: int
        x_a: int
        x_b: int
        y: int

    pf = S(data)
    out = pf.select_prefix("x_")
    assert out.schema().names() == ("x_a", "x_b")
    assert out.collect().columns.tolist() == ["x_a", "x_b"]

    out2 = pf.drop_regex("^x_")
    assert out2.schema().names() == ("id", "y")
    assert out2.collect().columns.tolist() == ["id", "y"]


def test_pattern_ops_select_regex_no_matches_returns_empty_schema() -> None:
    class S(PandasFrame):
        id: int
        x: int

    pf = S({"id": [1], "x": [2]})
    out = pf.select_regex("^does_not_exist$")
    assert out.schema().names() == ()
    assert out.collect().columns.tolist() == []


def test_concat_vertical_and_order_preservation() -> None:
    pf1 = User({"id": [2], "name": ["b"], "age": [20]})
    pf2 = User({"id": [1], "name": ["a"], "age": [10]})
    collected = pf1.concat_vertical(pf2).collect()
    assert collected["id"].to_list() == [2, 1]


def test_concat_horizontal_overlap_raises() -> None:
    class S(PandasFrame):
        id: int
        x: int

    pf = S({"id": [1], "x": [1]})
    with pytest.raises(PlanFrameSchemaError):
        pf.select("id").concat_horizontal(pf.select("id"))


def test_to_dicts_and_to_dict_execute() -> None:
    class S(PandasFrame):
        id: int
        x: int

    pf = S({"id": [2, 1], "x": [10, 20]}).sort("id")
    assert pf.to_dicts() == [{"id": 1, "x": 20}, {"id": 2, "x": 10}]
    assert pf.to_dict() == {"id": [1, 2], "x": [20, 10]}


def test_sort_nulls_last_and_unique_maintain_order() -> None:
    class S(PandasFrame):
        id: int | None
        x: int

    pf = S({"id": [2, None, 1, None], "x": [10, 99, 20, 42]})
    sorted_df = pf.sort("id", nulls_last=True).collect()
    # pandas uses NaN for missing numeric values
    assert sorted_df["id"].iloc[0] == 1
    assert sorted_df["id"].iloc[1] == 2
    assert pd.isna(sorted_df["id"].iloc[2])
    assert pd.isna(sorted_df["id"].iloc[3])

    class S2(PandasFrame):
        id: int
        x: int

    pf2 = S2({"id": [2, 1, 2, 1], "x": [10, 20, 11, 21]})
    out = pf2.unique("id", keep="first", maintain_order=True).collect()
    assert out["id"].to_list() == [2, 1]


def test_sample_n_is_deterministic_with_seed() -> None:
    class S(PandasFrame):
        id: int

    pf = S({"id": list(range(10))})
    a = pf.sample(3, seed=123, shuffle=True).sort("id").collect()["id"].to_list()
    b = pf.sample(3, seed=123, shuffle=True).sort("id").collect()["id"].to_list()
    assert a == b


def test_sample_frac_zero_and_one() -> None:
    class S(PandasFrame):
        id: int

    pf = S({"id": list(range(10))})
    assert len(pf.sample(frac=0.0, seed=1, shuffle=True).collect()) == 0
    assert len(pf.sample(frac=1.0, seed=1, shuffle=True).collect()) == 10


def test_io_write_csv_roundtrip_via_pandas_reader(tmp_path: Any) -> None:
    path = tmp_path / "out.csv"

    class S(PandasFrame):
        id: int
        age: int

    pf = S({"id": [1, 2], "age": [10, 20]})
    pf.write_csv(str(path))
    df = pd.read_csv(path).sort_values("id")
    assert df["age"].to_list() == [10, 20]


def test_io_write_ndjson_roundtrip_via_pandas_reader(tmp_path: Any) -> None:
    path = tmp_path / "out.ndjson"

    class S(PandasFrame):
        id: int
        age: int

    pf = S({"id": [1, 2], "age": [10, 20]})
    pf.write_ndjson(str(path))
    df = pd.read_json(path, lines=True).sort_values("id")
    assert df["age"].to_list() == [10, 20]


def test_io_write_database_sqlite_dbapi(tmp_path: Any) -> None:
    import sqlite3

    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(str(db_path))

    class S(PandasFrame):
        id: int
        age: int

    pf = S({"id": [1, 2], "age": [10, 20]})
    pf.write_database(table_name="t", connection=conn, if_table_exists="replace")
    out = pd.read_sql_query("SELECT id, age FROM t ORDER BY id", conn)
    assert out["age"].to_list() == [10, 20]


def test_write_ipc_not_implemented_raises_execution_error(tmp_path: Any) -> None:
    path = tmp_path / "out.ipc"
    pf = User({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(PlanFrameExecutionError):
        pf.write_ipc(str(path))


def test_pandas_scan_csv_and_ndjson_convenience(tmp_path: Any) -> None:
    csv_path = tmp_path / "x.csv"
    ndjson_path = tmp_path / "x.ndjson"

    class S(PandasFrame):
        id: int
        age: int

    S({"id": [1, 2], "age": [10, 20]}).write_csv(str(csv_path))
    S({"id": [1, 2], "age": [10, 20]}).write_ndjson(str(ndjson_path))

    out1 = PandasFrame.scan_csv(str(csv_path), schema=S).sort("id").collect()
    assert out1["age"].to_list() == [10, 20]

    out2 = PandasFrame.scan_ndjson(str(ndjson_path), schema=S).sort("id").collect()
    assert out2["age"].to_list() == [10, 20]
