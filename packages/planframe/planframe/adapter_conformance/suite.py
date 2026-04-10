from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar, cast

from planframe.expr import col, eq, lit, mul
from planframe.frame import Frame

FrameT = TypeVar("FrameT", bound=Frame[Any, Any, Any])

Factory = Callable[[Mapping[str, Sequence[object]]], Frame[Any, Any, Any]]


def _as_factory(
    users: type[Frame[Any, Any, Any]] | Factory,
) -> Factory:
    if isinstance(users, type):
        # Frame subclasses (e.g. PolarsFrame) expose a public dict-of-columns constructor;
        # static analysis still sees the internal Frame.__init__ signature.
        ctor = cast(Any, users)
        return lambda data: ctor(data)
    return users


@dataclass(frozen=True, slots=True)
class ConformanceCase:
    """One named check in :func:`run_minimal_adapter_conformance`."""

    name: str
    ok: bool
    detail: str = ""


@dataclass(frozen=True, slots=True)
class ConformanceResult:
    """Outcome of :func:`run_minimal_adapter_conformance`."""

    cases: tuple[ConformanceCase, ...]

    @property
    def passed(self) -> bool:
        return all(c.ok for c in self.cases)

    @property
    def failed(self) -> tuple[ConformanceCase, ...]:
        return tuple(c for c in self.cases if not c.ok)


def _norm_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Stable sort for order-insensitive comparisons."""

    def key(r: dict[str, object]) -> tuple[tuple[str, object], ...]:
        return tuple(sorted(r.items(), key=lambda kv: kv[0]))

    return sorted(rows, key=key)


def _assert_rows_equal(
    got: list[dict[str, object]],
    expected: list[dict[str, object]],
    *,
    ordered: bool,
) -> None:
    if ordered:
        assert got == expected
    else:
        assert _norm_rows(got) == _norm_rows(expected)


def run_minimal_adapter_conformance(
    *,
    users: type[Frame[Any, Any, Any]] | Factory,
    join_left: type[Frame[Any, Any, Any]] | Factory | None = None,
    join_right: type[Frame[Any, Any, Any]] | Factory | None = None,
    raise_on_failure: bool = True,
) -> ConformanceResult:
    """Run a small, stable set of checks against a ``Frame`` wired to your adapter.

    This is intended for **third-party** ``BaseAdapter`` implementations: you pass
    factory callables (or concrete ``Frame`` subclasses with a dict-of-columns
    constructor, like ``planframe_polars.PolarsFrame``) and get a pass/fail signal
    with **actionable case names** (select/filter, projections, sort, group_by,
    optional join).

    Args:
        users: Builds a frame with columns ``id`` (int), ``name`` (str), ``age`` (int).
        join_left: Optional. Builds a frame with ``id``, ``name`` for the join case.
        join_right: Optional. Builds a frame with ``id``, ``city`` for the join case.
        raise_on_failure: If ``True`` (default), raise ``AssertionError`` aggregating
            failed case names and details when any check fails.

    Returns:
        :class:`ConformanceResult` listing each case and whether it passed.

    Raises:
        AssertionError: When ``raise_on_failure`` is true and one or more cases fail.
    """
    make_users = _as_factory(users)
    cases: list[ConformanceCase] = []

    def _run(name: str, fn: Callable[[], None]) -> None:
        try:
            fn()
        except Exception as e:  # noqa: BLE001 — surface as a failed case with traceback context
            cases.append(ConformanceCase(name=name, ok=False, detail=repr(e)))
        else:
            cases.append(ConformanceCase(name=name, ok=True))

    def case_select_filter() -> None:
        pf = make_users({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
        out = pf.select("id", "name").filter(eq(col("id"), lit(1)))
        rows = out.to_dicts()
        _assert_rows_equal(rows, [{"id": 1, "name": "a"}], ordered=True)

    def case_project_expr() -> None:
        pf = make_users({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
        out = pf.select("id", ("twice_age", mul(col("age"), lit(2))))
        rows = out.to_dicts()
        _assert_rows_equal(
            rows,
            [{"id": 1, "twice_age": 20}, {"id": 2, "twice_age": 40}],
            ordered=False,
        )

    def case_sort() -> None:
        pf = make_users({"id": [1, 2, 3], "name": ["a", "b", "c"], "age": [30, 10, 20]})
        out = pf.sort("age")
        rows = out.to_dicts()
        assert [r["id"] for r in rows] == [2, 3, 1]

    def case_group_by_agg() -> None:
        pf = make_users({"id": [1, 1, 2], "name": ["a", "b", "c"], "age": [10, 20, 30]})
        out = pf.group_by("id").agg(total=("sum", "age"))
        rows = out.to_dicts()
        _assert_rows_equal(
            rows,
            [{"id": 1, "total": 30}, {"id": 2, "total": 30}],
            ordered=False,
        )

    _run("select_filter", case_select_filter)
    _run("project_expr", case_project_expr)
    _run("sort", case_sort)
    _run("group_by_agg", case_group_by_agg)

    if join_left is not None and join_right is not None:
        make_l = _as_factory(join_left)
        make_r = _as_factory(join_right)

        def case_join_inner() -> None:
            left = make_l({"id": [1], "name": ["a"]})
            right = make_r({"id": [1], "city": ["NY"]})
            out = left.join(right, on=("id",), how="inner")
            rows = out.to_dicts()
            _assert_rows_equal(rows, [{"id": 1, "name": "a", "city": "NY"}], ordered=False)

        _run("join_inner", case_join_inner)
    else:
        cases.append(
            ConformanceCase(
                name="join_inner",
                ok=True,
                detail="skipped (join_left/join_right not provided)",
            )
        )

    result = ConformanceResult(cases=tuple(cases))
    if raise_on_failure and not result.passed:
        lines = [f"{c.name}: {c.detail}" for c in result.failed]
        msg = "Adapter conformance failed:\n" + "\n".join(lines)
        raise AssertionError(msg)
    return result
