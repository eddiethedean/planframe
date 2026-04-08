"""Runtime detection of :class:`planframe.selector.ColumnSelector` (issue #72)."""

from __future__ import annotations

from planframe.schema.ir import Schema
from planframe.selector import (
    ColumnSelector,
    Union,
    by_name,
    dtype,
    prefix,
    regex,
    suffix,
)


def test_builtin_selectors_are_instance_of_protocol() -> None:
    assert isinstance(by_name("a"), ColumnSelector)
    assert isinstance(prefix("p"), ColumnSelector)
    assert isinstance(suffix("s"), ColumnSelector)
    assert isinstance(regex(".*"), ColumnSelector)
    assert isinstance(
        dtype(is_exact=int),
        ColumnSelector,
    )
    assert isinstance(Union(left=prefix("a"), right=suffix("b")), ColumnSelector)


def test_structural_match_with_select_method() -> None:
    """``@runtime_checkable`` allows isinstance for objects with a compatible ``select``."""

    class _Duck:
        def select(self, schema: Schema) -> tuple[str, ...]:
            return ()

    assert isinstance(_Duck(), ColumnSelector)


def test_non_selectors_are_not_instance() -> None:
    assert not isinstance(object(), ColumnSelector)
    assert not isinstance("not-a-selector", ColumnSelector)
