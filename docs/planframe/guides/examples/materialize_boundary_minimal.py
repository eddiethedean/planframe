"""Minimal `materialize_into` example with :class:`~planframe.execution_options.ExecutionOptions`.

Run from the repository root with the dev environment (``planframe`` + ``planframe-polars`` on ``PYTHONPATH``)::

    PYTHONPATH=packages/planframe python docs/planframe/guides/examples/materialize_boundary_minimal.py
"""

from __future__ import annotations

from planframe.execution_options import ExecutionOptions
from planframe.materialize import materialize_into
from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    age: int


def main() -> None:
    pf = User({"id": [1, 2], "age": [10, 20]}).select("id", "age")
    opts = ExecutionOptions(streaming=True)

    def summarize(cols: dict[str, list[object]]) -> tuple[int, ...]:
        return tuple(len(v) for v in cols.values())

    summary = materialize_into(pf, summarize, options=opts)
    print(f"column_lengths={summary}")
    assert summary == (2, 2), summary


if __name__ == "__main__":
    main()
