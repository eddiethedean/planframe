from __future__ import annotations

from typing import Any, ClassVar, Generic, TypeVar, cast

import pandas as pd

from planframe.frame import Frame
from planframe_pandas.adapter import PandasAdapter, PandasBackendExpr, PandasBackendFrame


def _to_pandas_df(data: object) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        raise TypeError(
            "PandasFrame constructors accept only Python data (dict-of-lists or list-of-dicts). "
            "Use `Frame.source(...)` for advanced usage."
        )
    if isinstance(data, list):
        return pd.DataFrame.from_records(cast(list[dict[str, object]], data))
    if isinstance(data, dict):
        return pd.DataFrame(cast(dict[str, list[object]], data))
    raise TypeError("PandasFrame expects dict-of-lists or list-of-dicts")


class _PandasFrameMeta(type):
    def __call__(cls, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        # Allow normal construction when `Frame.source(...)` calls `cls(_data=..., ...)`.
        if "_data" in kwargs and "_adapter" in kwargs and "_plan" in kwargs and "_schema" in kwargs:
            return super().__call__(*args, **kwargs)

        data = args[0] if args else kwargs.pop("data")
        if kwargs:
            raise TypeError(f"Unexpected constructor kwargs: {sorted(kwargs)}")
        df = _to_pandas_df(data)
        return PandasFrame.source(
            df,
            adapter=PandasFrame._adapter_singleton,
            schema=cast(type[Any], cls),
        )


SchemaT = TypeVar("SchemaT")


class PandasFrame(
    Frame[SchemaT, PandasBackendFrame, PandasBackendExpr],
    Generic[SchemaT],
    metaclass=_PandasFrameMeta,
):
    """A PlanFrame `Frame` bound to the pandas backend."""

    _adapter_singleton: ClassVar[PandasAdapter] = PandasAdapter()
    __planframe_model__ = True
