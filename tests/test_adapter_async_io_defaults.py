from __future__ import annotations

import asyncio
from typing import Any

from planframe.backend.adapter import BaseAdapter


class _AsyncIODefaultsAdapter(BaseAdapter[list[dict[str, Any]], object]):
    name = "async-io-defaults"

    # ---- AdapterReader surface (minimal for tests) ----
    def scan_csv(self, path: str, *, storage_options: object | None = None) -> list[dict[str, Any]]:
        _ = path, storage_options
        return [{"x": 1}]

    # --- minimal required BaseAdapter impls (not used by these tests) ---
    def select(self, df: list[dict[str, Any]], columns: tuple[str, ...]) -> list[dict[str, Any]]:
        return df

    def project(self, df: list[dict[str, Any]], items: tuple[Any, ...]) -> list[dict[str, Any]]:
        return df

    def drop(
        self, df: list[dict[str, Any]], columns: tuple[str, ...], *, strict: bool = True
    ) -> list[dict[str, Any]]:
        return df

    def rename(
        self, df: list[dict[str, Any]], mapping: dict[str, str], *, strict: bool = True
    ) -> list[dict[str, Any]]:
        return df

    def with_column(
        self, df: list[dict[str, Any]], name: str, expr: object
    ) -> list[dict[str, Any]]:
        return df

    def cast(self, df: list[dict[str, Any]], name: str, dtype: object) -> list[dict[str, Any]]:
        return df

    def with_row_count(
        self, df: list[dict[str, Any]], *, name: str = "row_nr", offset: int = 0
    ) -> list[dict[str, Any]]:
        return df

    def filter(self, df: list[dict[str, Any]], predicate: object) -> list[dict[str, Any]]:
        return df

    def sort(
        self,
        df: list[dict[str, Any]],
        keys: tuple[Any, ...],
        *,
        descending: tuple[bool, ...],
        nulls_last: tuple[bool, ...],
    ) -> list[dict[str, Any]]:
        return df

    def unique(
        self,
        df: list[dict[str, Any]],
        subset: tuple[str, ...] | None,
        *,
        keep: str = "first",
        maintain_order: bool = False,
    ) -> list[dict[str, Any]]:
        return df

    def duplicated(
        self,
        df: list[dict[str, Any]],
        subset: tuple[str, ...] | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> list[dict[str, Any]]:
        return df

    def group_by_agg(
        self, df: list[dict[str, Any]], *, keys: tuple[Any, ...], named_aggs: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return df

    def group_by_dynamic_agg(self, df: list[dict[str, Any]], **_: Any) -> list[dict[str, Any]]:
        return df

    def rolling_agg(self, df: list[dict[str, Any]], **_: Any) -> list[dict[str, Any]]:
        return df

    def drop_nulls(
        self,
        df: list[dict[str, Any]],
        subset: tuple[str, ...] | None,
        *,
        how: str = "any",
        threshold: int | None = None,
    ) -> list[dict[str, Any]]:
        return df

    def fill_null(
        self,
        df: list[dict[str, Any]],
        value: Any,
        subset: tuple[str, ...] | None,
        *,
        strategy: str | None = None,
    ) -> list[dict[str, Any]]:
        return df

    def melt(self, df: list[dict[str, Any]], **_: Any) -> list[dict[str, Any]]:
        return df

    def join(
        self, left: list[dict[str, Any]], right: list[dict[str, Any]], **_: Any
    ) -> list[dict[str, Any]]:
        return left

    def slice(
        self, df: list[dict[str, Any]], *, offset: int, length: int | None
    ) -> list[dict[str, Any]]:
        return df

    def head(self, df: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
        return df

    def tail(self, df: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
        return df

    def concat_vertical(
        self, left: list[dict[str, Any]], right: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        return left + right

    def concat_horizontal(
        self, left: list[dict[str, Any]], right: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        return left

    def pivot(self, df: list[dict[str, Any]], **_: Any) -> list[dict[str, Any]]:
        return df

    def write_parquet(self, df: list[dict[str, Any]], path: str, **_: Any) -> None:
        _ = df, path

    def write_csv(self, df: list[dict[str, Any]], path: str, **_: Any) -> None:
        _ = df, path

    def write_ndjson(self, df: list[dict[str, Any]], path: str, **_: Any) -> None:
        _ = df, path

    def write_ipc(self, df: list[dict[str, Any]], path: str, **_: Any) -> None:
        _ = df, path

    def write_database(self, df: list[dict[str, Any]], **_: Any) -> None:
        _ = df

    def write_excel(self, df: list[dict[str, Any]], path: str, **_: Any) -> None:
        _ = df, path

    def write_delta(self, df: list[dict[str, Any]], target: str, **_: Any) -> None:
        _ = df, target

    def write_avro(self, df: list[dict[str, Any]], path: str, **_: Any) -> None:
        _ = df, path

    def explode(
        self, df: list[dict[str, Any]], columns: tuple[str, ...], *, outer: bool = False
    ) -> list[dict[str, Any]]:
        return df

    def unnest(self, df: list[dict[str, Any]], items: tuple[Any, ...]) -> list[dict[str, Any]]:
        return df

    def posexplode(self, df: list[dict[str, Any]], column: str, **_: Any) -> list[dict[str, Any]]:
        return df

    def drop_nulls_all(
        self, df: list[dict[str, Any]], subset: tuple[str, ...] | None
    ) -> list[dict[str, Any]]:
        return df

    def sample(self, df: list[dict[str, Any]], **_: Any) -> list[dict[str, Any]]:
        return df

    def compile_expr(self, expr: object, *, schema: object | None = None) -> object:
        _ = expr, schema
        return expr

    def collect(
        self, df: list[dict[str, Any]], *, options: object | None = None
    ) -> list[dict[str, Any]]:
        _ = options
        return df

    def to_dicts(
        self, df: list[dict[str, Any]], *, options: object | None = None
    ) -> list[dict[str, object]]:
        _ = options
        return df  # type: ignore[return-value]

    def to_dict(
        self, df: list[dict[str, Any]], *, options: object | None = None
    ) -> dict[str, list[object]]:
        _ = options
        return {}


def test_default_areader_wraps_reader_in_thread() -> None:
    adapter = _AsyncIODefaultsAdapter()

    async def run() -> int:
        df = await adapter.areader.scan_csv("dummy.csv")
        return len(df)

    assert asyncio.run(run()) == 1


def test_default_awriter_wraps_writer_in_thread() -> None:
    adapter = _AsyncIODefaultsAdapter()

    async def run() -> None:
        # Ensure awriter calls through to writer wrapper without raising.
        await adapter.awriter.sink_csv([{"x": 1}], "out.csv")

    asyncio.run(run())
