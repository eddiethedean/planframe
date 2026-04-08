from __future__ import annotations

import time

from planframe.schema.ir import Field, Schema


def main() -> None:
    schema = Schema(fields=tuple(Field(name=f"c{i}", dtype=int) for i in range(10_000)))

    t0 = time.perf_counter()
    for _ in range(200_000):
        schema.get("c9999")
    t1 = time.perf_counter()

    # Force a couple field_map() calls (should be cached).
    t2 = time.perf_counter()
    for _ in range(200_000):
        _ = schema.field_map()["c9999"]
    t3 = time.perf_counter()

    print(f"get() total:      {t1 - t0:.3f}s")
    print(f"field_map() total:{t3 - t2:.3f}s")


if __name__ == "__main__":
    main()
