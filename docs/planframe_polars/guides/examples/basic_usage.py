from __future__ import annotations

from planframe.expr import add, col, lit
from planframe_polars import PolarsFrame


class User(PolarsFrame):
    id: int
    name: str
    age: int


def main() -> None:
    pf = User({"id": [1], "name": ["a"], "age": [10]})

    out = (
        pf.select("id", "name", "age")
        .rename(name="full_name")
        .with_column("age_plus_one", add(col("age"), lit(1)))
        .drop("age")
    )

    df = out.collect()
    print(f"columns={df.columns}")
    print(f"to_dict={out.to_dict()}")
    print(f"rows={out.to_dicts()}")

    models = out.collect(kind="dataclass", name="Row")
    print(
        f"row_models={[(m.__class__.__name__, m.id, m.full_name, m.age_plus_one) for m in models]}"
    )


if __name__ == "__main__":
    main()

