from __future__ import annotations

import ast
import inspect
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class MethodRow:
    name: str
    our_sig: str
    parent: str
    parent_sig: str
    status: str
    notes: str


@dataclass(frozen=True)
class MissingRow:
    name: str
    parent: str
    parent_sig: str
    notes: str


def _collapse_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _extract_pyi_methods(pyi_path: Path, class_name: str) -> dict[str, str]:
    """Best-effort parse of a .pyi class block to pull one signature per method name."""
    text = pyi_path.read_text(encoding="utf-8")
    lines = text.splitlines()

    # Find `class {class_name}(` line and indent.
    start = None
    for i, ln in enumerate(lines):
        if ln.startswith(f"class {class_name}"):
            start = i
            break
    if start is None:
        raise RuntimeError(f"Could not find class {class_name} in {pyi_path}")

    # Collect method defs inside the class (indent >= 4).
    out: dict[str, str] = {}
    for ln in lines[start + 1 :]:
        if ln and not ln.startswith(" "):
            break  # end of class
        if ln.lstrip().startswith("def "):
            sig = ln.strip().removesuffix(": ...")
            # name is between "def " and "("
            name = sig[4 : sig.find("(")]
            # Keep the first signature we see for that name (usually an overload).
            out.setdefault(name, _collapse_ws(sig))
    return out


def _extract_py_class_methods(py_path: Path, class_name: str) -> dict[str, str]:
    src = py_path.read_text(encoding="utf-8")
    mod = ast.parse(src)
    for node in mod.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            methods: dict[str, str] = {}
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    if item.name.startswith("_"):
                        continue
                    # reconstruct a light signature from args
                    args = []
                    for a in item.args.args:
                        args.append(a.arg)
                    if item.args.vararg is not None:
                        args.append("*" + item.args.vararg.arg)
                    for a in item.args.kwonlyargs:
                        args.append(a.arg)
                    if item.args.kwarg is not None:
                        args.append("**" + item.args.kwarg.arg)
                    methods[item.name] = f"def {item.name}({', '.join(args)})"
            return methods
    raise RuntimeError(f"Could not find class {class_name} in {py_path}")


def _parent_sig(obj: Any, name: str) -> str:
    try:
        member = getattr(obj, name)
    except Exception:
        return ""
    try:
        return str(inspect.signature(member))
    except Exception:
        return ""


def _build_polars_parent() -> tuple[str, Any]:
    try:
        import polars as pl

        return "polars.LazyFrame", pl.LazyFrame
    except Exception:
        return "polars.LazyFrame", None


def _build_pandas_parent() -> tuple[str, Any]:
    try:
        import pandas as pd

        return "pandas.DataFrame", pd.DataFrame
    except Exception:
        return "pandas.DataFrame", None


def _status_defaults(interface: str, method: str) -> tuple[str, str]:
    # Coarse defaults; the docs pages contain additional narrative/notes.
    if interface == "polars":
        if method in {
            "sink_parquet",
            "sink_csv",
            "sink_ndjson",
            "sink_ipc",
            "sink_database",
            "sink_excel",
            "sink_delta",
            "sink_avro",
        }:
            return "divergence", "PlanFrame uses sink_* naming for lazy IO boundaries"
        return "typed-parity", ""
    if interface == "pandas":
        if method in {"query"}:
            return "divergence", "typed predicate only; no string expression parser"
        if method in {"drop"}:
            return "divergence", "columns-only; no index semantics"
        if method in {"merge", "dropna", "fillna"}:
            return "divergence", "lowered to core ops; eager/index semantics differ"
        return "typed-parity", ""
    if interface == "spark":
        if method in {
            "cache",
            "persist",
            "unpersist",
            "repartition",
            "coalesce",
            "selectExpr",
            "sortWithinPartitions",
        }:
            return "unsupported", "Spark engine/partition semantics not part of PlanFrame core"
        if method in {"hint", "unionByName"}:
            return "divergence", "Plan-level hint / restricted unionByName shape"
        return "typed-parity", ""
    return "typed-parity", ""


def _render_table(title: str, rows: list[MethodRow]) -> str:
    lines: list[str] = []
    lines.append(f"## {title}")
    lines.append("")
    lines.append("| Method | Our signature | Parent | Parent signature | Status | Notes |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for r in sorted(rows, key=lambda x: x.name):
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{r.name}`",
                    f"`{r.our_sig}`",
                    f"`{r.parent}`",
                    f"`{r.parent_sig}`" if r.parent_sig else "—",
                    f"**{r.status}**",
                    (r.notes or "—").replace("\n", " "),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def _render_missing(title: str, rows: list[MissingRow]) -> str:
    lines: list[str] = []
    lines.append(f"## {title}")
    lines.append("")
    lines.append("| Polars method | Polars signature | Notes |")
    lines.append("| --- | --- | --- |")
    for r in sorted(rows, key=lambda x: x.name):
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{r.name}`",
                    f"`{r.parent_sig}`" if r.parent_sig else "—",
                    (r.notes or "—").replace("\n", " "),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def _public_callable_names(obj: Any) -> set[str]:
    if obj is None:
        return set()
    out: set[str] = set()
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            member = getattr(obj, name)
        except Exception:
            continue
        if callable(member):
            out.add(name)
    return out


def main() -> int:
    out_dir = ROOT / "docs" / "planframe" / "design" / "_generated"
    out_dir.mkdir(parents=True, exist_ok=True)

    frame_pyi = ROOT / "packages" / "planframe" / "planframe" / "frame" / "__init__.pyi"
    core_methods = _extract_pyi_methods(frame_pyi, "Frame")

    pandas_skin = ROOT / "packages" / "planframe" / "planframe" / "pandas" / "frame.py"
    pandas_methods = _extract_py_class_methods(pandas_skin, "PandasLikeFrame")

    spark_skin = ROOT / "packages" / "planframe" / "planframe" / "spark" / "frame.py"
    spark_methods = _extract_py_class_methods(spark_skin, "SparkFrame")

    polars_parent_name, polars_parent = _build_polars_parent()
    pandas_parent_name, pandas_parent = _build_pandas_parent()

    # Polars parity inventory: compare core Frame to LazyFrame names (best-effort).
    polars_rows: list[MethodRow] = []
    for name, sig in core_methods.items():
        status, notes = _status_defaults("polars", name)
        polars_rows.append(
            MethodRow(
                name=name,
                our_sig=sig,
                parent=polars_parent_name,
                parent_sig=_parent_sig(polars_parent, name) if polars_parent is not None else "",
                status=status,
                notes=notes,
            )
        )

    # Polars-only methods: methods present on LazyFrame but missing from our core Frame surface.
    polars_missing: list[MissingRow] = []
    if polars_parent is not None:
        polars_names = _public_callable_names(polars_parent)
        our_names = set(core_methods)
        for name in sorted(polars_names - our_names):
            polars_missing.append(
                MissingRow(
                    name=name,
                    parent=polars_parent_name,
                    parent_sig=_parent_sig(polars_parent, name),
                    notes="Not implemented in PlanFrame core `Frame` (yet).",
                )
            )

    pandas_rows: list[MethodRow] = []
    for name, sig in pandas_methods.items():
        status, notes = _status_defaults("pandas", name)
        pandas_rows.append(
            MethodRow(
                name=name,
                our_sig=sig,
                parent=pandas_parent_name,
                parent_sig=_parent_sig(pandas_parent, name) if pandas_parent is not None else "",
                status=status,
                notes=notes,
            )
        )

    spark_rows: list[MethodRow] = []
    for name, sig in spark_methods.items():
        status, notes = _status_defaults("spark", name)
        spark_rows.append(
            MethodRow(
                name=name,
                our_sig=sig,
                parent="pyspark.sql.DataFrame",
                parent_sig="",  # not introspected (Spark not required for docs generation)
                status=status,
                notes=notes,
            )
        )

    (out_dir / "interface-inventory-polars.md").write_text(
        "# Generated interface inventory (Polars parity)\n\n"
        "This file is generated from stubs/source. It inventories PlanFrame methods and attempts to\n"
        "show parent-interface signatures when available in the local environment.\n\n"
        + _render_table("Core `Frame` vs `polars.LazyFrame`", polars_rows),
        encoding="utf-8",
    )

    (out_dir / "polars-missing.md").write_text(
        "# Generated: Polars methods missing in PlanFrame core\n\n"
        "This file is generated by comparing the public callable surface of `polars.LazyFrame` against\n"
        "PlanFrame core `Frame` methods from stubs.\n\n"
        + (
            _render_missing(
                "`polars.LazyFrame` methods not present on PlanFrame `Frame`",
                polars_missing,
            )
            if polars_missing
            else "Polars is not installed (or could not be imported), so this report is empty.\n"
        ),
        encoding="utf-8",
    )

    (out_dir / "interface-inventory-pandas.md").write_text(
        "# Generated interface inventory (pandas parity)\n\n"
        "This file is generated from source. It inventories the pandas-like skin surface and attempts to\n"
        "show parent-interface signatures when available in the local environment.\n\n"
        + _render_table("`PandasLikeFrame` vs `pandas.DataFrame`", pandas_rows),
        encoding="utf-8",
    )

    (out_dir / "interface-inventory-spark.md").write_text(
        "# Generated interface inventory (Spark parity)\n\n"
        "This file is generated from source. It inventories the spark-like skin surface.\n\n"
        + _render_table("`SparkFrame` vs `pyspark.sql.DataFrame`", spark_rows),
        encoding="utf-8",
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
