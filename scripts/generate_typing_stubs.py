from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"


def _params(prefix: str, n: int) -> str:
    """Return ``name: LiteralString`` params like ``__c1: LiteralString, __c2: LiteralString``."""

    return ", ".join([f"{prefix}{i}: LiteralString" for i in range(1, n + 1)])


def _tuple_of(item: str, n: int) -> str:
    """Return tuple type content like ``LiteralString, LiteralString`` (without outer ``tuple[...]``)."""

    return ", ".join([item] * n)


def _jinja_env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        undefined=StrictUndefined,
        autoescape=False,
        keep_trailing_newline=True,
    )


def _ruff_format_pyi(path: Path, content: str) -> str:
    """Match `ruff format` so ``ruff format --check`` passes on generated stubs."""

    stdin_fn = f"--stdin-filename={path.relative_to(REPO_ROOT)}"
    ruff_exe = shutil.which("ruff")
    cmd = (
        [ruff_exe, "format", "-", stdin_fn]
        if ruff_exe
        else [sys.executable, "-m", "ruff", "format", "-", stdin_fn]
    )
    proc = subprocess.run(
        cmd,
        input=content,
        text=True,
        capture_output=True,
        cwd=REPO_ROOT,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr or "ruff format failed")
    return proc.stdout


def _write_if_changed(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding="utf-8") if path.exists() else None
    if existing == content:
        return
    path.write_text(content, encoding="utf-8")


def _differs(path: Path, content: str) -> bool:
    existing = path.read_text(encoding="utf-8") if path.exists() else None
    return existing != content


def _render_frame_pyi(*, max_arity: int = 10) -> str:
    env = _jinja_env()
    tmpl = env.get_template("frame.pyi.j2")
    return tmpl.render(max_arity=max_arity, params=_params, tuple_of=_tuple_of)


def _render_schema_types_pyi() -> str:
    env = _jinja_env()
    tmpl = env.get_template("schema_types.pyi.j2")
    return tmpl.render()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--max-arity", type=int, default=10)
    args = parser.parse_args(argv)

    frame_pyi_path = REPO_ROOT / "packages" / "planframe" / "planframe" / "frame" / "__init__.pyi"
    frame_pyi = _ruff_format_pyi(frame_pyi_path, _render_frame_pyi(max_arity=args.max_arity))
    schema_types_pyi_path = (
        REPO_ROOT / "packages" / "planframe" / "planframe" / "typing" / "_schema_types.pyi"
    )
    schema_types_pyi = _ruff_format_pyi(schema_types_pyi_path, _render_schema_types_pyi())

    if args.check:
        changed: list[str] = []
        if _differs(frame_pyi_path, frame_pyi):
            changed.append(str(frame_pyi_path.relative_to(REPO_ROOT)))
        if _differs(schema_types_pyi_path, schema_types_pyi):
            changed.append(str(schema_types_pyi_path.relative_to(REPO_ROOT)))
        if changed:
            print("Typing stubs are out of date. Re-run:")
            print("  python scripts/generate_typing_stubs.py")
            print("Changed:")
            for p in changed:
                print(f"  - {p}")
            return 1
        return 0

    _write_if_changed(frame_pyi_path, frame_pyi)
    _write_if_changed(schema_types_pyi_path, schema_types_pyi)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
