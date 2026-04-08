from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.typing

ROOT = Path(__file__).resolve().parents[1]
PYRIGHT_DIR = ROOT / "tests" / "pyright"


def _run_pyright(path: Path) -> subprocess.CompletedProcess[str]:
    cfg = PYRIGHT_DIR / "pyrightconfig.json"
    return subprocess.run(
        [str(ROOT / ".venv" / "bin" / "pyright"), "--project", str(cfg), str(path)],
        text=True,
        capture_output=True,
    )


def _fmt_res(res: subprocess.CompletedProcess[str]) -> str:
    return f"returncode={res.returncode}\nstdout:\n{res.stdout}\nstderr:\n{res.stderr}"


PASS_FILES = sorted((PYRIGHT_DIR / "pass").glob("*.py"))
FAIL_FILES = sorted((PYRIGHT_DIR / "fail").glob("*.py"))


@pytest.mark.parametrize("path", PASS_FILES, ids=lambda p: p.name)
def test_pyright_pass_file(path: Path) -> None:
    assert PASS_FILES, "no pyright pass tests found"
    res = _run_pyright(path)
    assert res.returncode == 0, f"{path}\n{_fmt_res(res)}"


@pytest.mark.parametrize("path", FAIL_FILES, ids=lambda p: p.name)
def test_pyright_fail_file(path: Path) -> None:
    assert FAIL_FILES, "no pyright fail tests found"
    res = _run_pyright(path)
    assert res.returncode != 0, f"expected pyright to fail but it passed: {path}\n{_fmt_res(res)}"
