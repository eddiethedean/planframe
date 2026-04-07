from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYRIGHT_DIR = ROOT / "tests" / "pyright"


def _run_pyright(path: Path) -> subprocess.CompletedProcess[str]:
    cfg = PYRIGHT_DIR / "pyrightconfig.json"
    return subprocess.run(
        [str(ROOT / ".venv" / "bin" / "pyright"), "--project", str(cfg), str(path)],
        text=True,
        capture_output=True,
    )


def test_pyright_pass_suite() -> None:
    pass_dir = PYRIGHT_DIR / "pass"
    files = sorted(pass_dir.glob("*.py"))
    assert files, "no pyright pass tests found"

    for f in files:
        res = _run_pyright(f)
        assert res.returncode == 0, f"{f}\n{res.stdout}\n{res.stderr}"


def test_pyright_fail_suite() -> None:
    fail_dir = PYRIGHT_DIR / "fail"
    files = sorted(fail_dir.glob("*.py"))
    assert files, "no pyright fail tests found"

    for f in files:
        res = _run_pyright(f)
        assert res.returncode != 0, f"expected pyright to fail but it passed: {f}"
