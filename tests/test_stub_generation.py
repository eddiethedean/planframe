from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_generate_typing_stubs_check() -> None:
    res = subprocess.run(
        [str(ROOT / ".venv" / "bin" / "python"), "scripts/generate_typing_stubs.py", "--check"],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
    )
    assert res.returncode == 0, res.stdout + "\n" + res.stderr
