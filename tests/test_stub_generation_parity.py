"""CI contract: Jinja-generated `.pyi` stubs match committed templates (Frame, schema types)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.typing

ROOT = Path(__file__).resolve().parents[1]


def test_generate_typing_stubs_check_passes() -> None:
    """Fails when `packages/planframe/.../*.pyi` drift from `scripts/generate_typing_stubs.py`.

    Same check as ``python scripts/generate_typing_stubs.py --check`` (also run in CI).
    """

    script = ROOT / "scripts" / "generate_typing_stubs.py"
    proc = subprocess.run(
        [sys.executable, str(script), "--check"],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert proc.returncode == 0, (
        "Typing stubs out of date. Run: python scripts/generate_typing_stubs.py\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
