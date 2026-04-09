from __future__ import annotations

import warnings


def warn_renamed(*, old: str, new: str, remove_in: str | None = None) -> None:
    msg = f"`{old}` is deprecated; use `{new}` instead."
    if remove_in is not None:
        msg = f"{msg} (will be removed in {remove_in})"
    warnings.warn(msg, DeprecationWarning, stacklevel=3)

