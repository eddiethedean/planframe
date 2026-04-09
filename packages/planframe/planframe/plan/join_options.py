from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class JoinOptions:
    """Optional join hints for backends that support them (for example Polars).

    Fields that are ``None`` are omitted when calling the backend so its defaults apply.

    ``streaming`` / ``engine_streaming`` mirror :class:`planframe.execution_options.ExecutionOptions`
    (user-level streaming vs engine-level streaming); backends may support none, one, or both.

    **Polars (``planframe-polars``) mapping and precedence**

    Optional fields are forwarded to :meth:`polars.LazyFrame.join` when the installed Polars
    version supports the corresponding keyword. Applied in this order; later steps override
    earlier ones where both set the same underlying argument:

    #. ``coalesce``, ``validate``, ``join_nulls`` (as ``nulls_equal``), ``maintain_order``.
    #. ``streaming`` — sets ``allow_parallel = not streaming`` (disable parallel join paths
       when the user prefers streaming-style execution).
    #. ``allow_parallel`` — overwrites ``allow_parallel`` from the previous step.
    #. ``force_parallel`` — sets Polars ``force_parallel`` (separate from ``allow_parallel``).
    #. ``engine_streaming`` — set when supported by Polars (not all versions expose it).
    """

    coalesce: bool | None = None
    validate: str | None = None
    join_nulls: bool | None = None
    maintain_order: str | bool | None = None
    streaming: bool | None = None
    engine_streaming: bool | None = None
    allow_parallel: bool | None = None
    force_parallel: bool | None = None
