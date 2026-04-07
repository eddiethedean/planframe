class PlanFrameError(Exception):
    """Base exception for PlanFrame."""


class PlanFrameSchemaError(PlanFrameError):
    """Raised when schema inference/evolution fails."""


class PlanFrameExpressionError(PlanFrameError):
    """Raised when expression typing/compilation fails."""


class PlanFrameBackendError(PlanFrameError):
    """Raised when a backend adapter fails or is misused."""


class PlanFrameExecutionError(PlanFrameBackendError):
    """Raised when backend execution/collection fails."""
