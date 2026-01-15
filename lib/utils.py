# =============================================================================
# lib/utils.py - Shared Utilities
# =============================================================================
# Common utilities used across the application.
# =============================================================================

from typing import Any
from uuid import UUID


# =============================================================================
# UUID Utilities
# =============================================================================

def normalize_uuid(value: str | UUID) -> str:
    """
    Normalize a UUID to string format.

    Handles both string and UUID objects, ensuring consistent string output.

    Args:
        value: UUID as string or UUID object

    Returns:
        String representation of the UUID

    Example:
        session_id = normalize_uuid(uuid_obj)  # "550e8400-..."
        session_id = normalize_uuid("550e8400-...")  # "550e8400-..."
    """
    return str(value) if isinstance(value, UUID) else value


# =============================================================================
# Base Error Class
# =============================================================================

class ApplicationError(Exception):
    """
    Base error class for application-specific errors.

    Provides actionable error messages following the principle:
    "Errors should tell HOW to fix, not just WHAT failed."

    Attributes:
        code: Error code for categorization
        message: Human-readable error message
        suggestion: Actionable suggestion for fixing the error
        details: Additional context for debugging

    Example:
        class MyServiceError(ApplicationError):
            def __init__(self, message: str, **kwargs):
                super().__init__(message, code="MY_SERVICE_ERROR", **kwargs)
    """

    def __init__(
        self,
        message: str,
        code: str = "APPLICATION_ERROR",
        suggestion: str | None = None,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.suggestion = suggestion
        self.details = details or {}

    def __str__(self) -> str:
        result = f"[{self.code}] {self.message}"
        if self.suggestion:
            result += f"\n  Suggestion: {self.suggestion}"
        return result

    def to_dict(self) -> dict[str, Any]:
        """Convert error to dictionary for API responses."""
        return {
            "code": self.code,
            "message": self.message,
            "suggestion": self.suggestion,
            "details": self.details,
        }
