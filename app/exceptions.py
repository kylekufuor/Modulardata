# =============================================================================
# app/exceptions.py - Custom Exception Handlers
# =============================================================================
# Centralized exception handling for the API.
# Following Anthropic's principle: "Errors should tell HOW to fix, not just WHAT failed."
# =============================================================================

from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse


class ModularDataException(Exception):
    """
    Base exception for ModularData API.

    All custom exceptions inherit from this class.
    Provides structured error responses with actionable suggestions.
    """

    def __init__(
        self,
        message: str,
        code: str = "MODULARDATA_ERROR",
        status_code: int = 500,
        suggestion: str | None = None,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.status_code = status_code
        self.suggestion = suggestion
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to API response dict."""
        result = {
            "detail": self.message,
            "code": self.code,
        }
        if self.suggestion:
            result["suggestion"] = self.suggestion
        if self.details:
            result["details"] = self.details
        return result


# =============================================================================
# Session Exceptions
# =============================================================================

class SessionNotFoundError(ModularDataException):
    """Raised when a session ID doesn't exist."""

    def __init__(self, session_id: str):
        super().__init__(
            message=f"Session not found: {session_id}",
            code="SESSION_NOT_FOUND",
            status_code=404,
            suggestion="Check that the session_id is correct and the session hasn't been archived",
            details={"session_id": session_id}
        )


class SessionArchivedError(ModularDataException):
    """Raised when trying to modify an archived session."""

    def __init__(self, session_id: str):
        super().__init__(
            message=f"Session is archived: {session_id}",
            code="SESSION_ARCHIVED",
            status_code=400,
            suggestion="Create a new session to continue working with data",
            details={"session_id": session_id}
        )


# =============================================================================
# Node Exceptions
# =============================================================================

class NodeNotFoundError(ModularDataException):
    """Raised when a node ID doesn't exist."""

    def __init__(self, node_id: str):
        super().__init__(
            message=f"Node not found: {node_id}",
            code="NODE_NOT_FOUND",
            status_code=404,
            suggestion="Check that the node_id is correct",
            details={"node_id": node_id}
        )


class NoDataError(ModularDataException):
    """Raised when a session has no data uploaded."""

    def __init__(self, session_id: str):
        super().__init__(
            message=f"No data uploaded for session: {session_id}",
            code="NO_DATA",
            status_code=400,
            suggestion="Upload a CSV file first using POST /sessions/{id}/upload",
            details={"session_id": session_id}
        )


# =============================================================================
# Upload Exceptions
# =============================================================================

class InvalidFileTypeError(ModularDataException):
    """Raised when uploaded file type is not allowed."""

    def __init__(self, filename: str, allowed: list[str]):
        super().__init__(
            message=f"Invalid file type: {filename}",
            code="INVALID_FILE_TYPE",
            status_code=400,
            suggestion=f"Only these file types are supported: {', '.join(allowed)}",
            details={"filename": filename, "allowed_types": allowed}
        )


class FileTooLargeError(ModularDataException):
    """Raised when uploaded file exceeds size limit."""

    def __init__(self, size_mb: float, max_mb: int):
        super().__init__(
            message=f"File too large: {size_mb:.1f}MB (max: {max_mb}MB)",
            code="FILE_TOO_LARGE",
            status_code=413,
            suggestion=f"Upload a file smaller than {max_mb}MB",
            details={"size_mb": size_mb, "max_mb": max_mb}
        )


class FileReadError(ModularDataException):
    """Raised when CSV file cannot be read."""

    def __init__(self, filename: str, error: str):
        super().__init__(
            message=f"Failed to read file: {error}",
            code="FILE_READ_ERROR",
            status_code=400,
            suggestion="Check that the file is a valid CSV with proper encoding",
            details={"filename": filename, "error": error}
        )


class StorageUploadError(ModularDataException):
    """Raised when file upload to storage fails."""

    def __init__(self, error: str):
        super().__init__(
            message=f"Failed to upload file to storage: {error}",
            code="STORAGE_UPLOAD_ERROR",
            status_code=500,
            suggestion="Try again later or contact support if the issue persists",
            details={"error": error}
        )


class StorageDownloadError(ModularDataException):
    """Raised when file download from storage fails."""

    def __init__(self, path: str, error: str):
        super().__init__(
            message=f"Failed to download file from storage: {error}",
            code="STORAGE_DOWNLOAD_ERROR",
            status_code=500,
            suggestion="Try again later or contact support if the issue persists",
            details={"path": path, "error": error}
        )


# =============================================================================
# Exception Handlers
# =============================================================================

async def modulardata_exception_handler(
    request: Request,
    exc: ModularDataException
) -> JSONResponse:
    """
    Convert ModularDataException to JSON response.

    Returns structured error with:
    - detail: Human-readable message
    - code: Machine-readable error code
    - suggestion: How to fix (if available)
    - details: Additional context
    """
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.to_dict()
    )


async def validation_exception_handler(
    request: Request,
    exc: Exception
) -> JSONResponse:
    """
    Handle Pydantic validation errors.

    Converts validation errors to user-friendly messages.
    """
    return JSONResponse(
        status_code=422,
        content={
            "detail": "Validation error",
            "code": "VALIDATION_ERROR",
            "errors": str(exc)
        }
    )
