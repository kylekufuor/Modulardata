# =============================================================================
# app/routers/sessions.py - Session CRUD Endpoints
# =============================================================================
# Handles session creation and management.
# All endpoints require authentication.
# =============================================================================

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, Path, Depends
from pydantic import BaseModel, Field

from app.auth import get_current_user, AuthUser
from core.services.session_service import SessionService
from core.models.session import SessionStatus, SessionResponse, SessionList

router = APIRouter()


# =============================================================================
# Request/Response Models
# =============================================================================

class SessionCreateRequest(BaseModel):
    """Optional metadata when creating a session."""
    name: str | None = Field(
        default=None,
        example="Q1 Sales Cleanup",
        description="Optional human-readable name for the session"
    )
    metadata: dict | None = Field(
        default=None,
        example={"project": "sales-analysis", "owner": "data-team"},
        description="Optional metadata dictionary"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"name": "Customer Data Cleanup", "metadata": {"source": "salesforce"}},
                {},
            ]
        }
    }


class SessionCreateResponse(BaseModel):
    """Response when creating a session."""
    session_id: str = Field(..., example="550e8400-e29b-41d4-a716-446655440000")
    status: str = Field(..., example="pending")
    created_at: str = Field(..., example="2024-01-15T10:30:00Z")
    message: str = Field(default="Session created successfully")

    model_config = {
        "json_schema_extra": {
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "pending",
                "created_at": "2024-01-15T10:30:00Z",
                "message": "Session created successfully"
            }
        }
    }


# =============================================================================
# Endpoints
# =============================================================================

@router.post("", response_model=SessionCreateResponse)
async def create_session(
    user: AuthUser = Depends(get_current_user),
    request: SessionCreateRequest | None = None,
):
    """
    Create a new session.

    Creates an empty session. Use POST /sessions/{id}/upload to add data.

    Returns the session_id which is used for all subsequent operations.
    """
    session = SessionService.create_session(user_id=user.id)

    return SessionCreateResponse(
        session_id=str(session["id"]),
        status=session["status"],
        created_at=session["created_at"],
    )


@router.get("/{session_id}")
async def get_session(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    user: AuthUser = Depends(get_current_user),
):
    """
    Get session details.

    Returns the session with current data profile if data has been uploaded.
    User must own the session.
    """
    session = SessionService.get_session_with_profile(str(session_id), user_id=user.id)

    return {
        "session_id": session["id"],
        "status": session["status"],
        "created_at": session["created_at"],
        "original_filename": session.get("original_filename"),
        "current_node_id": session.get("current_node_id"),
        "row_count": session.get("row_count", 0),
        "column_count": session.get("column_count", 0),
        "profile": session.get("profile"),
    }


@router.get("")
async def list_sessions(
    user: AuthUser = Depends(get_current_user),
    page: Annotated[int, Query(ge=1, description="Page number")] = 1,
    page_size: Annotated[int, Query(ge=1, le=100, description="Items per page")] = 10,
    status: Annotated[SessionStatus | None, Query(description="Filter by status")] = None,
):
    """
    List all sessions with pagination.

    Returns only sessions owned by the authenticated user.
    Returns active sessions by default. Use status=archived to see archived sessions.
    """
    sessions, total = SessionService.list_sessions(
        user_id=user.id,
        page=page,
        page_size=page_size,
        status=status,
    )

    return {
        "sessions": [
            {
                "session_id": s["id"],
                "status": s["status"],
                "created_at": s["created_at"],
                "original_filename": s.get("original_filename"),
                "current_node_id": s.get("current_node_id"),
            }
            for s in sessions
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


class SessionUpdateRequest(BaseModel):
    """Request to update session details."""
    name: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        description="New name for the module (stored as original_filename)"
    )


@router.patch("/{session_id}")
async def update_session(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    request: SessionUpdateRequest,
    user: AuthUser = Depends(get_current_user),
):
    """
    Update session/module details.

    Currently supports renaming the module.
    User must own the session.
    """
    session_id_str = str(session_id)

    # Verify ownership
    session = SessionService.get_session(session_id_str, user_id=user.id)

    # Update name if provided (stored in original_filename field)
    if request.name is not None:
        session = SessionService.update_session(
            session_id=session_id_str,
            original_filename=request.name,
        )

    return {
        "session_id": session["id"],
        "name": session.get("original_filename"),
        "message": "Module updated successfully",
    }


@router.delete("/{session_id}")
async def archive_session(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    user: AuthUser = Depends(get_current_user),
):
    """
    Archive (soft delete) a session.

    The session is marked as archived but not deleted.
    Data history is preserved for auditing.
    User must own the session.
    """
    session = SessionService.archive_session(str(session_id), user_id=user.id)

    return {
        "session_id": session["id"],
        "status": session["status"],
        "message": "Session archived successfully",
    }
