# =============================================================================
# core/models/session.py - Session Schemas
# =============================================================================
# These models define the API contract for session operations:
# - SessionCreate: Input for creating a new session (after file upload)
# - SessionResponse: Output when returning session data to clients
# - SessionStatus: Enum for session states
#
# A session represents one user interaction with one CSV file.
# All transformations and conversations are scoped to a session.
# =============================================================================

from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field

from .profile import ProfileSummary


class SessionStatus(str, Enum):
    """
    Possible states for a session/module.

    - draft: Module is being trained/edited, cannot be run on new data
    - deployed: Module is ready to run on new data
    - archived: Module is soft-deleted

    Flow: draft -> deployed -> (edit) -> draft -> deployed
    """
    DRAFT = "draft"
    DEPLOYED = "deployed"
    ARCHIVED = "archived"


class SessionCreate(BaseModel):
    """
    Schema for creating a new session.

    This is used internally after a file upload is processed.
    The API endpoint receives a file, processes it, then creates a session.

    Note: We don't expose this directly to clients - they upload a file
    and the session is created automatically.

    Example:
        {
            "original_filename": "sales_data.csv"
        }
    """

    # The name of the uploaded file (for display purposes)
    original_filename: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Original filename of the uploaded CSV"
    )


class SessionResponse(BaseModel):
    """
    Schema for returning session data to clients.

    Returned by:
    - POST /upload (after file upload creates a session)
    - GET /session/{id} (fetching session details)
    - GET /sessions (listing all sessions)

    Example:
        {
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "created_at": "2024-01-15T10:30:00Z",
            "original_filename": "sales_data.csv",
            "status": "active",
            "current_node_id": "660e8400-e29b-41d4-a716-446655440001",
            "profile": {"row_count": 1000, "column_count": 5, ...}
        }
    """

    # Unique identifier for this session
    id: UUID = Field(
        ...,
        description="Unique session identifier"
    )

    # When the session was created
    created_at: datetime = Field(
        ...,
        description="Timestamp when session was created"
    )

    # Original filename (for display in UI)
    original_filename: str = Field(
        ...,
        description="Original filename of the uploaded CSV"
    )

    # Current status of the session
    status: SessionStatus = Field(
        ...,
        description="Session status (active or archived)"
    )

    # The currently active version (node) of the data
    # Clients use this to know which version they're viewing
    current_node_id: UUID | None = Field(
        default=None,
        description="ID of the currently active data version"
    )

    # Basic profile info (row/column counts, column names)
    # Full profile is available via the node endpoint
    profile: ProfileSummary | None = Field(
        default=None,
        description="Basic data profile summary"
    )

    class Config:
        """Pydantic configuration for this model."""
        # Allow creating from ORM objects (Supabase returns dict-like objects)
        from_attributes = True


class SessionList(BaseModel):
    """
    Schema for listing multiple sessions.

    Returned by GET /sessions endpoint.
    Includes pagination info for large result sets.

    Example:
        {
            "sessions": [...],
            "total": 42,
            "page": 1,
            "page_size": 10
        }
    """

    # List of session summaries
    sessions: list[SessionResponse] = Field(
        default_factory=list,
        description="List of sessions"
    )

    # Total number of sessions (for pagination)
    total: int = Field(
        default=0,
        ge=0,
        description="Total number of sessions"
    )

    # Current page number (1-indexed)
    page: int = Field(
        default=1,
        ge=1,
        description="Current page number"
    )

    # Number of items per page
    page_size: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Number of sessions per page"
    )


class SessionUpdate(BaseModel):
    """
    Schema for updating a session.

    Only certain fields can be updated:
    - status: To archive a session
    - current_node_id: When rolling back to a previous version

    Example:
        {
            "status": "archived"
        }
        or
        {
            "current_node_id": "660e8400-e29b-41d4-a716-446655440001"
        }
    """

    # Update the session status
    status: SessionStatus | None = Field(
        default=None,
        description="New session status"
    )

    # Update the current node (for rollbacks)
    current_node_id: UUID | None = Field(
        default=None,
        description="New current node ID (for version rollback)"
    )
