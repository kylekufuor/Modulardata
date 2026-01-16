# =============================================================================
# core/models/node.py - Node/Version Schemas
# =============================================================================
# These models define the API contract for version tracking:
# - NodeCreate: Input for creating a new version (after transformation)
# - NodeResponse: Output when returning node data to clients
#
# Nodes form a tree structure enabling "Time Travel":
#   Node 0 (original) -> Node 1 -> Node 2
#                              \-> Node 2b (branch after undo)
#
# Each node stores:
# - Link to parent (previous version)
# - Path to CSV file in storage
# - Data profile snapshot
# - Description of transformation
# =============================================================================

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field

from .profile import DataProfile


class NodeCreate(BaseModel):
    """
    Schema for creating a new node (version).

    Used internally after an AI transformation is applied.
    Not directly exposed to API clients.

    Example:
        {
            "session_id": "550e8400-...",
            "parent_id": "660e8400-...",
            "storage_path": "sessions/550e.../node_1.csv",
            "row_count": 850,
            "column_count": 5,
            "transformation": "Dropped 150 rows where 'age' was null"
        }
    """

    # Which session this node belongs to
    session_id: UUID = Field(
        ...,
        description="Session this node belongs to"
    )

    # The parent node (previous version)
    # NULL for Node 0 (the original upload)
    parent_id: UUID | None = Field(
        default=None,
        description="Parent node ID (NULL for original upload)"
    )

    # Path to the CSV file in Supabase Storage
    storage_path: str = Field(
        ...,
        min_length=1,
        description="Path to CSV in Supabase Storage"
    )

    # Row and column counts for quick reference
    row_count: int = Field(
        ...,
        ge=0,
        description="Number of rows in this version"
    )

    column_count: int = Field(
        ...,
        ge=0,
        description="Number of columns in this version"
    )

    # Full data profile (stored as JSONB in DB)
    profile_json: DataProfile | None = Field(
        default=None,
        description="Full data profile for this version"
    )

    # Human-readable description of the transformation
    # NULL for Node 0 (original upload)
    transformation: str | None = Field(
        default=None,
        max_length=500,
        description="Description of transformation that created this version"
    )

    # The actual Python/pandas code that was executed
    # NULL for Node 0 (original upload)
    # This allows users to view, copy, and learn from the generated code
    transformation_code: str | None = Field(
        default=None,
        description="Python/pandas code executed for this transformation"
    )

    # Individual step descriptions for batch transformations
    # Allows UI to show a summary of what each step did
    step_descriptions: list[str] | None = Field(
        default=None,
        description="List of human-readable descriptions for each transformation step"
    )

    # Preview of first N rows as JSON for quick display
    # Avoids needing to fetch full file from storage for previews
    preview_rows: list[dict] | None = Field(
        default=None,
        description="First N rows of data for quick preview (typically 10-20 rows)"
    )


class NodeResponse(BaseModel):
    """
    Schema for returning node data to clients.

    Returned by:
    - GET /node/{id} (fetching specific version)
    - GET /history/{session_id} (listing all versions)

    Example:
        {
            "id": "660e8400-e29b-41d4-a716-446655440001",
            "session_id": "550e8400-e29b-41d4-a716-446655440000",
            "parent_id": "660e8400-e29b-41d4-a716-446655440000",
            "created_at": "2024-01-15T10:35:00Z",
            "row_count": 850,
            "column_count": 5,
            "transformation": "Dropped 150 rows where 'age' was null",
            "is_current": true
        }
    """

    # Unique identifier for this version
    id: UUID = Field(
        ...,
        description="Unique node identifier"
    )

    # Which session this belongs to
    session_id: UUID = Field(
        ...,
        description="Session this node belongs to"
    )

    # Parent node (NULL for Node 0)
    parent_id: UUID | None = Field(
        default=None,
        description="Parent node ID"
    )

    # When this version was created
    created_at: datetime = Field(
        ...,
        description="Timestamp when this version was created"
    )

    # Path to the file in storage
    storage_path: str = Field(
        ...,
        description="Path to CSV in storage"
    )

    # Quick stats about the data
    row_count: int = Field(
        ...,
        ge=0,
        description="Number of rows"
    )

    column_count: int = Field(
        ...,
        ge=0,
        description="Number of columns"
    )

    # Full profile (optional - may be omitted in list views)
    profile: DataProfile | None = Field(
        default=None,
        description="Full data profile (if requested)"
    )

    # What transformation created this version
    transformation: str | None = Field(
        default=None,
        description="Description of transformation"
    )

    # The actual code that was executed (viewable by user)
    transformation_code: str | None = Field(
        default=None,
        description="Python/pandas code executed for this transformation"
    )

    # Individual step descriptions for batch transformations
    step_descriptions: list[str] | None = Field(
        default=None,
        description="List of human-readable descriptions for each transformation step"
    )

    # Quick preview of data at this version
    preview_rows: list[dict] | None = Field(
        default=None,
        description="First N rows for quick preview"
    )

    # Is this the currently active version for the session?
    # Computed field - not stored in DB
    is_current: bool = Field(
        default=False,
        description="Whether this is the active version"
    )

    class Config:
        """Pydantic configuration."""
        from_attributes = True


class NodeHistory(BaseModel):
    """
    Schema for version history of a session.

    Returned by GET /history/{session_id}.
    Shows all versions in chronological order with their relationships.

    Example:
        {
            "session_id": "550e8400-...",
            "current_node_id": "660e8400-...002",
            "nodes": [
                {"id": "...000", "transformation": null, ...},
                {"id": "...001", "transformation": "Dropped nulls", ...},
                {"id": "...002", "transformation": "Renamed column", ...}
            ],
            "total_versions": 3
        }
    """

    # Which session this history is for
    session_id: UUID = Field(
        ...,
        description="Session ID"
    )

    # The currently active version
    current_node_id: UUID | None = Field(
        default=None,
        description="Currently active node ID"
    )

    # All versions in chronological order
    nodes: list[NodeResponse] = Field(
        default_factory=list,
        description="List of all versions"
    )

    # Total number of versions
    total_versions: int = Field(
        default=0,
        ge=0,
        description="Total number of versions"
    )


class RollbackRequest(BaseModel):
    """
    Schema for rolling back to a previous version.

    Used by POST /node/rollback.
    Sets the session's current_node_id to the specified node.

    Example:
        {
            "session_id": "550e8400-...",
            "target_node_id": "660e8400-...001"
        }

    After rollback:
    - session.current_node_id points to target_node_id
    - Future transformations branch from target_node_id
    - Old nodes are NOT deleted (history preserved)
    """

    # Which session to rollback
    session_id: UUID = Field(
        ...,
        description="Session ID"
    )

    # Which version to rollback to
    target_node_id: UUID = Field(
        ...,
        description="Node ID to rollback to"
    )


class RollbackResponse(BaseModel):
    """
    Schema for rollback operation response.

    Confirms the rollback was successful and returns updated state.

    Example:
        {
            "success": true,
            "session_id": "550e8400-...",
            "previous_node_id": "660e8400-...002",
            "current_node_id": "660e8400-...001",
            "message": "Rolled back to version from 2024-01-15T10:32:00Z"
        }
    """

    success: bool = Field(
        ...,
        description="Whether rollback succeeded"
    )

    session_id: UUID = Field(
        ...,
        description="Session ID"
    )

    # What we rolled back from
    previous_node_id: UUID | None = Field(
        default=None,
        description="Previous current node ID"
    )

    # What we rolled back to
    current_node_id: UUID = Field(
        ...,
        description="New current node ID"
    )

    # Human-readable message
    message: str = Field(
        ...,
        description="Status message"
    )
