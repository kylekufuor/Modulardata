# =============================================================================
# app/routers/history.py - Version History & Rollback Endpoints
# =============================================================================
# Handles the version tree navigation and undo/rollback functionality.
#
# Endpoints:
# - GET /history: Get full session history (nodes + messages)
# - GET /nodes: List all nodes for a session
# - GET /nodes/{node_id}: Get specific node details
# - POST /rollback: Revert to a previous node
# =============================================================================

import logging
from typing import Annotated
from uuid import UUID
from datetime import datetime

from fastapi import APIRouter, Path, HTTPException, Query, Depends
from pydantic import BaseModel, Field

from app.auth import get_current_user, AuthUser
from app.exceptions import SessionNotFoundError, NodeNotFoundError
from core.services.session_service import SessionService
from core.services.node_service import NodeService
from lib.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

router = APIRouter()


# =============================================================================
# Response Models
# =============================================================================

class NodeSummary(BaseModel):
    """Summary of a node in the version tree."""
    id: str = Field(..., example="a1b2c3d4-e5f6-7890-abcd-ef1234567890")
    parent_id: str | None = Field(None, example="00000000-0000-0000-0000-000000000000")
    created_at: datetime = Field(..., example="2024-01-15T10:30:00Z")
    transformation: str | None = Field(None, example="Remove rows where email is empty")
    row_count: int = Field(..., example=1000)
    column_count: int = Field(..., example=5)
    is_current: bool = Field(False, example=True)


class ChatMessageSummary(BaseModel):
    """Summary of a chat message."""
    id: str = Field(..., example="msg-123")
    role: str = Field(..., example="user", description="Either 'user' or 'assistant'")
    content: str = Field(..., example="remove rows where email is blank")
    node_id: str | None = Field(None, example="a1b2c3d4-e5f6-7890-abcd-ef1234567890")
    created_at: datetime = Field(..., example="2024-01-15T10:31:00Z")


class SessionHistoryResponse(BaseModel):
    """Full session history including nodes and messages."""
    session_id: str = Field(..., example="550e8400-e29b-41d4-a716-446655440000")
    current_node_id: str | None = Field(None, example="a1b2c3d4-e5f6-7890-abcd-ef1234567890")
    total_nodes: int = Field(..., example=3)
    total_messages: int = Field(..., example=5)
    nodes: list[NodeSummary]
    messages: list[ChatMessageSummary]


class NodeDetailResponse(BaseModel):
    """Detailed node information."""
    id: str = Field(..., example="a1b2c3d4-e5f6-7890-abcd-ef1234567890")
    session_id: str = Field(..., example="550e8400-e29b-41d4-a716-446655440000")
    parent_id: str | None = Field(None, example="00000000-0000-0000-0000-000000000000")
    created_at: datetime = Field(..., example="2024-01-15T10:30:00Z")
    transformation: str | None = Field(None, example="Remove rows where email is empty")
    transformation_code: str | None = Field(None, example="df = df[df['email'].notna()]")
    row_count: int = Field(..., example=1000)
    column_count: int = Field(..., example=5)
    storage_path: str | None = Field(None, example="sessions/550e8400.../data.csv")
    preview_rows: list[dict] | None = Field(None, example=[{"name": "Alice", "email": "alice@example.com"}])
    is_current: bool = Field(False, example=True)


class RollbackRequest(BaseModel):
    """Request to rollback to a specific node."""
    target_node_id: str = Field(
        ...,
        description="The node ID to rollback to",
        example="00000000-0000-0000-0000-000000000000"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "target_node_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
            }
        }
    }


class RollbackResponse(BaseModel):
    """Response from rollback operation."""
    success: bool = Field(..., example=True)
    session_id: str = Field(..., example="550e8400-e29b-41d4-a716-446655440000")
    previous_node_id: str | None = Field(None, example="node-after-transform")
    current_node_id: str = Field(..., example="a1b2c3d4-e5f6-7890-abcd-ef1234567890")
    message: str = Field(..., example="Rolled back to node a1b2c3d4... (row count: 1000)")

    model_config = {
        "json_schema_extra": {
            "example": {
                "success": True,
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "previous_node_id": "new-node-id",
                "current_node_id": "original-node-id",
                "message": "Rolled back to node a1b2c3d4... (row count: 1000)"
            }
        }
    }


# =============================================================================
# History Endpoints
# =============================================================================

@router.get("/{session_id}/history", response_model=SessionHistoryResponse)
async def get_session_history(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    include_messages: bool = Query(True, description="Include chat messages"),
    user: AuthUser = Depends(get_current_user),
):
    """
    Get the full history of a session.

    Returns:
    - All nodes in the version tree (chronological order)
    - All chat messages (if include_messages=true)
    - Current node indicator

    Use this to:
    - Display the transformation timeline
    - Show the undo/redo options
    - Review what changes were made

    User must own the session.
    """
    session_id_str = str(session_id)

    # Verify session exists and user owns it
    try:
        session = SessionService.get_session(session_id_str, user_id=user.id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    current_node_id = session.get("current_node_id")

    # Get all nodes
    nodes_data = NodeService.get_node_history(session_id_str)
    nodes = [
        NodeSummary(
            id=n["id"],
            parent_id=n.get("parent_id"),
            created_at=n["created_at"],
            transformation=n.get("transformation"),
            row_count=n.get("row_count", 0),
            column_count=n.get("column_count", 0),
            is_current=(n["id"] == current_node_id),
        )
        for n in nodes_data
    ]

    # Get chat messages if requested
    messages = []
    if include_messages:
        messages_data = SupabaseClient.fetch_chat_messages(session_id_str, limit=100)
        messages = [
            ChatMessageSummary(
                id=m["id"],
                role=m["role"],
                content=m["content"],
                node_id=m.get("node_id"),
                created_at=m["created_at"],
            )
            for m in messages_data
        ]

    return SessionHistoryResponse(
        session_id=session_id_str,
        current_node_id=current_node_id,
        total_nodes=len(nodes),
        total_messages=len(messages),
        nodes=nodes,
        messages=messages,
    )


@router.get("/{session_id}/nodes", response_model=list[NodeSummary])
async def list_nodes(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    user: AuthUser = Depends(get_current_user),
):
    """
    List all nodes (versions) for a session.

    Returns nodes in chronological order with:
    - Node ID and parent ID (for tree structure)
    - Transformation description
    - Row/column counts
    - Whether it's the current node

    User must own the session.
    """
    session_id_str = str(session_id)

    try:
        session = SessionService.get_session(session_id_str, user_id=user.id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    current_node_id = session.get("current_node_id")
    nodes_data = NodeService.get_node_history(session_id_str)

    return [
        NodeSummary(
            id=n["id"],
            parent_id=n.get("parent_id"),
            created_at=n["created_at"],
            transformation=n.get("transformation"),
            row_count=n.get("row_count", 0),
            column_count=n.get("column_count", 0),
            is_current=(n["id"] == current_node_id),
        )
        for n in nodes_data
    ]


@router.get("/{session_id}/nodes/{node_id}", response_model=NodeDetailResponse)
async def get_node_detail(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    node_id: Annotated[UUID, Path(description="Node UUID")],
    user: AuthUser = Depends(get_current_user),
):
    """
    Get detailed information about a specific node.

    Includes:
    - Full transformation details and code
    - Data preview (first 10 rows)
    - Storage path

    User must own the session.
    """
    session_id_str = str(session_id)
    node_id_str = str(node_id)

    try:
        session = SessionService.get_session(session_id_str, user_id=user.id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    try:
        node = NodeService.get_node(node_id_str)
    except NodeNotFoundError:
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id_str}")

    # Verify node belongs to session
    if node.get("session_id") != session_id_str:
        raise HTTPException(status_code=404, detail=f"Node not found in this session")

    current_node_id = session.get("current_node_id")

    return NodeDetailResponse(
        id=node["id"],
        session_id=node["session_id"],
        parent_id=node.get("parent_id"),
        created_at=node["created_at"],
        transformation=node.get("transformation"),
        transformation_code=node.get("transformation_code"),
        row_count=node.get("row_count", 0),
        column_count=node.get("column_count", 0),
        storage_path=node.get("storage_path"),
        preview_rows=node.get("preview_rows"),
        is_current=(node["id"] == current_node_id),
    )


# =============================================================================
# Rollback Endpoints
# =============================================================================

@router.post("/{session_id}/rollback", response_model=RollbackResponse)
async def rollback_to_node(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    request: RollbackRequest,
    user: AuthUser = Depends(get_current_user),
):
    """
    Rollback the session to a previous node (undo).

    This changes the session's "current pointer" to the specified node.
    The next transformation will branch from this node, creating a new timeline.

    Note: This does NOT delete any nodes. All history is preserved.
    You can always rollback to any node in the tree.
    User must own the session.
    """
    session_id_str = str(session_id)
    target_node_id = request.target_node_id

    # Verify session exists and user owns it
    try:
        session = SessionService.get_session(session_id_str, user_id=user.id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    previous_node_id = session.get("current_node_id")

    # Verify target node exists and belongs to this session
    try:
        target_node = NodeService.get_node(target_node_id)
    except NodeNotFoundError:
        raise HTTPException(status_code=404, detail=f"Node not found: {target_node_id}")

    if target_node.get("session_id") != session_id_str:
        raise HTTPException(status_code=400, detail="Node does not belong to this session")

    # Already at this node?
    if previous_node_id == target_node_id:
        return RollbackResponse(
            success=True,
            session_id=session_id_str,
            previous_node_id=previous_node_id,
            current_node_id=target_node_id,
            message="Already at this node",
        )

    # Update session's current node
    SessionService.update_session(
        session_id=session_id_str,
        current_node_id=target_node_id,
    )

    # Log the rollback action
    logger.info(f"Session {session_id_str} rolled back: {previous_node_id} -> {target_node_id}")

    # Optionally save a chat message about the rollback
    try:
        SupabaseClient.insert_chat_message(
            session_id=session_id_str,
            role="assistant",
            content=f"Rolled back to previous version (node {target_node_id[:8]}...)",
            node_id=target_node_id,
            metadata={"action": "rollback", "from_node": previous_node_id},
        )
    except Exception as e:
        logger.warning(f"Failed to log rollback message: {e}")

    return RollbackResponse(
        success=True,
        session_id=session_id_str,
        previous_node_id=previous_node_id,
        current_node_id=target_node_id,
        message=f"Rolled back to node {target_node_id[:8]}... (row count: {target_node.get('row_count', '?')})",
    )


@router.post("/{session_id}/undo", response_model=RollbackResponse)
async def undo_last_transformation(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    user: AuthUser = Depends(get_current_user),
):
    """
    Undo the last transformation (go to parent node).

    This is a convenience endpoint that automatically finds the parent
    of the current node and rolls back to it.

    Returns an error if already at the root node (original upload).
    User must own the session.
    """
    session_id_str = str(session_id)

    # Get session and verify ownership
    try:
        session = SessionService.get_session(session_id_str, user_id=user.id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    current_node_id = session.get("current_node_id")
    if not current_node_id:
        raise HTTPException(status_code=400, detail="No data uploaded yet")

    # Get current node
    try:
        current_node = NodeService.get_node(current_node_id)
    except NodeNotFoundError:
        raise HTTPException(status_code=500, detail="Current node not found")

    # Check if we have a parent
    parent_id = current_node.get("parent_id")
    if not parent_id:
        raise HTTPException(
            status_code=400,
            detail="Cannot undo: this is the original data (root node)"
        )

    # Rollback to parent
    request = RollbackRequest(target_node_id=parent_id)
    return await rollback_to_node(session_id, request, user)


@router.get("/{session_id}/lineage/{node_id}", response_model=list[NodeSummary])
async def get_node_lineage(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    node_id: Annotated[UUID, Path(description="Node UUID")],
    depth: int = Query(10, ge=1, le=50, description="How many ancestors to fetch"),
    user: AuthUser = Depends(get_current_user),
):
    """
    Get the ancestor chain (lineage) of a node.

    Returns nodes from the oldest ancestor to the specified node.
    Useful for understanding the transformation history that led to this state.
    User must own the session.
    """
    session_id_str = str(session_id)
    node_id_str = str(node_id)

    try:
        session = SessionService.get_session(session_id_str, user_id=user.id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    current_node_id = session.get("current_node_id")

    # Get lineage
    lineage_data = NodeService.get_node_lineage(node_id_str, depth)

    return [
        NodeSummary(
            id=n["id"],
            parent_id=n.get("parent_id"),
            created_at=n["created_at"],
            transformation=n.get("transformation"),
            row_count=n.get("row_count", 0),
            column_count=n.get("column_count", 0),
            is_current=(n["id"] == current_node_id),
        )
        for n in lineage_data
    ]
