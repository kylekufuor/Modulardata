# =============================================================================
# app/routers/data.py - Data Access Endpoints
# =============================================================================
# Provides endpoints for accessing data profiles and downloading data.
# =============================================================================

import io
import logging
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Path, Query, Depends
from fastapi.responses import StreamingResponse

from app.auth import get_current_user, AuthUser
from app.exceptions import NoDataError, NodeNotFoundError
from core.services.session_service import SessionService
from core.services.node_service import NodeService
from core.services.storage_service import StorageService

logger = logging.getLogger(__name__)

router = APIRouter()


# =============================================================================
# Profile Endpoints
# =============================================================================

@router.get("/{session_id}/profile")
async def get_profile(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    user: AuthUser = Depends(get_current_user),
):
    """
    Get the current data profile for a session.

    Returns detailed column information, statistics, and data quality issues.
    User must own the session.
    """
    session_id_str = str(session_id)

    # Verify session exists and user owns it
    SessionService.get_session(session_id_str, user_id=user.id)

    # Get current node
    current_node = NodeService.get_current_node(session_id_str)

    if not current_node:
        raise NoDataError(session_id_str)

    profile = current_node.get("profile_json", {})

    return {
        "session_id": session_id_str,
        "node_id": current_node["id"],
        "row_count": current_node.get("row_count", 0),
        "column_count": current_node.get("column_count", 0),
        "profile": profile,
    }


@router.get("/{session_id}/profile/summary")
async def get_profile_summary(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    user: AuthUser = Depends(get_current_user),
):
    """
    Get a compact profile summary.

    Returns just the essential info: row/column counts and column names.
    User must own the session.
    """
    session_id_str = str(session_id)

    # Verify session exists and user owns it
    SessionService.get_session(session_id_str, user_id=user.id)

    # Get current node
    current_node = NodeService.get_current_node(session_id_str)

    if not current_node:
        raise NoDataError(session_id_str)

    profile = current_node.get("profile_json", {})
    columns = profile.get("columns", [])

    return {
        "session_id": session_id_str,
        "row_count": current_node.get("row_count", 0),
        "column_count": current_node.get("column_count", 0),
        "column_names": [c.get("name") for c in columns],
    }


# =============================================================================
# Data Download Endpoints
# =============================================================================

@router.get("/{session_id}/data")
async def get_data(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    format: Annotated[Literal["csv", "json"], Query(description="Output format")] = "csv",
    limit: Annotated[int | None, Query(ge=1, le=10000, description="Max rows to return")] = None,
    user: AuthUser = Depends(get_current_user),
):
    """
    Download the current data.

    Supports CSV (default) or JSON format.
    Use limit parameter to get only first N rows.
    User must own the session.
    """
    session_id_str = str(session_id)

    # Verify session exists and user owns it
    SessionService.get_session(session_id_str, user_id=user.id)

    # Get current node
    current_node = NodeService.get_current_node(session_id_str)

    if not current_node:
        raise NoDataError(session_id_str)

    storage_path = current_node.get("storage_path")
    if not storage_path:
        raise NoDataError(session_id_str)

    # Download data
    df = StorageService.download_csv(storage_path)

    # Apply limit if specified
    if limit:
        df = df.head(limit)

    # Return based on format
    if format == "json":
        # Replace NaN with None for JSON serialization
        data = df.where(df.notnull(), None).to_dict(orient="records")
        return {
            "session_id": session_id_str,
            "node_id": current_node["id"],
            "row_count": len(df),
            "column_count": len(df.columns),
            "data": data,
        }

    else:  # CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        filename = f"session_{session_id_str[:8]}_data.csv"

        return StreamingResponse(
            iter([csv_buffer.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
            }
        )


@router.get("/{session_id}/preview")
async def get_preview(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    rows: Annotated[int, Query(ge=1, le=100, description="Number of rows")] = 10,
    user: AuthUser = Depends(get_current_user),
):
    """
    Get a quick preview of the data.

    Returns first N rows as JSON for display in UI.
    This is faster than downloading full data.
    User must own the session.
    """
    session_id_str = str(session_id)

    # Verify session exists and user owns it
    SessionService.get_session(session_id_str, user_id=user.id)

    # Get current node
    current_node = NodeService.get_current_node(session_id_str)

    if not current_node:
        raise NoDataError(session_id_str)

    # Check if we have preview_rows cached
    preview_rows = current_node.get("preview_rows")

    if preview_rows and len(preview_rows) >= rows:
        return {
            "session_id": session_id_str,
            "node_id": current_node["id"],
            "row_count": current_node.get("row_count", 0),
            "column_count": current_node.get("column_count", 0),
            "preview": preview_rows[:rows],
        }

    # Otherwise, fetch from storage
    storage_path = current_node.get("storage_path")
    if not storage_path:
        raise NoDataError(session_id_str)

    df = StorageService.download_csv(storage_path)
    # Replace NaN with None for JSON serialization
    preview = df.head(rows).where(df.head(rows).notnull(), None).to_dict(orient="records")

    return {
        "session_id": session_id_str,
        "node_id": current_node["id"],
        "row_count": len(df),
        "column_count": len(df.columns),
        "preview": preview,
    }


# =============================================================================
# Node-Specific Data Access
# =============================================================================

@router.get("/{session_id}/nodes/{node_id}/data")
async def get_node_data(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    node_id: Annotated[UUID, Path(description="Node UUID")],
    format: Annotated[Literal["csv", "json"], Query(description="Output format")] = "csv",
    limit: Annotated[int | None, Query(ge=1, le=10000, description="Max rows")] = None,
    user: AuthUser = Depends(get_current_user),
):
    """
    Download data from a specific node (version).

    Useful for comparing different versions or downloading historical data.
    User must own the session.
    """
    session_id_str = str(session_id)
    node_id_str = str(node_id)

    # Verify session exists and user owns it
    SessionService.get_session(session_id_str, user_id=user.id)

    # Get node
    node = NodeService.get_node(node_id_str)

    # Verify node belongs to session
    if str(node.get("session_id")) != session_id_str:
        raise NodeNotFoundError(node_id_str)

    storage_path = node.get("storage_path")
    if not storage_path:
        raise NoDataError(session_id_str)

    # Download data
    df = StorageService.download_csv(storage_path)

    if limit:
        df = df.head(limit)

    if format == "json":
        # Replace NaN with None for JSON serialization
        data = df.where(df.notnull(), None).to_dict(orient="records")
        return {
            "session_id": session_id_str,
            "node_id": node_id_str,
            "row_count": len(df),
            "column_count": len(df.columns),
            "data": data,
        }

    else:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        filename = f"session_{session_id_str[:8]}_node_{node_id_str[:8]}.csv"

        return StreamingResponse(
            iter([csv_buffer.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
            }
        )


@router.get("/{session_id}/nodes/{node_id}/profile")
async def get_node_profile(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    node_id: Annotated[UUID, Path(description="Node UUID")],
    user: AuthUser = Depends(get_current_user),
):
    """
    Get profile for a specific node (version).

    Useful for comparing how data changed across versions.
    User must own the session.
    """
    session_id_str = str(session_id)
    node_id_str = str(node_id)

    # Verify session exists and user owns it
    SessionService.get_session(session_id_str, user_id=user.id)

    # Get node
    node = NodeService.get_node(node_id_str)

    # Verify node belongs to session
    if str(node.get("session_id")) != session_id_str:
        raise NodeNotFoundError(node_id_str)

    return {
        "session_id": session_id_str,
        "node_id": node_id_str,
        "row_count": node.get("row_count", 0),
        "column_count": node.get("column_count", 0),
        "transformation": node.get("transformation"),
        "transformation_code": node.get("transformation_code"),
        "profile": node.get("profile_json", {}),
    }
