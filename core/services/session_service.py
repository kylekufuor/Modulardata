# =============================================================================
# core/services/session_service.py - Session Business Logic
# =============================================================================
# Handles session CRUD operations and business logic.
# Separates HTTP concerns from database/business logic.
# =============================================================================

import logging
from typing import Any
from uuid import UUID

from lib.supabase_client import SupabaseClient, SupabaseClientError
from core.models.session import SessionStatus, SessionResponse
from app.exceptions import SessionNotFoundError, SessionArchivedError

logger = logging.getLogger(__name__)


class SessionService:
    """
    Service for session management operations.

    Provides a clean interface between API routes and database.
    """

    @staticmethod
    def create_session(original_filename: str | None = None) -> dict[str, Any]:
        """
        Create a new session.

        Args:
            original_filename: Optional filename (defaults to 'pending_upload.csv')

        Returns:
            Created session dict with id, status, created_at

        Raises:
            Exception: If creation fails
        """
        client = SupabaseClient.get_client()

        # Use placeholder filename if none provided (database requires non-null)
        data = {
            "status": SessionStatus.ACTIVE.value,
            "original_filename": original_filename or "pending_upload.csv",
        }

        try:
            response = (
                client.table("sessions")
                .insert(data)
                .execute()
            )

            if response.data:
                session = response.data[0]
                logger.info(f"Created session: {session['id']}")
                return session

            raise Exception("Insert returned no data")

        except Exception as e:
            logger.error(f"Failed to create session: {e}")
            raise

    @staticmethod
    def get_session(session_id: str | UUID) -> dict[str, Any]:
        """
        Get a session by ID.

        Args:
            session_id: The session UUID

        Returns:
            Session dict

        Raises:
            SessionNotFoundError: If session doesn't exist
        """
        session = SupabaseClient.fetch_session(session_id)

        if not session:
            raise SessionNotFoundError(str(session_id))

        return session

    @staticmethod
    def get_session_with_profile(session_id: str | UUID) -> dict[str, Any]:
        """
        Get session with current node profile.

        Args:
            session_id: The session UUID

        Returns:
            Session dict with profile from current node

        Raises:
            SessionNotFoundError: If session doesn't exist
        """
        session = SessionService.get_session(session_id)

        # Get current node's profile if exists
        if session.get("current_node_id"):
            current_node = SupabaseClient.fetch_node(session["current_node_id"])
            if current_node:
                session["profile"] = current_node.get("profile_json")
                session["row_count"] = current_node.get("row_count", 0)
                session["column_count"] = current_node.get("column_count", 0)

        return session

    @staticmethod
    def update_session(
        session_id: str | UUID,
        current_node_id: str | UUID | None = None,
        status: SessionStatus | None = None,
        original_filename: str | None = None,
    ) -> dict[str, Any]:
        """
        Update a session.

        Args:
            session_id: The session UUID
            current_node_id: New current node ID
            status: New status
            original_filename: Filename to set

        Returns:
            Updated session dict

        Raises:
            SessionNotFoundError: If session doesn't exist
            SessionArchivedError: If session is archived
        """
        # Verify session exists
        session = SessionService.get_session(session_id)

        # Check if archived
        if session.get("status") == SessionStatus.ARCHIVED.value:
            raise SessionArchivedError(str(session_id))

        client = SupabaseClient.get_client()
        session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id

        # Build update data
        update_data = {}
        if current_node_id:
            update_data["current_node_id"] = str(current_node_id) if isinstance(current_node_id, UUID) else current_node_id
        if status:
            update_data["status"] = status.value
        if original_filename:
            update_data["original_filename"] = original_filename

        if not update_data:
            return session  # Nothing to update

        try:
            response = (
                client.table("sessions")
                .update(update_data)
                .eq("id", session_id_str)
                .execute()
            )

            if response.data:
                logger.info(f"Updated session: {session_id_str}")
                return response.data[0]

            return session

        except Exception as e:
            logger.error(f"Failed to update session: {e}")
            raise

    @staticmethod
    def archive_session(session_id: str | UUID) -> dict[str, Any]:
        """
        Archive (soft delete) a session.

        Args:
            session_id: The session UUID

        Returns:
            Updated session dict
        """
        return SessionService.update_session(
            session_id,
            status=SessionStatus.ARCHIVED
        )

    @staticmethod
    def list_sessions(
        page: int = 1,
        page_size: int = 10,
        status: SessionStatus | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """
        List sessions with pagination.

        Args:
            page: Page number (1-indexed)
            page_size: Items per page
            status: Optional status filter

        Returns:
            Tuple of (sessions list, total count)
        """
        client = SupabaseClient.get_client()

        # Build query
        query = client.table("sessions").select("*", count="exact")

        if status:
            query = query.eq("status", status.value)

        # Add pagination
        offset = (page - 1) * page_size
        query = query.order("created_at", desc=True).range(offset, offset + page_size - 1)

        try:
            response = query.execute()
            sessions = response.data or []
            total = response.count or 0

            return sessions, total

        except Exception as e:
            logger.error(f"Failed to list sessions: {e}")
            raise
