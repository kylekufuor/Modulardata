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
    def create_session(
        user_id: UUID | str,
        original_filename: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a new session.

        Args:
            user_id: The user ID who owns this session
            original_filename: Optional filename (defaults to 'pending_upload.csv')

        Returns:
            Created session dict with id, status, created_at

        Raises:
            Exception: If creation fails
        """
        client = SupabaseClient.get_client()

        # Use placeholder filename if none provided (database requires non-null)
        data = {
            "status": SessionStatus.DRAFT.value,
            "original_filename": original_filename or "pending_upload.csv",
            "user_id": str(user_id),
        }

        try:
            response = (
                client.table("sessions")
                .insert(data)
                .execute()
            )

            if response.data:
                session = response.data[0]
                logger.info(f"Created session: {session['id']} for user: {user_id}")
                return session

            raise Exception("Insert returned no data")

        except Exception as e:
            logger.error(f"Failed to create session: {e}")
            raise

    @staticmethod
    def get_session(
        session_id: str | UUID,
        user_id: UUID | str | None = None,
    ) -> dict[str, Any]:
        """
        Get a session by ID.

        Args:
            session_id: The session UUID
            user_id: If provided, verify the session belongs to this user

        Returns:
            Session dict

        Raises:
            SessionNotFoundError: If session doesn't exist or user doesn't own it
        """
        session = SupabaseClient.fetch_session(session_id)

        if not session:
            raise SessionNotFoundError(str(session_id))

        # Verify ownership if user_id provided
        if user_id and str(session.get("user_id")) != str(user_id):
            # Don't reveal that session exists - return not found
            raise SessionNotFoundError(str(session_id))

        return session

    @staticmethod
    def get_session_with_profile(
        session_id: str | UUID,
        user_id: UUID | str | None = None,
    ) -> dict[str, Any]:
        """
        Get session with current node profile.

        Args:
            session_id: The session UUID
            user_id: If provided, verify the session belongs to this user

        Returns:
            Session dict with profile from current node

        Raises:
            SessionNotFoundError: If session doesn't exist or user doesn't own it
        """
        session = SessionService.get_session(session_id, user_id=user_id)

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
        user_id: UUID | str | None = None,
    ) -> dict[str, Any]:
        """
        Update a session.

        Args:
            session_id: The session UUID
            current_node_id: New current node ID
            status: New status
            original_filename: Filename to set
            user_id: If provided, verify the session belongs to this user

        Returns:
            Updated session dict

        Raises:
            SessionNotFoundError: If session doesn't exist or user doesn't own it
            SessionArchivedError: If session is archived
        """
        # Verify session exists and user owns it
        session = SessionService.get_session(session_id, user_id=user_id)

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
    def archive_session(
        session_id: str | UUID,
        user_id: UUID | str | None = None,
    ) -> dict[str, Any]:
        """
        Archive (soft delete) a session.

        Args:
            session_id: The session UUID
            user_id: If provided, verify the session belongs to this user

        Returns:
            Updated session dict
        """
        return SessionService.update_session(
            session_id,
            status=SessionStatus.ARCHIVED,
            user_id=user_id,
        )

    @staticmethod
    def list_sessions(
        user_id: UUID | str,
        page: int = 1,
        page_size: int = 10,
        status: SessionStatus | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """
        List sessions with pagination for a specific user.

        Args:
            user_id: Filter sessions by this user
            page: Page number (1-indexed)
            page_size: Items per page
            status: Optional status filter

        Returns:
            Tuple of (sessions list, total count)
        """
        client = SupabaseClient.get_client()

        # Build query - always filter by user_id
        query = client.table("sessions").select("*", count="exact")
        query = query.eq("user_id", str(user_id))

        if status:
            query = query.eq("status", status.value)
        else:
            # By default, exclude archived sessions
            query = query.neq("status", SessionStatus.ARCHIVED.value)

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

    @staticmethod
    def deploy_session(
        session_id: str | UUID,
        user_id: UUID | str | None = None,
    ) -> dict[str, Any]:
        """
        Deploy a module, making it ready to run on new data.

        Args:
            session_id: The session UUID
            user_id: If provided, verify the session belongs to this user

        Returns:
            Updated session dict

        Raises:
            SessionNotFoundError: If session doesn't exist or user doesn't own it
            ValueError: If module has no transformations to deploy
        """
        from datetime import datetime, timezone

        # Verify session exists and user owns it
        session = SessionService.get_session(session_id, user_id=user_id)

        # Check if archived
        if session.get("status") == SessionStatus.ARCHIVED.value:
            raise SessionArchivedError(str(session_id))

        # Check if module has data
        if not session.get("current_node_id"):
            raise ValueError("Module has no data. Upload data before deploying.")

        client = SupabaseClient.get_client()
        session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id

        # Get current node to set as deployed version
        current_node_id = session.get("current_node_id")

        try:
            response = (
                client.table("sessions")
                .update({
                    "status": SessionStatus.DEPLOYED.value,
                    "deployed_at": datetime.now(timezone.utc).isoformat(),
                    "deployed_node_id": current_node_id,
                })
                .eq("id", session_id_str)
                .execute()
            )

            if response.data:
                logger.info(f"Deployed session: {session_id_str}, deployed_node_id: {current_node_id}")
                return response.data[0]

            return session

        except Exception as e:
            logger.error(f"Failed to deploy session: {e}")
            raise

    @staticmethod
    def revert_to_draft(
        session_id: str | UUID,
        user_id: UUID | str | None = None,
    ) -> dict[str, Any]:
        """
        Revert a deployed module back to draft status.

        Called automatically when a deployed module is edited.

        Args:
            session_id: The session UUID
            user_id: If provided, verify the session belongs to this user

        Returns:
            Updated session dict
        """
        session = SessionService.get_session(session_id, user_id=user_id)

        # Only revert if currently deployed
        if session.get("status") != SessionStatus.DEPLOYED.value:
            return session

        client = SupabaseClient.get_client()
        session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id

        try:
            response = (
                client.table("sessions")
                .update({"status": SessionStatus.DRAFT.value})
                .eq("id", session_id_str)
                .execute()
            )

            if response.data:
                logger.info(f"Reverted session to draft: {session_id_str}")
                return response.data[0]

            return session

        except Exception as e:
            logger.error(f"Failed to revert session to draft: {e}")
            raise

    @staticmethod
    def is_deployed(session_id: str | UUID) -> bool:
        """Check if a session is deployed."""
        session = SessionService.get_session(session_id)
        return session.get("status") == SessionStatus.DEPLOYED.value
