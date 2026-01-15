# =============================================================================
# lib/supabase_client.py - Supabase Client Wrapper
# =============================================================================
# This module provides a typed wrapper for Supabase database operations.
# It implements the singleton pattern to reuse a single client connection
# and provides specialized methods for fetching:
# - Chat messages for conversation history
# - Node profiles for data context
# - Session information
# - Node lineage for undo/rollback operations
#
# Design follows Anthropic's "Just-In-Time Retrieval" principle:
# Fetch only what's needed, when it's needed.
#
# Usage:
#   from lib.supabase_client import SupabaseClient
#   messages = await SupabaseClient.fetch_chat_messages(session_id, limit=10)
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from supabase import create_client, Client

from app.config import settings

# Set up logging for this module
logger = logging.getLogger(__name__)


class SupabaseClientError(Exception):
    """
    Error during Supabase operations.

    Provides actionable error messages following Anthropic's principle:
    "Errors should tell HOW to fix, not just WHAT failed."
    """

    def __init__(
        self,
        message: str,
        code: str = "SUPABASE_ERROR",
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
            result += f" Suggestion: {self.suggestion}"
        return result


class SupabaseClient:
    """
    Typed wrapper for Supabase database operations.

    Implements singleton pattern - one client instance is shared across
    the application. All methods are class methods for easy access without
    instantiation.

    Example:
        # Fetch last 10 messages for a session
        messages = await SupabaseClient.fetch_chat_messages(
            session_id="550e8400-...",
            limit=10
        )

        # Fetch current node's profile
        node = await SupabaseClient.fetch_current_node(session_id)
        profile = node.get("profile_json") if node else None
    """

    _instance: Client | None = None

    @classmethod
    def get_client(cls) -> Client:
        """
        Get or create the singleton Supabase client.

        Uses service_role key which bypasses Row Level Security (RLS).
        This is appropriate for server-side operations.

        Returns:
            Client: Supabase client instance

        Raises:
            SupabaseClientError: If client creation fails
        """
        if cls._instance is None:
            try:
                cls._instance = create_client(
                    settings.SUPABASE_URL,
                    settings.SUPABASE_SERVICE_KEY
                )
                logger.info("Supabase client initialized successfully")
            except Exception as e:
                raise SupabaseClientError(
                    message=f"Failed to create Supabase client: {e}",
                    code="CLIENT_INIT_FAILED",
                    suggestion="Check SUPABASE_URL and SUPABASE_SERVICE_KEY in your .env file"
                )
        return cls._instance

    @classmethod
    def _normalize_uuid(cls, uuid_value: str | UUID) -> str:
        """Convert UUID to string for queries."""
        return str(uuid_value) if isinstance(uuid_value, UUID) else uuid_value

    # -------------------------------------------------------------------------
    # Chat Messages
    # -------------------------------------------------------------------------

    @classmethod
    def fetch_chat_messages(
        cls,
        session_id: str | UUID,
        limit: int = 10,
        order: str = "asc",
    ) -> list[dict[str, Any]]:
        """
        Fetch chat messages for a session.

        Returns messages in chronological order (oldest first by default)
        for proper conversation context building.

        Args:
            session_id: The session UUID
            limit: Maximum number of messages to return (default: 10)
            order: Sort order - "asc" (oldest first) or "desc" (newest first)

        Returns:
            List of message dicts with keys:
            - id: Message UUID
            - role: "user" or "assistant"
            - content: Message text
            - created_at: Timestamp
            - node_id: Associated node UUID (nullable)
            - metadata: Additional JSONB data

        Raises:
            SupabaseClientError: If query fails

        Example:
            messages = SupabaseClient.fetch_chat_messages(
                session_id="550e8400-...",
                limit=10,
                order="asc"
            )
            for msg in messages:
                print(f"{msg['role']}: {msg['content'][:50]}...")
        """
        client = cls.get_client()
        session_id_str = cls._normalize_uuid(session_id)

        try:
            # Build query: fetch from chat_logs table
            query = (
                client.table("chat_logs")
                .select("id, role, content, created_at, node_id, metadata")
                .eq("session_id", session_id_str)
                .order("created_at", desc=(order == "desc"))
                .limit(limit)
            )

            response = query.execute()

            # If we fetched in desc order but want asc for display, reverse
            messages = response.data or []
            if order == "desc":
                messages = list(reversed(messages))

            logger.debug(f"Fetched {len(messages)} messages for session {session_id_str}")
            return messages

        except Exception as e:
            raise SupabaseClientError(
                message=f"Failed to fetch chat messages: {e}",
                code="FETCH_MESSAGES_FAILED",
                suggestion="Check that the session_id exists and chat_logs table is accessible",
                details={"session_id": session_id_str, "limit": limit}
            )

    # -------------------------------------------------------------------------
    # Node Operations
    # -------------------------------------------------------------------------

    @classmethod
    def fetch_node(cls, node_id: str | UUID) -> dict[str, Any] | None:
        """
        Fetch a specific node by ID.

        Args:
            node_id: The node UUID

        Returns:
            Node dict with all fields, or None if not found

        Raises:
            SupabaseClientError: If query fails
        """
        client = cls.get_client()
        node_id_str = cls._normalize_uuid(node_id)

        try:
            response = (
                client.table("nodes")
                .select("*")
                .eq("id", node_id_str)
                .single()
                .execute()
            )

            return response.data

        except Exception as e:
            # Check if it's a "not found" error
            if "PGRST116" in str(e):  # PostgREST code for no rows
                return None
            raise SupabaseClientError(
                message=f"Failed to fetch node: {e}",
                code="FETCH_NODE_FAILED",
                suggestion="Check that the node_id exists",
                details={"node_id": node_id_str}
            )

    @classmethod
    def fetch_node_profile(cls, node_id: str | UUID) -> dict[str, Any] | None:
        """
        Fetch only the profile_json for a node.

        This is more efficient than fetching the full node when
        you only need the data profile.

        Args:
            node_id: The node UUID

        Returns:
            Profile dict (parsed from JSONB), or None if not found

        Raises:
            SupabaseClientError: If query fails
        """
        client = cls.get_client()
        node_id_str = cls._normalize_uuid(node_id)

        try:
            response = (
                client.table("nodes")
                .select("profile_json")
                .eq("id", node_id_str)
                .single()
                .execute()
            )

            if response.data:
                return response.data.get("profile_json")
            return None

        except Exception as e:
            if "PGRST116" in str(e):
                return None
            raise SupabaseClientError(
                message=f"Failed to fetch node profile: {e}",
                code="FETCH_PROFILE_FAILED",
                details={"node_id": node_id_str}
            )

    @classmethod
    def fetch_current_node(cls, session_id: str | UUID) -> dict[str, Any] | None:
        """
        Fetch the current (active) node for a session.

        First looks up the session's current_node_id, then fetches
        that node's full data.

        Args:
            session_id: The session UUID

        Returns:
            Current node dict with all fields, or None if no current node

        Raises:
            SupabaseClientError: If query fails
        """
        client = cls.get_client()
        session_id_str = cls._normalize_uuid(session_id)

        try:
            # First, get the session to find current_node_id
            session_response = (
                client.table("sessions")
                .select("current_node_id")
                .eq("id", session_id_str)
                .single()
                .execute()
            )

            if not session_response.data:
                return None

            current_node_id = session_response.data.get("current_node_id")
            if not current_node_id:
                return None

            # Now fetch the full node
            return cls.fetch_node(current_node_id)

        except Exception as e:
            if "PGRST116" in str(e):
                return None
            raise SupabaseClientError(
                message=f"Failed to fetch current node: {e}",
                code="FETCH_CURRENT_NODE_FAILED",
                suggestion="Check that the session exists and has a current_node_id",
                details={"session_id": session_id_str}
            )

    @classmethod
    def fetch_node_lineage(
        cls,
        node_id: str | UUID,
        depth: int = 3,
    ) -> list[dict[str, Any]]:
        """
        Fetch the parent chain (lineage) of a node.

        Traverses the parent_id links to build version history.
        Useful for understanding what transformations led to current state
        and for resolving "undo" requests.

        Args:
            node_id: Starting node UUID
            depth: How many ancestors to fetch (default: 3)

        Returns:
            List of node dicts, ordered from oldest ancestor to current.
            Example: [grandparent, parent, current]

        Raises:
            SupabaseClientError: If query fails

        Example:
            # For undo, get the parent (second-to-last in lineage)
            lineage = SupabaseClient.fetch_node_lineage(current_node_id, depth=2)
            parent_node = lineage[-2] if len(lineage) >= 2 else None
        """
        client = cls.get_client()
        node_id_str = cls._normalize_uuid(node_id)

        lineage: list[dict[str, Any]] = []
        current_id: str | None = node_id_str

        try:
            # Walk up the parent chain
            while current_id and len(lineage) < depth:
                response = (
                    client.table("nodes")
                    .select("id, parent_id, transformation, transformation_code, created_at, row_count, column_count")
                    .eq("id", current_id)
                    .single()
                    .execute()
                )

                if not response.data:
                    break

                # Insert at beginning so oldest is first
                lineage.insert(0, response.data)
                current_id = response.data.get("parent_id")

            logger.debug(f"Fetched lineage of {len(lineage)} nodes for {node_id_str}")
            return lineage

        except Exception as e:
            if "PGRST116" in str(e):
                return lineage  # Return what we have so far
            raise SupabaseClientError(
                message=f"Failed to fetch node lineage: {e}",
                code="FETCH_LINEAGE_FAILED",
                details={"node_id": node_id_str, "depth": depth}
            )

    # -------------------------------------------------------------------------
    # Session Operations
    # -------------------------------------------------------------------------

    @classmethod
    def fetch_session(cls, session_id: str | UUID) -> dict[str, Any] | None:
        """
        Fetch a session by ID.

        Args:
            session_id: The session UUID

        Returns:
            Session dict with all fields, or None if not found

        Raises:
            SupabaseClientError: If query fails
        """
        client = cls.get_client()
        session_id_str = cls._normalize_uuid(session_id)

        try:
            response = (
                client.table("sessions")
                .select("*")
                .eq("id", session_id_str)
                .single()
                .execute()
            )

            return response.data

        except Exception as e:
            if "PGRST116" in str(e):
                return None
            raise SupabaseClientError(
                message=f"Failed to fetch session: {e}",
                code="FETCH_SESSION_FAILED",
                suggestion="Check that the session_id exists",
                details={"session_id": session_id_str}
            )

    # -------------------------------------------------------------------------
    # Write Operations (for completeness)
    # -------------------------------------------------------------------------

    @classmethod
    def insert_chat_message(
        cls,
        session_id: str | UUID,
        role: str,
        content: str,
        node_id: str | UUID | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Insert a new chat message.

        Args:
            session_id: The session UUID
            role: "user" or "assistant"
            content: Message text
            node_id: Associated node UUID (optional)
            metadata: Additional JSONB data (optional)

        Returns:
            Inserted message dict with generated id and created_at

        Raises:
            SupabaseClientError: If insert fails
        """
        client = cls.get_client()

        data = {
            "session_id": cls._normalize_uuid(session_id),
            "role": role,
            "content": content,
            "metadata": metadata or {},
        }

        if node_id:
            data["node_id"] = cls._normalize_uuid(node_id)

        try:
            response = (
                client.table("chat_logs")
                .insert(data)
                .execute()
            )

            if response.data:
                return response.data[0]
            raise SupabaseClientError(
                message="Insert returned no data",
                code="INSERT_NO_DATA"
            )

        except SupabaseClientError:
            raise
        except Exception as e:
            raise SupabaseClientError(
                message=f"Failed to insert chat message: {e}",
                code="INSERT_MESSAGE_FAILED",
                details={"session_id": str(session_id), "role": role}
            )
