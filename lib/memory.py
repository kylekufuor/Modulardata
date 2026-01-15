# =============================================================================
# lib/memory.py - Conversation Context Builder
# =============================================================================
# This module builds conversation context for AI agents.
# It follows Anthropic's "Just-In-Time Retrieval" principle:
# - Fetch only what's needed, when it's needed
# - Support concise vs detailed output modes
# - Format context optimally for LLM consumption
#
# The ConversationContext dataclass holds all context needed by the Strategist
# agent to understand the user's request in the context of their data and
# conversation history.
#
# Usage:
#   from lib.memory import build_conversation_context
#   context = build_conversation_context(session_id)
#   llm_context = context.format_for_llm(detail_level="concise")
# =============================================================================

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from core.models.profile import DataProfile
from lib.supabase_client import SupabaseClient, SupabaseClientError

# Set up logging for this module
logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ChatMessage:
    """
    Represents a single chat message in conversation history.

    This is a simplified view of the chat_logs table row,
    containing only what's needed for context building.
    """
    role: str  # "user" or "assistant"
    content: str
    created_at: datetime | None = None
    node_id: str | None = None  # The node associated with this message

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "ChatMessage":
        """Create ChatMessage from a database row."""
        created_at = None
        if row.get("created_at"):
            # Parse ISO format timestamp
            try:
                created_at = datetime.fromisoformat(
                    row["created_at"].replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                pass

        return cls(
            role=row.get("role", "user"),
            content=row.get("content", ""),
            created_at=created_at,
            node_id=row.get("node_id"),
        )


@dataclass
class TransformationRecord:
    """
    Represents a transformation in the node history.

    Used to understand what transformations have been applied
    and support referential commands like "undo that" or "do the same for X".
    """
    node_id: str
    parent_id: str | None
    transformation: str | None  # Human-readable description
    transformation_code: str | None  # The actual code
    created_at: datetime | None = None
    row_count: int = 0
    column_count: int = 0

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "TransformationRecord":
        """Create TransformationRecord from a database row."""
        created_at = None
        if row.get("created_at"):
            try:
                created_at = datetime.fromisoformat(
                    row["created_at"].replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                pass

        return cls(
            node_id=row.get("id", ""),
            parent_id=row.get("parent_id"),
            transformation=row.get("transformation"),
            transformation_code=row.get("transformation_code"),
            created_at=created_at,
            row_count=row.get("row_count", 0),
            column_count=row.get("column_count", 0),
        )


@dataclass
class ConversationContext:
    """
    Complete context for the Strategist agent.

    Contains everything needed to understand the user's request:
    - Current data profile (what the data looks like)
    - Conversation history (what was discussed)
    - Recent transformations (what has been done)
    - Current node info (for undo/rollback)

    Example:
        context = build_conversation_context(session_id)

        # Get concise format for token efficiency
        llm_input = context.format_for_llm(detail_level="concise")

        # Get detailed format for complex decisions
        llm_input = context.format_for_llm(detail_level="detailed")
    """

    # Session identification
    session_id: str

    # Current data state
    current_node_id: str | None = None
    parent_node_id: str | None = None  # For undo support
    current_profile: DataProfile | None = None

    # Conversation history
    messages: list[ChatMessage] = field(default_factory=list)

    # Transformation history
    recent_transformations: list[TransformationRecord] = field(default_factory=list)

    # Additional metadata
    original_filename: str | None = None
    current_row_count: int = 0
    current_column_count: int = 0

    # -------------------------------------------------------------------------
    # Formatting Methods
    # -------------------------------------------------------------------------

    def format_for_llm(
        self,
        detail_level: Literal["concise", "detailed"] = "concise",
    ) -> str:
        """
        Format context as text for LLM consumption.

        Follows Anthropic's principle of response format control:
        - "concise": Minimal tokens, key info only
        - "detailed": Full context for complex decisions

        Args:
            detail_level: "concise" or "detailed"

        Returns:
            Formatted string optimized for LLM understanding

        Example output (concise):
            <data_profile>
            Dataset: 1,000 rows x 5 columns
            Columns: customer_id (ID), email (EMAIL), name (TEXT)...
            Issues: 3 data quality issues found
            </data_profile>

            <recent_messages>
            User: Remove rows where email is blank
            Assistant: Removed 15 rows with null email values
            </recent_messages>
        """
        sections = []

        # -----------------------------------------------------------------
        # Data Profile Section
        # -----------------------------------------------------------------
        sections.append(self._format_profile(detail_level))

        # -----------------------------------------------------------------
        # Recent Transformations Section
        # -----------------------------------------------------------------
        if self.recent_transformations:
            sections.append(self._format_transformations(detail_level))

        # -----------------------------------------------------------------
        # Conversation History Section
        # -----------------------------------------------------------------
        if self.messages:
            sections.append(self._format_messages(detail_level))

        # -----------------------------------------------------------------
        # Current State Section
        # -----------------------------------------------------------------
        sections.append(self._format_current_state())

        return "\n\n".join(sections)

    def _format_profile(self, detail_level: str) -> str:
        """Format the data profile section."""
        lines = ["<data_profile>"]

        if self.current_profile:
            if detail_level == "detailed":
                # Use the full text summary from DataProfile
                lines.append(self.current_profile.to_text_summary(verbose=True))
            else:
                # Concise summary
                lines.append(self.current_profile.to_text_summary(verbose=False))
        else:
            lines.append("No data profile available.")
            if self.current_row_count > 0:
                lines.append(f"Dataset: {self.current_row_count:,} rows x {self.current_column_count} columns")

        lines.append("</data_profile>")
        return "\n".join(lines)

    def _format_transformations(self, detail_level: str) -> str:
        """Format the recent transformations section."""
        lines = ["<recent_transformations>"]

        for t in self.recent_transformations:
            if t.transformation:
                if detail_level == "detailed" and t.transformation_code:
                    lines.append(f"- {t.transformation}")
                    lines.append(f"  Code: {t.transformation_code[:200]}...")
                else:
                    lines.append(f"- {t.transformation}")

        lines.append("</recent_transformations>")
        return "\n".join(lines)

    def _format_messages(self, detail_level: str) -> str:
        """Format the conversation history section."""
        lines = ["<recent_messages>"]

        # Limit messages based on detail level
        max_messages = 10 if detail_level == "detailed" else 5
        messages_to_show = self.messages[-max_messages:]

        for msg in messages_to_show:
            # Truncate long messages in concise mode
            content = msg.content
            if detail_level == "concise" and len(content) > 200:
                content = content[:200] + "..."

            role_label = "User" if msg.role == "user" else "Assistant"
            lines.append(f"{role_label}: {content}")

        lines.append("</recent_messages>")
        return "\n".join(lines)

    def _format_current_state(self) -> str:
        """Format the current state section for undo support."""
        lines = ["<current_state>"]

        if self.current_node_id:
            lines.append(f"current_node_id: {self.current_node_id}")

        if self.parent_node_id:
            lines.append(f"parent_node_id: {self.parent_node_id}")

        if self.original_filename:
            lines.append(f"original_file: {self.original_filename}")

        lines.append("</current_state>")
        return "\n".join(lines)

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def get_last_transformation(self) -> TransformationRecord | None:
        """Get the most recent transformation (for 'do the same' commands)."""
        if self.recent_transformations:
            return self.recent_transformations[-1]
        return None

    def get_mentioned_columns(self) -> list[str]:
        """
        Extract column names mentioned in recent messages.

        Useful for resolving "that column" references.
        """
        if not self.current_profile:
            return []

        column_names = [col.name for col in self.current_profile.columns]
        mentioned = []

        # Check recent messages for column mentions
        for msg in reversed(self.messages[-5:]):
            content_lower = msg.content.lower()
            for col_name in column_names:
                if col_name.lower() in content_lower:
                    if col_name not in mentioned:
                        mentioned.append(col_name)

        return mentioned

    def get_column_names(self) -> list[str]:
        """Get all column names from the current profile."""
        if not self.current_profile:
            return []
        return [col.name for col in self.current_profile.columns]


# =============================================================================
# Context Building Functions
# =============================================================================

def build_conversation_context(
    session_id: str | UUID,
    message_limit: int = 10,
    transformation_depth: int = 3,
) -> ConversationContext:
    """
    Build complete context for the Strategist agent.

    Fetches all necessary data from Supabase:
    1. Session info (filename, current node)
    2. Current node's profile
    3. Recent chat messages
    4. Recent transformation history

    Follows Anthropic's "Just-In-Time Retrieval" principle.

    Args:
        session_id: The session UUID
        message_limit: Max messages to fetch (default: 10)
        transformation_depth: How many transformations back to look (default: 3)

    Returns:
        ConversationContext ready for formatting

    Raises:
        SupabaseClientError: If database queries fail
        ContextBuildError: If context cannot be built

    Example:
        context = build_conversation_context(
            session_id="550e8400-...",
            message_limit=10,
            transformation_depth=3
        )
        llm_input = context.format_for_llm(detail_level="concise")
    """
    session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id
    logger.info(f"Building context for session {session_id_str}")

    # Initialize context with session_id
    context = ConversationContext(session_id=session_id_str)

    # -------------------------------------------------------------------------
    # 1. Fetch Session Info
    # -------------------------------------------------------------------------
    session = SupabaseClient.fetch_session(session_id_str)
    if session:
        context.original_filename = session.get("original_filename")
        context.current_node_id = session.get("current_node_id")
        logger.debug(f"Session found: {context.original_filename}, node: {context.current_node_id}")

    # -------------------------------------------------------------------------
    # 2. Fetch Current Node & Profile
    # -------------------------------------------------------------------------
    if context.current_node_id:
        current_node = SupabaseClient.fetch_node(context.current_node_id)
        if current_node:
            context.parent_node_id = current_node.get("parent_id")
            context.current_row_count = current_node.get("row_count", 0)
            context.current_column_count = current_node.get("column_count", 0)

            # Parse profile_json if available
            profile_json = current_node.get("profile_json")
            if profile_json:
                try:
                    context.current_profile = DataProfile.model_validate(profile_json)
                    logger.debug(f"Profile loaded: {context.current_row_count} rows, {context.current_column_count} cols")
                except Exception as e:
                    logger.warning(f"Failed to parse profile_json: {e}")

    # -------------------------------------------------------------------------
    # 3. Fetch Chat Messages
    # -------------------------------------------------------------------------
    try:
        messages_data = SupabaseClient.fetch_chat_messages(
            session_id_str,
            limit=message_limit,
            order="asc"  # Oldest first for chronological context
        )
        context.messages = [ChatMessage.from_db_row(m) for m in messages_data]
        logger.debug(f"Loaded {len(context.messages)} messages")
    except SupabaseClientError as e:
        logger.warning(f"Failed to fetch messages: {e}")

    # -------------------------------------------------------------------------
    # 4. Fetch Transformation History
    # -------------------------------------------------------------------------
    if context.current_node_id:
        try:
            lineage = SupabaseClient.fetch_node_lineage(
                context.current_node_id,
                depth=transformation_depth
            )
            context.recent_transformations = [
                TransformationRecord.from_db_row(n) for n in lineage
            ]
            logger.debug(f"Loaded {len(context.recent_transformations)} transformations")
        except SupabaseClientError as e:
            logger.warning(f"Failed to fetch lineage: {e}")

    return context


def format_messages_for_openai(
    messages: list[ChatMessage],
) -> list[dict[str, str]]:
    """
    Format chat messages for OpenAI's messages array.

    Converts our ChatMessage objects to the format expected
    by the OpenAI API.

    Args:
        messages: List of ChatMessage objects

    Returns:
        List of dicts with "role" and "content" keys

    Example:
        messages = [ChatMessage(role="user", content="Hello")]
        openai_format = format_messages_for_openai(messages)
        # [{"role": "user", "content": "Hello"}]
    """
    return [
        {"role": msg.role, "content": msg.content}
        for msg in messages
    ]


class ContextBuildError(Exception):
    """
    Error during context building.

    Provides actionable error messages following Anthropic's principle.
    """

    def __init__(
        self,
        message: str,
        code: str = "CONTEXT_BUILD_ERROR",
        suggestion: str | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.suggestion = suggestion

    def __str__(self) -> str:
        result = f"[{self.code}] {self.message}"
        if self.suggestion:
            result += f" Suggestion: {self.suggestion}"
        return result
