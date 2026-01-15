# =============================================================================
# tests/test_memory.py - Memory Module Tests
# =============================================================================
# This module contains tests for:
# - ConversationContext formatting
# - Context building with mocked Supabase
# - Message formatting for OpenAI
#
# Tests use mocked Supabase responses to avoid database calls.
# =============================================================================

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from lib.memory import (
    ConversationContext,
    ChatMessage,
    TransformationRecord,
    build_conversation_context,
    format_messages_for_openai,
    ContextBuildError,
)


# =============================================================================
# ChatMessage Tests
# =============================================================================

class TestChatMessage:
    """Test ChatMessage dataclass."""

    def test_from_db_row(self):
        """Test creating ChatMessage from database row."""
        row = {
            "id": "msg-123",
            "role": "user",
            "content": "Remove null emails",
            "created_at": "2024-01-15T10:30:00Z",
            "node_id": "node-456",
        }

        msg = ChatMessage.from_db_row(row)

        assert msg.role == "user"
        assert msg.content == "Remove null emails"
        assert msg.node_id == "node-456"
        assert msg.created_at is not None

    def test_from_db_row_missing_fields(self):
        """Test ChatMessage with missing optional fields."""
        row = {
            "role": "assistant",
            "content": "Done!",
        }

        msg = ChatMessage.from_db_row(row)

        assert msg.role == "assistant"
        assert msg.content == "Done!"
        assert msg.created_at is None
        assert msg.node_id is None


# =============================================================================
# TransformationRecord Tests
# =============================================================================

class TestTransformationRecord:
    """Test TransformationRecord dataclass."""

    def test_from_db_row(self):
        """Test creating TransformationRecord from database row."""
        row = {
            "id": "node-123",
            "parent_id": "node-122",
            "transformation": "Dropped null emails",
            "transformation_code": "df = df.dropna(subset=['email'])",
            "created_at": "2024-01-15T10:30:00Z",
            "row_count": 950,
            "column_count": 5,
        }

        record = TransformationRecord.from_db_row(row)

        assert record.node_id == "node-123"
        assert record.parent_id == "node-122"
        assert record.transformation == "Dropped null emails"
        assert record.row_count == 950


# =============================================================================
# ConversationContext Tests
# =============================================================================

class TestConversationContext:
    """Test ConversationContext dataclass."""

    @pytest.fixture
    def sample_context(self):
        """Create a sample ConversationContext for testing."""
        return ConversationContext(
            session_id="test-session",
            current_node_id="node-123",
            parent_node_id="node-122",
            current_row_count=1000,
            current_column_count=5,
            original_filename="sales_data.csv",
            messages=[
                ChatMessage(role="user", content="Remove rows where email is blank"),
                ChatMessage(role="assistant", content="Removed 50 rows with null emails"),
            ],
            recent_transformations=[
                TransformationRecord(
                    node_id="node-122",
                    parent_id="node-121",
                    transformation="Original upload",
                    transformation_code=None,
                ),
                TransformationRecord(
                    node_id="node-123",
                    parent_id="node-122",
                    transformation="Dropped null emails",
                    transformation_code="df = df.dropna(subset=['email'])",
                ),
            ],
        )

    def test_format_for_llm_concise(self, sample_context):
        """Test concise formatting for LLM."""
        output = sample_context.format_for_llm(detail_level="concise")

        # Check XML tags present
        assert "<data_profile>" in output
        assert "</data_profile>" in output
        assert "<recent_messages>" in output
        assert "</recent_messages>" in output
        assert "<current_state>" in output

        # Check content is included
        assert "User:" in output
        assert "Assistant:" in output

    def test_format_for_llm_detailed(self, sample_context):
        """Test detailed formatting includes more info."""
        output = sample_context.format_for_llm(detail_level="detailed")

        assert "<data_profile>" in output
        assert "<recent_transformations>" in output

    def test_format_includes_current_state(self, sample_context):
        """Test that current state is included for undo support."""
        output = sample_context.format_for_llm(detail_level="concise")

        assert "current_node_id" in output
        assert "parent_node_id" in output
        assert "node-123" in output
        assert "node-122" in output

    def test_get_last_transformation(self, sample_context):
        """Test getting the last transformation."""
        last = sample_context.get_last_transformation()

        assert last is not None
        assert last.transformation == "Dropped null emails"

    def test_get_last_transformation_empty(self):
        """Test getting last transformation when empty."""
        context = ConversationContext(session_id="test")
        assert context.get_last_transformation() is None

    def test_get_column_names_no_profile(self):
        """Test get_column_names without profile."""
        context = ConversationContext(session_id="test")
        assert context.get_column_names() == []


# =============================================================================
# Format Messages Tests
# =============================================================================

class TestFormatMessages:
    """Test message formatting for OpenAI."""

    def test_format_messages_for_openai(self):
        """Test converting ChatMessages to OpenAI format."""
        messages = [
            ChatMessage(role="user", content="Hello"),
            ChatMessage(role="assistant", content="Hi there!"),
            ChatMessage(role="user", content="Do something"),
        ]

        formatted = format_messages_for_openai(messages)

        assert len(formatted) == 3
        assert formatted[0]["role"] == "user"
        assert formatted[0]["content"] == "Hello"
        assert formatted[1]["role"] == "assistant"
        assert formatted[2]["role"] == "user"

    def test_format_messages_empty(self):
        """Test formatting empty message list."""
        formatted = format_messages_for_openai([])
        assert formatted == []


# =============================================================================
# Context Building Tests (Mocked Supabase)
# =============================================================================

class TestBuildConversationContext:
    """Test context building with mocked Supabase."""

    @pytest.fixture
    def mock_supabase(self):
        """Create mocked SupabaseClient methods."""
        with patch("lib.memory.SupabaseClient") as mock:
            # Setup return values
            mock.fetch_session.return_value = {
                "id": "session-123",
                "current_node_id": "node-456",
                "original_filename": "test.csv",
            }

            mock.fetch_node.return_value = {
                "id": "node-456",
                "parent_id": "node-455",
                "row_count": 1000,
                "column_count": 5,
                "profile_json": None,  # Simplified for testing
            }

            mock.fetch_chat_messages.return_value = [
                {"role": "user", "content": "Test message"},
            ]

            mock.fetch_node_lineage.return_value = [
                {"id": "node-455", "transformation": "Original"},
                {"id": "node-456", "transformation": "Transform 1"},
            ]

            yield mock

    def test_build_context_fetches_session(self, mock_supabase):
        """Test that context building fetches session."""
        context = build_conversation_context("session-123")

        mock_supabase.fetch_session.assert_called_once_with("session-123")
        assert context.session_id == "session-123"
        assert context.original_filename == "test.csv"

    def test_build_context_fetches_messages(self, mock_supabase):
        """Test that context building fetches chat messages."""
        context = build_conversation_context("session-123", message_limit=10)

        mock_supabase.fetch_chat_messages.assert_called_once()
        assert len(context.messages) == 1

    def test_build_context_fetches_lineage(self, mock_supabase):
        """Test that context building fetches node lineage."""
        context = build_conversation_context("session-123")

        mock_supabase.fetch_node_lineage.assert_called_once()
        assert len(context.recent_transformations) == 2

    def test_build_context_handles_missing_session(self, mock_supabase):
        """Test handling when session doesn't exist."""
        mock_supabase.fetch_session.return_value = None

        context = build_conversation_context("nonexistent-session")

        assert context.session_id == "nonexistent-session"
        assert context.current_node_id is None


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestContextBuildError:
    """Test ContextBuildError exception."""

    def test_error_with_suggestion(self):
        """Test error includes suggestion."""
        error = ContextBuildError(
            message="Session not found",
            code="SESSION_NOT_FOUND",
            suggestion="Check that the session ID is correct",
        )

        error_str = str(error)
        assert "SESSION_NOT_FOUND" in error_str
        assert "Session not found" in error_str
        assert "Check that the session ID" in error_str

    def test_error_without_suggestion(self):
        """Test error works without suggestion."""
        error = ContextBuildError(
            message="Unknown error",
            code="UNKNOWN",
        )

        error_str = str(error)
        assert "UNKNOWN" in error_str
        assert "Suggestion" not in error_str
