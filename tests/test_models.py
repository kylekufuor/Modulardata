# =============================================================================
# tests/test_models.py - Pydantic Model Tests
# =============================================================================
# Unit tests for all Pydantic models to ensure:
# - Valid data is accepted and parsed correctly
# - Invalid data raises ValidationError
# - Models serialize to JSON properly
# - Default values work as expected
#
# Run with: poetry run pytest tests/test_models.py -v
# =============================================================================

from datetime import datetime
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

# Import all models from the core package
from core.models import (
    ChatMessage,
    ChatRequest,
    ChatResponse,
    ColumnProfile,
    ConversationHistory,
    DataProfile,
    MessageRole,
    NodeCreate,
    NodeHistory,
    NodeResponse,
    ProfileSummary,
    RollbackRequest,
    RollbackResponse,
    SessionCreate,
    SessionList,
    SessionResponse,
    SessionStatus,
    SessionUpdate,
    TaskError,
    TaskResult,
    TaskState,
    TaskStatus,
)


# =============================================================================
# Profile Model Tests
# =============================================================================

class TestColumnProfile:
    """Tests for ColumnProfile model."""

    def test_valid_column_profile(self):
        """Test creating a valid ColumnProfile."""
        # Arrange: Define valid column data
        data = {
            "name": "age",
            "dtype": "int64",
            "null_count": 50,
            "null_percent": 5.0,
            "unique_count": 75,
            "sample_values": [25, 34, 42],
        }

        # Act: Create the model
        profile = ColumnProfile(**data)

        # Assert: Values are correct
        assert profile.name == "age"
        assert profile.dtype == "int64"
        assert profile.null_count == 50
        assert profile.null_percent == 5.0

    def test_column_profile_defaults(self):
        """Test that defaults are applied correctly."""
        # Only required fields
        profile = ColumnProfile(name="email", dtype="object")

        # Check defaults
        assert profile.null_count == 0
        assert profile.null_percent == 0.0
        assert profile.unique_count == 0
        assert profile.sample_values == []

    def test_null_percent_validation(self):
        """Test that null_percent must be between 0 and 100."""
        # Should reject values over 100
        with pytest.raises(ValidationError):
            ColumnProfile(name="test", dtype="int64", null_percent=150.0)

        # Should reject negative values
        with pytest.raises(ValidationError):
            ColumnProfile(name="test", dtype="int64", null_percent=-5.0)


class TestDataProfile:
    """Tests for DataProfile model."""

    def test_valid_data_profile(self):
        """Test creating a valid DataProfile."""
        data = {
            "row_count": 1000,
            "column_count": 5,
            "columns": [
                {"name": "id", "dtype": "int64"},
                {"name": "name", "dtype": "object"},
            ],
            "sample_rows": [
                {"id": 1, "name": "John"},
                {"id": 2, "name": "Jane"},
            ],
        }

        profile = DataProfile(**data)

        assert profile.row_count == 1000
        assert profile.column_count == 5
        assert len(profile.columns) == 2

    def test_to_text_summary(self):
        """Test the text summary generation."""
        profile = DataProfile(
            row_count=100,
            column_count=2,
            columns=[
                ColumnProfile(
                    name="age",
                    dtype="int64",
                    null_count=10,
                    null_percent=10.0,
                    sample_values=[25, 30, 35],
                ),
            ],
            sample_rows=[{"age": 25}],
        )

        summary = profile.to_text_summary()

        # Check key parts are in the summary
        assert "Rows: 100" in summary
        assert "age" in summary
        assert "10.0%" in summary


# =============================================================================
# Session Model Tests
# =============================================================================

class TestSessionModels:
    """Tests for Session-related models."""

    def test_session_create(self):
        """Test SessionCreate model."""
        session = SessionCreate(original_filename="data.csv")
        assert session.original_filename == "data.csv"

    def test_session_create_empty_filename_fails(self):
        """Test that empty filename is rejected."""
        with pytest.raises(ValidationError):
            SessionCreate(original_filename="")

    def test_session_response(self):
        """Test SessionResponse model."""
        session_id = uuid4()
        node_id = uuid4()

        response = SessionResponse(
            id=session_id,
            created_at=datetime.now(),
            original_filename="test.csv",
            status=SessionStatus.ACTIVE,
            current_node_id=node_id,
        )

        assert response.id == session_id
        assert response.status == SessionStatus.ACTIVE

    def test_session_status_enum(self):
        """Test SessionStatus enum values."""
        assert SessionStatus.ACTIVE.value == "active"
        assert SessionStatus.ARCHIVED.value == "archived"

    def test_session_list(self):
        """Test SessionList pagination model."""
        session_list = SessionList(
            sessions=[],
            total=0,
            page=1,
            page_size=10,
        )

        assert session_list.total == 0
        assert session_list.page == 1


# =============================================================================
# Node Model Tests
# =============================================================================

class TestNodeModels:
    """Tests for Node-related models."""

    def test_node_create(self):
        """Test NodeCreate model."""
        session_id = uuid4()
        parent_id = uuid4()

        node = NodeCreate(
            session_id=session_id,
            parent_id=parent_id,
            storage_path="sessions/abc/node_1.csv",
            row_count=500,
            column_count=5,
            transformation="Dropped rows with null ages",
        )

        assert node.session_id == session_id
        assert node.parent_id == parent_id
        assert node.row_count == 500

    def test_node_create_root_node(self):
        """Test creating Node 0 (no parent)."""
        node = NodeCreate(
            session_id=uuid4(),
            parent_id=None,  # Root node has no parent
            storage_path="sessions/abc/node_0.csv",
            row_count=1000,
            column_count=10,
            transformation=None,  # Original upload
        )

        assert node.parent_id is None
        assert node.transformation is None

    def test_rollback_request(self):
        """Test RollbackRequest model."""
        request = RollbackRequest(
            session_id=uuid4(),
            target_node_id=uuid4(),
        )

        assert request.session_id is not None
        assert request.target_node_id is not None


# =============================================================================
# Chat Model Tests
# =============================================================================

class TestChatModels:
    """Tests for Chat-related models."""

    def test_chat_request(self):
        """Test ChatRequest model."""
        request = ChatRequest(
            session_id=uuid4(),
            message="Remove all rows where age is blank",
        )

        assert "age" in request.message

    def test_chat_request_message_length(self):
        """Test message length validation."""
        # Empty message should fail
        with pytest.raises(ValidationError):
            ChatRequest(session_id=uuid4(), message="")

        # Very long message should fail (over 2000 chars)
        with pytest.raises(ValidationError):
            ChatRequest(session_id=uuid4(), message="x" * 2001)

    def test_chat_response(self):
        """Test ChatResponse model."""
        response = ChatResponse(
            task_id="task_abc123",
            status=TaskState.QUEUED,
            session_id=uuid4(),
        )

        assert response.task_id == "task_abc123"
        assert response.status == TaskState.QUEUED

    def test_task_states(self):
        """Test TaskState enum values."""
        assert TaskState.QUEUED.value == "queued"
        assert TaskState.PROCESSING.value == "processing"
        assert TaskState.DONE.value == "done"
        assert TaskState.FAILED.value == "failed"

    def test_task_status_with_result(self):
        """Test TaskStatus with completed result."""
        result = TaskResult(
            node_id=uuid4(),
            transformation="Removed blank rows",
            rows_before=1000,
            rows_after=950,
            rows_affected=50,
            assistant_message="I removed 50 rows with blank values.",
        )

        status = TaskStatus(
            task_id="task_123",
            status=TaskState.DONE,
            progress="Complete",
            result=result,
        )

        assert status.status == TaskState.DONE
        assert status.result.rows_affected == 50

    def test_task_status_with_error(self):
        """Test TaskStatus with error."""
        error = TaskError(
            code="INVALID_COLUMN",
            message="Column 'eamil' not found",
            details={"suggested": "email"},
        )

        status = TaskStatus(
            task_id="task_456",
            status=TaskState.FAILED,
            progress="Failed",
            error=error,
        )

        assert status.status == TaskState.FAILED
        assert status.error.code == "INVALID_COLUMN"

    def test_chat_message(self):
        """Test ChatMessage model."""
        message = ChatMessage(
            id=uuid4(),
            role=MessageRole.USER,
            content="Hello",
            created_at=datetime.now(),
        )

        assert message.role == MessageRole.USER

    def test_message_roles(self):
        """Test MessageRole enum."""
        assert MessageRole.USER.value == "user"
        assert MessageRole.ASSISTANT.value == "assistant"


# =============================================================================
# Serialization Tests
# =============================================================================

class TestSerialization:
    """Tests for JSON serialization."""

    def test_session_response_to_json(self):
        """Test that SessionResponse serializes to JSON correctly."""
        response = SessionResponse(
            id=uuid4(),
            created_at=datetime.now(),
            original_filename="test.csv",
            status=SessionStatus.ACTIVE,
        )

        # Convert to dict (which is what JSON serialization does)
        data = response.model_dump(mode="json")

        # UUID should be string in JSON
        assert isinstance(data["id"], str)
        # Enum should be its value
        assert data["status"] == "active"

    def test_data_profile_to_json(self):
        """Test DataProfile JSON serialization."""
        profile = DataProfile(
            row_count=100,
            column_count=2,
            columns=[
                ColumnProfile(name="id", dtype="int64"),
            ],
        )

        data = profile.model_dump(mode="json")

        assert data["row_count"] == 100
        assert isinstance(data["columns"], list)
