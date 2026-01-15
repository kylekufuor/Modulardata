# =============================================================================
# tests/test_chat_modes.py - Chat Mode Tests
# =============================================================================
# Tests for plan mode and transform mode functionality.
# =============================================================================

from __future__ import annotations

from unittest.mock import MagicMock, patch
import json

import pytest

from core.models.chat import ChatMode, ChatRequest, PlanResponse
from agents.models.technical_plan import (
    TechnicalPlan,
    TransformationType,
    ColumnTarget,
    FilterCondition,
    FilterOperator,
)
from agents.chat_handler import (
    handle_chat_request,
    preview_transformation,
    chat,
    _build_assistant_message,
    _handle_plan_mode,
)


# =============================================================================
# ChatMode Enum Tests
# =============================================================================

class TestChatMode:
    """Test ChatMode enum."""

    def test_plan_mode_value(self):
        """Test plan mode enum value."""
        assert ChatMode.PLAN.value == "plan"

    def test_transform_mode_value(self):
        """Test transform mode enum value."""
        assert ChatMode.TRANSFORM.value == "transform"

    def test_mode_from_string(self):
        """Test creating mode from string."""
        assert ChatMode("plan") == ChatMode.PLAN
        assert ChatMode("transform") == ChatMode.TRANSFORM


# =============================================================================
# ChatRequest Tests
# =============================================================================

class TestChatRequest:
    """Test ChatRequest model with mode."""

    def test_default_mode_is_plan(self):
        """Test that default mode is plan."""
        request = ChatRequest(
            session_id="550e8400-e29b-41d4-a716-446655440000",
            message="remove blank rows",
        )
        assert request.mode == ChatMode.PLAN

    def test_explicit_plan_mode(self):
        """Test setting plan mode explicitly."""
        request = ChatRequest(
            session_id="550e8400-e29b-41d4-a716-446655440000",
            message="remove blank rows",
            mode=ChatMode.PLAN,
        )
        assert request.mode == ChatMode.PLAN

    def test_transform_mode(self):
        """Test setting transform mode."""
        request = ChatRequest(
            session_id="550e8400-e29b-41d4-a716-446655440000",
            message="remove blank rows",
            mode=ChatMode.TRANSFORM,
        )
        assert request.mode == ChatMode.TRANSFORM

    def test_mode_from_string(self):
        """Test setting mode from string."""
        request = ChatRequest(
            session_id="550e8400-e29b-41d4-a716-446655440000",
            message="remove blank rows",
            mode="transform",
        )
        assert request.mode == ChatMode.TRANSFORM


# =============================================================================
# PlanResponse Tests
# =============================================================================

class TestPlanResponse:
    """Test PlanResponse model."""

    def test_plan_response_structure(self):
        """Test PlanResponse has expected fields."""
        response = PlanResponse(
            session_id="550e8400-e29b-41d4-a716-446655440000",
            plan={"transformation_type": "drop_rows"},
            assistant_message="I'll remove rows where email is blank",
        )

        assert response.mode == ChatMode.PLAN
        assert response.plan["transformation_type"] == "drop_rows"
        assert response.can_execute is True
        assert response.clarification_needed is None

    def test_plan_response_with_clarification(self):
        """Test PlanResponse when clarification is needed."""
        response = PlanResponse(
            session_id="550e8400-e29b-41d4-a716-446655440000",
            plan={"transformation_type": "drop_rows"},
            assistant_message="I need more information",
            can_execute=False,
            clarification_needed="Which column did you mean?",
        )

        assert response.can_execute is False
        assert "Which column" in response.clarification_needed


# =============================================================================
# Chat Handler Tests
# =============================================================================

class TestChatHandler:
    """Test chat handler functions."""

    @pytest.fixture
    def mock_plan(self):
        """Create a mock TechnicalPlan."""
        return TechnicalPlan(
            transformation_type=TransformationType.DROP_ROWS,
            target_columns=[ColumnTarget(column_name="email")],
            conditions=[FilterCondition(column="email", operator=FilterOperator.ISNULL)],
            explanation="Remove rows where email is null",
            confidence=0.95,
        )

    @pytest.fixture
    def mock_agent(self, mock_plan):
        """Create a mock StrategistAgent."""
        with patch("agents.chat_handler.StrategistAgent") as mock:
            instance = MagicMock()
            instance.create_plan.return_value = mock_plan
            mock.return_value = instance
            yield mock

    def test_handle_plan_mode(self, mock_agent, mock_plan):
        """Test handling request in plan mode."""
        response = handle_chat_request(
            session_id="550e8400-e29b-41d4-a716-446655440000",
            message="remove blank emails",
            mode=ChatMode.PLAN,
        )

        assert isinstance(response, PlanResponse)
        assert response.mode == ChatMode.PLAN
        assert response.can_execute is True

    def test_handle_transform_mode_not_implemented(self, mock_agent, mock_plan):
        """Test that transform mode raises NotImplementedError."""
        with pytest.raises(NotImplementedError) as exc_info:
            handle_chat_request(
                session_id="550e8400-e29b-41d4-a716-446655440000",
                message="remove blank emails",
                mode=ChatMode.TRANSFORM,
            )

        assert "Engineer" in str(exc_info.value)

    def test_preview_transformation(self, mock_agent, mock_plan):
        """Test preview_transformation convenience function."""
        response = preview_transformation(
            session_id="550e8400-e29b-41d4-a716-446655440000",
            message="remove blank emails",
        )

        assert isinstance(response, PlanResponse)
        assert response.mode == ChatMode.PLAN

    def test_chat_preview(self, mock_agent, mock_plan):
        """Test chat function in preview mode."""
        response = chat(
            session_id="550e8400-e29b-41d4-a716-446655440000",
            message="remove blank emails",
            execute=False,
        )

        assert isinstance(response, PlanResponse)

    def test_chat_execute_not_implemented(self, mock_agent, mock_plan):
        """Test chat function in execute mode raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            chat(
                session_id="550e8400-e29b-41d4-a716-446655440000",
                message="remove blank emails",
                execute=True,
            )


# =============================================================================
# Assistant Message Tests
# =============================================================================

class TestBuildAssistantMessage:
    """Test assistant message generation."""

    def test_undo_message(self):
        """Test message for undo operation."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.UNDO,
            explanation="Undo",
            rollback_to_node_id="parent-node",
        )

        message = _build_assistant_message(plan)
        assert "undo" in message.lower()
        assert "restore" in message.lower()

    def test_drop_rows_message(self):
        """Test message for drop_rows operation."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.DROP_ROWS,
            target_columns=[ColumnTarget(column_name="email")],
            explanation="Drop null emails",
            confidence=0.95,
        )

        message = _build_assistant_message(plan)
        assert "remove" in message.lower()
        assert "email" in message.lower()

    def test_low_confidence_includes_percentage(self):
        """Test that low confidence shows percentage."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.DROP_ROWS,
            target_columns=[ColumnTarget(column_name="email")],
            explanation="Drop emails",
            confidence=0.7,
        )

        message = _build_assistant_message(plan)
        assert "70%" in message


# =============================================================================
# Handle Plan Mode Tests
# =============================================================================

class TestHandlePlanMode:
    """Test _handle_plan_mode function."""

    def test_high_confidence_can_execute(self):
        """Test that high confidence plans can execute."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.DROP_ROWS,
            target_columns=[ColumnTarget(column_name="email")],
            explanation="Drop null emails",
            confidence=0.95,
        )

        response = _handle_plan_mode("550e8400-e29b-41d4-a716-446655440000", plan)

        assert response.can_execute is True
        assert response.clarification_needed is None

    def test_low_confidence_needs_clarification(self):
        """Test that low confidence plans need clarification."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.DROP_ROWS,
            target_columns=[ColumnTarget(column_name="email")],
            explanation="Drop emails",
            confidence=0.5,
            clarification_needed="Which column did you mean?",
        )

        response = _handle_plan_mode("550e8400-e29b-41d4-a716-446655440000", plan)

        assert response.can_execute is False
        assert "Which column" in response.clarification_needed

    def test_includes_estimated_impact(self):
        """Test that estimated impact is included when conditions exist."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.DROP_ROWS,
            target_columns=[ColumnTarget(column_name="email")],
            conditions=[FilterCondition(column="email", operator=FilterOperator.ISNULL)],
            explanation="Drop null emails",
            confidence=0.95,
        )

        response = _handle_plan_mode("550e8400-e29b-41d4-a716-446655440000", plan)

        assert response.estimated_impact is not None
        assert "condition" in response.estimated_impact["description"].lower()
