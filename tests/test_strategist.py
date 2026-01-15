# =============================================================================
# tests/test_strategist.py - Strategist Agent Tests
# =============================================================================
# This module contains tests for:
# - TechnicalPlan schema validation
# - StrategistAgent logic (with mocked OpenAI)
# - Referential understanding (undo, that column, do same)
# - Error handling (actionable messages)
#
# Tests use mocked OpenAI responses to avoid API costs.
# =============================================================================

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from agents.models.technical_plan import (
    TechnicalPlan,
    TransformationType,
    ColumnTarget,
    FilterCondition,
    FilterOperator,
    CaseType,
    FillMethod,
)
from agents.strategist import StrategistAgent, StrategyError


# =============================================================================
# TechnicalPlan Schema Tests
# =============================================================================

class TestTechnicalPlanSchema:
    """Test TechnicalPlan Pydantic validation."""

    def test_valid_drop_rows_plan(self):
        """Test creating a valid drop_rows plan."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.DROP_ROWS,
            target_columns=[ColumnTarget(column_name="email")],
            conditions=[
                FilterCondition(
                    column="email",
                    operator=FilterOperator.ISNULL,
                )
            ],
            explanation="Remove rows where email is null",
        )

        assert plan.transformation_type == TransformationType.DROP_ROWS
        assert len(plan.target_columns) == 1
        assert plan.target_columns[0].column_name == "email"
        assert len(plan.conditions) == 1
        assert plan.conditions[0].operator == FilterOperator.ISNULL
        assert plan.confidence == 1.0  # Default

    def test_valid_fill_nulls_plan(self):
        """Test creating a valid fill_nulls plan with parameters."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.FILL_NULLS,
            target_columns=[ColumnTarget(column_name="price")],
            parameters={"method": "mean"},
            explanation="Fill null prices with mean value",
            confidence=0.9,
        )

        assert plan.transformation_type == TransformationType.FILL_NULLS
        assert plan.parameters["method"] == "mean"
        assert plan.confidence == 0.9

    def test_valid_undo_plan(self):
        """Test creating a valid undo plan."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.UNDO,
            explanation="Rollback to previous version",
            rollback_to_node_id="550e8400-e29b-41d4-a716-446655440000",
        )

        assert plan.is_undo()
        assert plan.rollback_to_node_id == "550e8400-e29b-41d4-a716-446655440000"

    def test_valid_standardize_plan(self):
        """Test creating a standardize plan with multiple operations."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.STANDARDIZE,
            target_columns=[ColumnTarget(column_name="customer_name")],
            parameters={"operations": ["trim_whitespace", "title_case"]},
            explanation="Standardize customer names",
        )

        assert plan.transformation_type == TransformationType.STANDARDIZE
        assert "trim_whitespace" in plan.parameters["operations"]

    def test_missing_transformation_type_fails(self):
        """Test that missing transformation_type raises error."""
        with pytest.raises(Exception):
            TechnicalPlan(
                explanation="Missing type",
            )

    def test_missing_explanation_fails(self):
        """Test that missing explanation raises error."""
        with pytest.raises(Exception):
            TechnicalPlan(
                transformation_type=TransformationType.DROP_ROWS,
            )

    def test_confidence_bounds(self):
        """Test that confidence is bounded between 0 and 1."""
        # Above 1.0 should raise validation error
        with pytest.raises(Exception):
            TechnicalPlan(
                transformation_type=TransformationType.DROP_ROWS,
                explanation="Test",
                confidence=1.5,
            )

        # Below 0.0 should raise validation error
        with pytest.raises(Exception):
            TechnicalPlan(
                transformation_type=TransformationType.DROP_ROWS,
                explanation="Test",
                confidence=-0.5,
            )

        # Valid range should work
        plan = TechnicalPlan(
            transformation_type=TransformationType.DROP_ROWS,
            explanation="Test",
            confidence=0.75,
        )
        assert plan.confidence == 0.75

    def test_needs_clarification(self):
        """Test needs_clarification helper method."""
        # Low confidence with clarification
        plan = TechnicalPlan(
            transformation_type=TransformationType.DROP_ROWS,
            explanation="Test",
            confidence=0.5,
            clarification_needed="Which column did you mean?",
        )
        assert plan.needs_clarification()

        # High confidence - no clarification needed
        plan = TechnicalPlan(
            transformation_type=TransformationType.DROP_ROWS,
            explanation="Test",
            confidence=0.9,
        )
        assert not plan.needs_clarification()

    def test_get_affected_columns(self):
        """Test get_affected_columns includes both targets and conditions."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.DROP_ROWS,
            target_columns=[ColumnTarget(column_name="email")],
            conditions=[
                FilterCondition(column="email", operator=FilterOperator.ISNULL),
                FilterCondition(column="name", operator=FilterOperator.NOTNULL),
            ],
            explanation="Test",
        )

        affected = plan.get_affected_columns()
        assert "email" in affected
        assert "name" in affected

    def test_to_engineer_prompt(self):
        """Test formatting plan for Engineer agent."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.DROP_ROWS,
            target_columns=[ColumnTarget(column_name="email")],
            conditions=[
                FilterCondition(column="email", operator=FilterOperator.ISNULL),
            ],
            explanation="Remove null emails",
        )

        prompt = plan.to_engineer_prompt()
        assert "drop_rows" in prompt
        assert "email" in prompt
        assert "Remove null emails" in prompt


# =============================================================================
# FilterCondition Tests
# =============================================================================

class TestFilterCondition:
    """Test FilterCondition model."""

    def test_valid_isnull_condition(self):
        """Test creating isnull condition (no value needed)."""
        cond = FilterCondition(
            column="email",
            operator=FilterOperator.ISNULL,
        )
        assert cond.column == "email"
        assert cond.operator == FilterOperator.ISNULL
        assert cond.value is None

    def test_valid_equals_condition(self):
        """Test creating equals condition with value."""
        cond = FilterCondition(
            column="status",
            operator=FilterOperator.EQ,
            value="active",
        )
        assert cond.column == "status"
        assert cond.operator == FilterOperator.EQ
        assert cond.value == "active"

    def test_valid_in_condition(self):
        """Test creating IN condition with list value."""
        cond = FilterCondition(
            column="status",
            operator=FilterOperator.IN,
            value=["active", "pending"],
        )
        assert cond.operator == FilterOperator.IN
        assert "active" in cond.value

    def test_valid_contains_condition(self):
        """Test creating contains condition."""
        cond = FilterCondition(
            column="name",
            operator=FilterOperator.CONTAINS,
            value="Smith",
            case_sensitive=False,
        )
        assert cond.operator == FilterOperator.CONTAINS
        assert not cond.case_sensitive


# =============================================================================
# ColumnTarget Tests
# =============================================================================

class TestColumnTarget:
    """Test ColumnTarget model."""

    def test_single_column(self):
        """Test single column target."""
        target = ColumnTarget(column_name="email")
        assert target.column_name == "email"
        assert target.secondary_column is None

    def test_two_columns(self):
        """Test two-column target (for merge operations)."""
        target = ColumnTarget(
            column_name="first_name",
            secondary_column="last_name",
        )
        assert target.column_name == "first_name"
        assert target.secondary_column == "last_name"

    def test_empty_column_name_fails(self):
        """Test that empty column name raises error."""
        with pytest.raises(Exception):
            ColumnTarget(column_name="")


# =============================================================================
# StrategistAgent Tests (Mocked OpenAI)
# =============================================================================

class TestStrategistAgent:
    """Test StrategistAgent with mocked OpenAI responses."""

    @pytest.fixture
    def mock_openai_response(self):
        """Create a mock OpenAI response."""
        def _create_mock(response_json: dict):
            mock_response = MagicMock()
            mock_response.choices = [MagicMock()]
            mock_response.choices[0].message.content = json.dumps(response_json)
            return mock_response
        return _create_mock

    @pytest.fixture
    def mock_context(self):
        """Create a mock ConversationContext."""
        from lib.memory import ConversationContext
        return ConversationContext(
            session_id="test-session-id",
            current_node_id="current-node-id",
            parent_node_id="parent-node-id",
            current_row_count=100,
            current_column_count=5,
        )

    def test_parse_valid_response(self, mock_openai_response):
        """Test parsing a valid JSON response."""
        agent = StrategistAgent.__new__(StrategistAgent)

        response_json = {
            "transformation_type": "drop_rows",
            "target_columns": [{"column_name": "email"}],
            "conditions": [{"column": "email", "operator": "isnull"}],
            "parameters": {},
            "explanation": "Remove null emails",
            "confidence": 0.95,
        }

        plan = agent._parse_response(json.dumps(response_json))

        assert plan.transformation_type == TransformationType.DROP_ROWS
        assert plan.confidence == 0.95

    def test_parse_invalid_json_raises_error(self):
        """Test that invalid JSON raises StrategyError."""
        agent = StrategistAgent.__new__(StrategistAgent)

        with pytest.raises(StrategyError) as exc_info:
            agent._parse_response("not valid json")

        assert exc_info.value.code == "JSON_PARSE_ERROR"
        assert exc_info.value.suggestion is not None

    def test_parse_missing_fields_raises_error(self):
        """Test that missing required fields raises StrategyError."""
        agent = StrategistAgent.__new__(StrategistAgent)

        # Missing explanation
        invalid_json = json.dumps({
            "transformation_type": "drop_rows",
        })

        with pytest.raises(StrategyError) as exc_info:
            agent._parse_response(invalid_json)

        assert exc_info.value.code == "VALIDATION_ERROR"

    def test_resolve_undo_sets_node_id(self, mock_context):
        """Test that _resolve_undo sets rollback_to_node_id."""
        agent = StrategistAgent.__new__(StrategistAgent)

        plan = TechnicalPlan(
            transformation_type=TransformationType.UNDO,
            explanation="Undo last change",
        )

        resolved = agent._resolve_undo(plan, mock_context)

        assert resolved.rollback_to_node_id == "parent-node-id"

    def test_resolve_undo_no_parent(self):
        """Test _resolve_undo when no parent exists."""
        from lib.memory import ConversationContext

        agent = StrategistAgent.__new__(StrategistAgent)

        context = ConversationContext(
            session_id="test-session",
            current_node_id="first-node",
            parent_node_id=None,  # No parent
        )

        plan = TechnicalPlan(
            transformation_type=TransformationType.UNDO,
            explanation="Undo",
        )

        resolved = agent._resolve_undo(plan, context)

        # Should have low confidence and clarification
        assert resolved.confidence < 0.5
        assert resolved.clarification_needed is not None
        assert "Cannot undo" in resolved.clarification_needed

    def test_find_similar_columns(self):
        """Test finding similar column names."""
        agent = StrategistAgent.__new__(StrategistAgent)

        columns = ["customer_email", "customer_name", "order_date", "total_amount"]

        # Substring match
        suggestions = agent._find_similar_columns("email", columns)
        assert "customer_email" in suggestions

        # Word overlap
        suggestions = agent._find_similar_columns("customer", columns)
        assert "customer_email" in suggestions
        assert "customer_name" in suggestions


# =============================================================================
# Referential Understanding Tests
# =============================================================================

class TestReferentialUnderstanding:
    """Test referential command handling."""

    def test_undo_plan_structure(self):
        """Test that undo plan has correct structure."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.UNDO,
            explanation="Undo last transformation",
            confidence=1.0,
            rollback_to_node_id="parent-node-uuid",
        )

        assert plan.is_undo()
        assert plan.rollback_to_node_id is not None
        assert len(plan.target_columns) == 0  # Undo has no targets

    def test_low_confidence_triggers_clarification(self):
        """Test that low confidence plans trigger clarification."""
        plan = TechnicalPlan(
            transformation_type=TransformationType.DROP_COLUMNS,
            target_columns=[ColumnTarget(column_name="unknown_column")],
            explanation="Drop the column",
            confidence=0.5,
            clarification_needed="Did you mean 'email' or 'customer_email'?",
        )

        assert plan.needs_clarification()
        assert "Did you mean" in plan.clarification_needed


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestErrorHandling:
    """Test actionable error messages."""

    def test_strategy_error_with_suggestion(self):
        """Test StrategyError includes actionable suggestion."""
        error = StrategyError(
            message="Column 'emial' not found",
            code="COLUMN_NOT_FOUND",
            suggestion="Did you mean 'email'?",
            details={"requested": "emial", "available": ["email", "name"]},
        )

        error_str = str(error)
        assert "COLUMN_NOT_FOUND" in error_str
        assert "emial" in error_str
        assert "Did you mean 'email'" in error_str

    def test_strategy_error_without_suggestion(self):
        """Test StrategyError works without suggestion."""
        error = StrategyError(
            message="Unknown error occurred",
            code="UNKNOWN_ERROR",
        )

        error_str = str(error)
        assert "UNKNOWN_ERROR" in error_str
        assert "Suggestion" not in error_str


# =============================================================================
# Enum Value Tests
# =============================================================================

class TestEnumValues:
    """Test that enum values serialize correctly."""

    def test_transformation_type_values(self):
        """Test TransformationType enum values."""
        assert TransformationType.DROP_ROWS.value == "drop_rows"
        assert TransformationType.FILL_NULLS.value == "fill_nulls"
        assert TransformationType.UNDO.value == "undo"

    def test_filter_operator_values(self):
        """Test FilterOperator enum values."""
        assert FilterOperator.ISNULL.value == "isnull"
        assert FilterOperator.CONTAINS.value == "contains"
        assert FilterOperator.IN.value == "in"

    def test_case_type_values(self):
        """Test CaseType enum values."""
        assert CaseType.UPPER.value == "upper"
        assert CaseType.LOWER.value == "lower"
        assert CaseType.TITLE.value == "title"

    def test_fill_method_values(self):
        """Test FillMethod enum values."""
        assert FillMethod.MEAN.value == "mean"
        assert FillMethod.MEDIAN.value == "median"
        assert FillMethod.VALUE.value == "value"
