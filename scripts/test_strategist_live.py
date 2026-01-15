#!/usr/bin/env python3
# =============================================================================
# scripts/test_strategist_live.py - Live Test for Strategist Agent
# =============================================================================
# Tests the Strategist agent with real OpenAI API calls.
#
# Usage:
#   poetry run python scripts/test_strategist_live.py
#
# Make sure OPENAI_API_KEY is set in your environment or .env file.
# =============================================================================

import os
import sys
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

# Check for API key
if not os.getenv("OPENAI_API_KEY"):
    print("ERROR: OPENAI_API_KEY not found in environment")
    print("Please set it in your .env file or environment")
    sys.exit(1)

from agents.strategist import StrategistAgent, StrategyError
from agents.models.technical_plan import TechnicalPlan
from lib.memory import ConversationContext, ChatMessage
from core.models.profile import DataProfile, ColumnProfile, SemanticType


def create_mock_context():
    """Create a mock conversation context for testing."""
    # Create a mock DataProfile
    mock_profile = DataProfile(
        row_count=1000,
        column_count=5,
        columns=[
            ColumnProfile(
                name="id",
                dtype="int64",
                semantic_type=SemanticType.ID,
                null_count=0,
                null_percent=0.0,
                unique_count=1000,
            ),
            ColumnProfile(
                name="name",
                dtype="object",
                semantic_type=SemanticType.NAME,
                null_count=10,
                null_percent=1.0,
                unique_count=950,
            ),
            ColumnProfile(
                name="email",
                dtype="object",
                semantic_type=SemanticType.EMAIL,
                null_count=50,
                null_percent=5.0,
                unique_count=950,
            ),
            ColumnProfile(
                name="age",
                dtype="float64",
                semantic_type=SemanticType.NUMERIC,
                null_count=25,
                null_percent=2.5,
                unique_count=80,
            ),
            ColumnProfile(
                name="created_at",
                dtype="object",
                semantic_type=SemanticType.DATE,
                null_count=0,
                null_percent=0.0,
                unique_count=500,
            ),
        ],
    )

    return ConversationContext(
        session_id="550e8400-e29b-41d4-a716-446655440000",
        current_node_id="660e8400-e29b-41d4-a716-446655440001",
        parent_node_id="660e8400-e29b-41d4-a716-446655440000",
        current_profile=mock_profile,
        messages=[
            ChatMessage(role="user", content="I uploaded my customer data"),
            ChatMessage(role="assistant", content="I see you have 1000 rows with 5 columns. I notice some null values in email, age, and name columns."),
        ],
        recent_transformations=[],
        original_filename="customers.csv",
        current_row_count=1000,
        current_column_count=5,
    )


def run_test(name: str, user_message: str):
    """Run a single test with mocked context."""
    print(f"\n{'=' * 60}")
    print(f"TEST: {name}")
    print(f"Message: '{user_message}'")
    print("=" * 60)

    # Mock the context fetching
    mock_context = create_mock_context()

    with patch("agents.strategist.build_conversation_context") as mock_build:
        mock_build.return_value = mock_context

        agent = StrategistAgent()

        try:
            plan = agent.create_plan(
                session_id="550e8400-e29b-41d4-a716-446655440000",
                user_message=user_message
            )

            print("\n✓ Plan created successfully!")
            print(f"\nTransformation Type: {plan.transformation_type}")
            print(f"Target Columns: {plan.get_target_column_names()}")
            print(f"Confidence: {plan.confidence:.0%}")
            print(f"Explanation: {plan.explanation}")

            if plan.conditions:
                print(f"Conditions: {len(plan.conditions)}")
                for c in plan.conditions:
                    val = c.value if hasattr(c, 'value') else ''
                    op = c.operator.value if hasattr(c.operator, 'value') else c.operator
                    print(f"  - {c.column} {op} {val or ''}")

            if plan.parameters:
                print(f"Parameters: {plan.parameters}")

            if plan.clarification_needed:
                print(f"\n⚠ Clarification Needed: {plan.clarification_needed}")

            if plan.is_undo():
                print(f"Rollback to: {plan.rollback_to_node_id}")

            return True

        except StrategyError as e:
            print(f"\n✗ Strategy Error: {e.message}")
            print(f"  Code: {e.code}")
            if e.suggestion:
                print(f"  Suggestion: {e.suggestion}")
            return False

        except Exception as e:
            print(f"\n✗ Unexpected Error: {e}")
            return False


def test_chat_handler():
    """Test using the chat handler interface."""
    print(f"\n{'=' * 60}")
    print("TEST: Chat Handler - preview_transformation()")
    print("=" * 60)

    from agents.chat_handler import preview_transformation

    mock_context = create_mock_context()

    with patch("agents.strategist.build_conversation_context") as mock_build:
        mock_build.return_value = mock_context

        try:
            response = preview_transformation(
                session_id="550e8400-e29b-41d4-a716-446655440000",
                message="remove duplicate rows"
            )

            print("\n✓ PlanResponse received!")
            print(f"\nMode: {response.mode}")
            print(f"Can Execute: {response.can_execute}")
            print(f"Assistant Message: {response.assistant_message}")

            if response.clarification_needed:
                print(f"Clarification: {response.clarification_needed}")

            print(f"\nPlan Type: {response.plan.get('transformation_type')}")
            print(f"Confidence: {response.plan.get('confidence', 0):.0%}")

            return True

        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback
            traceback.print_exc()
            return False


if __name__ == "__main__":
    print("=" * 60)
    print("STRATEGIST AGENT - LIVE TESTING (with OpenAI)")
    print("=" * 60)
    print("\nThis will make real OpenAI API calls.")
    print("Using mocked Supabase context for testing.\n")

    results = []

    # Test cases
    test_cases = [
        ("Basic: Remove blank emails", "remove rows where email is blank"),
        ("Ambiguous: Clean up data", "clean up the data"),
        ("Undo operation", "undo that"),
        ("Date formatting", "convert created_at to YYYY-MM-DD format"),
        ("Fill nulls", "fill missing ages with 0"),
        ("Deduplicate", "remove duplicate rows"),
        ("Drop column", "delete the age column"),
        ("Rename column", "rename 'name' to 'full_name'"),
    ]

    try:
        for name, message in test_cases:
            success = run_test(name, message)
            results.append((name, success))

        # Test chat handler
        success = test_chat_handler()
        results.append(("Chat Handler", success))

        # Summary
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)

        passed = sum(1 for _, s in results if s)
        total = len(results)

        for name, success in results:
            status = "✓ PASS" if success else "✗ FAIL"
            print(f"  {status}: {name}")

        print(f"\nTotal: {passed}/{total} passed")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n\nTests cancelled.")
        sys.exit(0)
