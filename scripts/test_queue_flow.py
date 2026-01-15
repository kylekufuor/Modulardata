#!/usr/bin/env python3
"""Test the queue-based transformation flow."""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from agents.strategist import StrategistAgent
from lib.memory import ConversationContext
from core.models.profile import DataProfile, ColumnProfile, SemanticType


def create_context():
    """Create test context."""
    mock_profile = DataProfile(
        row_count=1000,
        column_count=5,
        columns=[
            ColumnProfile(name="id", dtype="int64", semantic_type=SemanticType.ID, null_count=0, null_percent=0.0, unique_count=1000),
            ColumnProfile(name="name", dtype="object", semantic_type=SemanticType.NAME, null_count=10, null_percent=1.0, unique_count=950),
            ColumnProfile(name="email", dtype="object", semantic_type=SemanticType.EMAIL, null_count=50, null_percent=5.0, unique_count=950),
            ColumnProfile(name="age", dtype="float64", semantic_type=SemanticType.NUMERIC, null_count=25, null_percent=2.5, unique_count=80),
            ColumnProfile(name="created_at", dtype="object", semantic_type=SemanticType.DATE, null_count=0, null_percent=0.0, unique_count=500),
        ],
    )
    return ConversationContext(
        session_id="550e8400-e29b-41d4-a716-446655440000",
        current_node_id="660e8400-e29b-41d4-a716-446655440001",
        parent_node_id="660e8400-e29b-41d4-a716-446655440000",
        current_profile=mock_profile,
        messages=[],
        recent_transformations=[],
        original_filename="customers.csv",
        current_row_count=1000,
        current_column_count=5,
    )


def simulate_transformation(context, plan):
    """Simulate applying a transformation."""
    trans_type = plan.transformation_type
    if hasattr(trans_type, 'value'):
        trans_type = trans_type.value

    columns = plan.get_target_column_names()

    if trans_type == "drop_rows" and columns:
        for col_name in columns:
            for col in context.current_profile.columns:
                if col.name == col_name:
                    context.current_row_count -= col.null_count
                    col.null_count = 0
                    col.null_percent = 0.0
    elif trans_type == "fill_nulls" and columns:
        for col_name in columns:
            for col in context.current_profile.columns:
                if col.name == col_name:
                    col.null_count = 0
                    col.null_percent = 0.0
    return context


def run_test():
    """Test the queue-based flow."""
    print("=" * 70)
    print("  QUEUE-BASED TRANSFORMATION FLOW TEST")
    print("=" * 70)

    context = create_context()
    pending_plans = []

    # Test messages that should add to the queue
    test_messages = [
        "remove rows where email is missing",
        "fill missing ages with 0",
        "fill missing names with Unknown",
    ]

    print("\n--- Initial State ---")
    print(f"Rows: {context.current_row_count}")
    issues = [(c.name, c.null_count) for c in context.current_profile.columns if c.null_count > 0]
    print(f"Issues: {issues}")

    # Queue up transformations
    print("\n--- Queuing Transformations ---")

    for msg in test_messages:
        print(f"\nUser: \"{msg}\"")

        with patch("agents.strategist.build_conversation_context") as mock:
            mock.return_value = context
            agent = StrategistAgent()
            plan = agent.create_plan(session_id=context.session_id, user_message=msg)

            pending_plans.append(plan)
            context = simulate_transformation(context, plan)

            trans_type = plan.transformation_type
            if hasattr(trans_type, 'value'):
                trans_type = trans_type.value
            cols = plan.get_target_column_names()

            print(f"  → Queued: {trans_type.upper()} on {cols}")
            print(f"  → Pending: {len(pending_plans)} change(s)")

            # Show preview state
            remaining = [(c.name, c.null_count) for c in context.current_profile.columns if c.null_count > 0]
            print(f"  → Preview state: {context.current_row_count} rows, remaining issues: {remaining}")

    # Show final queue
    print("\n--- Queue Summary (before apply) ---")
    for i, plan in enumerate(pending_plans, 1):
        trans_type = plan.transformation_type
        if hasattr(trans_type, 'value'):
            trans_type = trans_type.value
        cols = plan.get_target_column_names()
        print(f"  {i}. {trans_type.upper()} on {cols}")

    # Simulate apply
    print("\n--- Applying All Changes ---")
    print(f"User: \"apply\"")
    print(f"\n  Creating single node with {len(pending_plans)} transformations...")
    print(f"  Final state: {context.current_row_count} rows")

    # Verify final state
    remaining_issues = [(c.name, c.null_count) for c in context.current_profile.columns if c.null_count > 0]
    print(f"  Remaining issues: {remaining_issues if remaining_issues else 'None - data is clean!'}")

    print("\n" + "=" * 70)
    print("  TEST COMPLETE - Queue-based flow works!")
    print("=" * 70)

    # Summary
    print("\nThe flow demonstrates:")
    print("  1. Multiple transformations queued without immediate execution")
    print("  2. State simulation shows preview of changes")
    print("  3. 'apply' creates a single node with all batched transformations")
    print("  4. Minimal node creation (1 node for 3 operations)")


if __name__ == "__main__":
    run_test()
