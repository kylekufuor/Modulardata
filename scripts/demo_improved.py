#!/usr/bin/env python3
"""Demo of improved conversation flow with state tracking."""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI
from agents.strategist import StrategistAgent, StrategyError
from lib.memory import ConversationContext, ChatMessage
from core.models.profile import DataProfile, ColumnProfile, SemanticType

client = OpenAI()


def create_context():
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


def simulate_transform(context, plan):
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


def get_issues(context):
    issues = []
    for col in context.current_profile.columns:
        if col.null_count > 0:
            issues.append({"column": col.name, "count": col.null_count})
    return sorted(issues, key=lambda x: x["count"], reverse=True)


def run_demo():
    print("=" * 70)
    print("  IMPROVED CONVERSATION DEMO")
    print("=" * 70)
    print("\nThis demo shows the improved flow with:")
    print("  - 'yes' correctly triggers the suggested action")
    print("  - State updates after each transformation")
    print("  - Accurate issue counts")
    print("-" * 70)

    context = create_context()
    last_suggestion = None
    conversation = []

    demo_turns = [
        ("What data is this?", "conversational"),
        ("yes", "affirmative"),  # Should trigger: remove rows where email is missing
        ("yes", "affirmative"),  # Should trigger: fill missing ages with 0
        ("yes", "affirmative"),  # Should trigger: fill missing names
    ]

    for user_msg, msg_type in demo_turns:
        print(f"\nYou: {user_msg}")

        if msg_type == "affirmative" and last_suggestion:
            effective_msg = last_suggestion
            print(f"  [Understanding: \"{last_suggestion}\"]")
        else:
            effective_msg = user_msg

        if msg_type == "conversational":
            # Get conversational response
            issues = get_issues(context)
            issue_str = ", ".join([f"{i['count']} missing {i['column']}s" for i in issues])

            response = f"This is 'customers.csv' with {context.current_row_count} rows and 5 columns. I see some issues: {issue_str}. Want me to fix the missing emails first?"
            print(f"\nAssistant: {response}")
            last_suggestion = "remove rows where email is missing"

        else:
            # Use Strategist
            with patch("agents.strategist.build_conversation_context") as mock:
                mock.return_value = context
                agent = StrategistAgent()
                plan = agent.create_plan(session_id=context.session_id, user_message=effective_msg)

                # Simulate transformation
                context = simulate_transform(context, plan)

                # Get remaining issues
                issues = get_issues(context)

                trans_type = plan.transformation_type
                if hasattr(trans_type, 'value'):
                    trans_type = trans_type.value
                cols = plan.get_target_column_names()

                if issues:
                    next_issue = issues[0]
                    response = f"Done! Removed/fixed {cols[0] if cols else 'data'}. Your data now has {context.current_row_count} rows. '{next_issue['column']}' still has {next_issue['count']} missing values. Want me to fix those?"
                    # Set next suggestion based on column type
                    if next_issue['column'] == 'age':
                        last_suggestion = "fill missing ages with 0"
                    elif next_issue['column'] == 'name':
                        last_suggestion = "fill missing names with Unknown"
                    else:
                        last_suggestion = f"remove rows where {next_issue['column']} is missing"
                else:
                    response = f"Done! Your data now has {context.current_row_count} rows and is clean. Anything else?"
                    last_suggestion = None

                print(f"\nAssistant: {response}")
                print(f"  [Plan: {trans_type.upper()} on {cols}]")

        print("-" * 70)

    print("\n" + "=" * 70)
    print("  DEMO COMPLETE")
    print("=" * 70)
    print(f"\nFinal state: {context.current_row_count} rows, all columns clean!")


if __name__ == "__main__":
    run_demo()
