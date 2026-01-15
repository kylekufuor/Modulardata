#!/usr/bin/env python3
# =============================================================================
# scripts/demo_conversation.py - Demo Conversation with Data Assistant
# =============================================================================
# Simulates a realistic conversation to demonstrate the agent's capabilities.
# The agent proactively suggests next steps after each response.
# =============================================================================

import os
import sys
from unittest.mock import patch

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI
from agents.strategist import StrategistAgent, StrategyError
from lib.memory import ConversationContext, ChatMessage
from core.models.profile import DataProfile, ColumnProfile, SemanticType

# Initialize OpenAI client
client = OpenAI()


def create_sample_context():
    """Create a sample data context."""
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
        messages=[],
        recent_transformations=[],
        original_filename="customers.csv",
        current_row_count=1000,
        current_column_count=5,
    )


def get_data_issues(context):
    """Get a list of data issues for suggestions."""
    issues = []
    for col in context.current_profile.columns:
        if col.null_count > 0:
            issues.append({
                "column": col.name,
                "issue": "missing_values",
                "count": col.null_count,
                "percent": col.null_percent,
                "semantic_type": col.semantic_type.value,
            })
    return sorted(issues, key=lambda x: x["count"], reverse=True)


def get_next_suggestion(context, last_action=None, conversation_history=None):
    """Generate a proactive suggestion for next steps."""
    issues = get_data_issues(context)

    if not issues:
        return "\n\nYour data looks clean! Is there anything else you'd like to do with it?"

    # Find the most pressing issue that hasn't been addressed
    top_issue = issues[0]
    col = top_issue["column"]
    count = top_issue["count"]

    # Generate contextual suggestions based on the column type
    if top_issue["semantic_type"] == "email":
        suggestion = f"\n\nI notice there are {count} missing emails. Would you like me to remove those rows, or would you prefer to keep them?"
    elif top_issue["semantic_type"] == "numeric":
        suggestion = f"\n\nThere are {count} missing values in '{col}'. Would you like me to fill them with a default value (like 0 or the average), or remove those rows?"
    elif top_issue["semantic_type"] == "name":
        suggestion = f"\n\nI see {count} missing names. Would you like to fill them with 'Unknown' or remove those rows?"
    else:
        suggestion = f"\n\nThe '{col}' column has {count} missing values. Would you like me to help clean that up?"

    return suggestion


def get_post_transform_suggestion(context, plan):
    """Generate a suggestion after a transformation."""
    issues = get_data_issues(context)

    trans_type = plan.transformation_type
    if hasattr(trans_type, 'value'):
        trans_type = trans_type.value

    if plan.is_undo():
        return "\n\nI've restored the previous version. What would you like to do now?"

    # Check for remaining issues
    if issues:
        next_issue = issues[0]
        if trans_type == "drop_rows" and next_issue["issue"] == "missing_values":
            return f"\n\nDone! There are still {next_issue['count']} missing values in '{next_issue['column']}'. Would you like me to handle those next?"
        elif trans_type == "fill_nulls":
            if len(issues) > 1:
                next_issue = issues[0]
                return f"\n\nGot it! I also noticed '{next_issue['column']}' has {next_issue['count']} missing values. Should I address that too?"
            else:
                return "\n\nDone! Is there anything else you'd like to clean up?"
        else:
            return f"\n\nAll set! The '{next_issue['column']}' column still has some missing values. Want me to help with that, or is there something else?"
    else:
        return "\n\nDone! Your data is looking cleaner. What else can I help you with?"


def get_data_summary(context):
    """Get a text summary of the data."""
    lines = [
        f"The user has uploaded a file called '{context.original_filename}' with {context.current_row_count:,} rows and {context.current_column_count} columns.",
        "",
        "Columns:",
    ]

    for col in context.current_profile.columns:
        null_info = f" - {col.null_count} missing values ({col.null_percent:.1f}%)" if col.null_count > 0 else " - no missing values"
        lines.append(f"  - {col.name} ({col.dtype}, {col.semantic_type.value}){null_info}")

    issues = []
    for col in context.current_profile.columns:
        if col.null_count > 0:
            issues.append(f"{col.name} has {col.null_count} missing values")

    if issues:
        lines.append("")
        lines.append("Data quality issues:")
        for issue in issues:
            lines.append(f"  - {issue}")

    return "\n".join(lines)


def get_conversational_response(message, context, conversation_history):
    """Get a conversational response with proactive suggestions."""
    system_prompt = f"""You are a friendly data cleaning assistant. You help users understand and clean their data through natural conversation.

Current data context:
{get_data_summary(context)}

Guidelines:
- Be conversational, friendly, and helpful
- When users ask about their data, describe it in plain language
- Keep responses concise (2-3 short paragraphs max)
- You can see the data profile but not the actual data values
- Don't use technical jargon unless the user does
- IMPORTANT: End with exactly ONE short question or suggestion (never multiple questions)
- If there are data issues, suggest fixing the biggest one (e.g., "Want me to clean up the 50 missing emails?")
- Keep the closing question under 15 words"""

    messages = [{"role": "system", "content": system_prompt}]
    for msg in conversation_history[-10:]:
        messages.append(msg)
    messages.append({"role": "user", "content": message})

    response = client.chat.completions.create(
        model="gpt-4-turbo",
        messages=messages,
        temperature=0.7,
        max_tokens=350,
    )

    return response.choices[0].message.content


def get_transformation_response(message, context, plan):
    """Generate a conversational response for a transformation."""
    trans_type = plan.transformation_type
    if hasattr(trans_type, 'value'):
        trans_type = trans_type.value

    columns = plan.get_target_column_names()
    confidence = plan.confidence

    if plan.clarification_needed:
        return f"{plan.explanation}\n\n{plan.clarification_needed}"

    if plan.is_undo():
        return "Got it! I'll undo the last change and restore your previous version." + get_post_transform_suggestion(context, plan)

    # Confidence indicator
    if confidence >= 0.9:
        conf_phrase = "I'll"
    elif confidence >= 0.7:
        conf_phrase = "I think I should"
    else:
        conf_phrase = "I'm not entirely sure, but I could"

    action_templates = {
        "drop_rows": f"{conf_phrase} remove rows where {', '.join(columns) if columns else 'certain conditions are met'}",
        "deduplicate": f"{conf_phrase} remove duplicate rows from your data",
        "drop_columns": f"{conf_phrase} remove the {', '.join(columns)} column{'s' if len(columns) > 1 else ''}",
        "rename_column": f"{conf_phrase} rename '{columns[0] if columns else 'the column'}' to '{plan.parameters.get('new_name', 'new name')}'",
        "fill_nulls": f"{conf_phrase} fill the missing values in {', '.join(columns)}",
        "parse_date": f"{conf_phrase} convert {', '.join(columns)} to a proper date format",
        "standardize": f"{conf_phrase} standardize the format of {', '.join(columns)}",
    }

    base_response = action_templates.get(trans_type, plan.explanation)

    # Add condition details
    if plan.conditions:
        for c in plan.conditions:
            op = c.operator.value if hasattr(c.operator, 'value') else c.operator
            if op == "isnull":
                base_response += f" where {c.column} is blank or missing"

    base_response += "."

    # Add impact info
    if trans_type == "drop_rows" and columns:
        col = columns[0]
        for c in context.current_profile.columns:
            if c.name == col and c.null_count > 0:
                base_response += f" This will affect approximately {c.null_count} rows."
                break

    # Add proactive next step suggestion
    base_response += get_post_transform_suggestion(context, plan)

    return base_response


def is_transformation_request(message):
    """Check if the message is asking for a transformation."""
    keywords = [
        "remove", "delete", "drop", "clean", "fill", "replace", "rename",
        "convert", "format", "deduplicate", "duplicate", "fix", "undo",
        "blank", "null", "empty", "missing", "get rid", "yes", "do it",
        "go ahead", "sure", "okay", "proceed"
    ]
    return any(kw in message.lower() for kw in keywords)


def run_demo():
    """Run a demo conversation."""
    print("=" * 70)
    print("  MODULARDATA - Demo Conversation (with Proactive Suggestions)")
    print("=" * 70)
    print("\nSimulating a realistic user conversation with the data assistant.")
    print("Notice how the agent suggests next steps after each response.\n")
    print("-" * 70)

    context = create_sample_context()
    conversation_history = []

    # Demo conversation - more natural back-and-forth
    demo_messages = [
        "Hi! What data do I have here?",
        "Yes, please clean up the emails",
        "Sure, fill the ages with 0",
        "What about the names?",
        "No, leave the names for now. Can you rename 'name' to 'full_name' instead?",
    ]

    for user_message in demo_messages:
        print(f"\nYou: {user_message}")
        print()

        if is_transformation_request(user_message):
            # Use Strategist agent for transformations
            with patch("agents.strategist.build_conversation_context") as mock_build:
                mock_build.return_value = context

                try:
                    agent = StrategistAgent()
                    plan = agent.create_plan(
                        session_id=context.session_id,
                        user_message=user_message
                    )

                    response = get_transformation_response(user_message, context, plan)
                    print(f"Assistant: {response}")

                    # Show plan details
                    trans_type = plan.transformation_type
                    if hasattr(trans_type, 'value'):
                        trans_type = trans_type.value
                    print(f"\n  [Plan: {trans_type.upper()} | Confidence: {plan.confidence:.0%}]")

                except StrategyError as e:
                    print(f"Assistant: I had trouble with that. {e.message}")
        else:
            # General conversation
            response = get_conversational_response(user_message, context, conversation_history)
            print(f"Assistant: {response}")

        # Update history
        conversation_history.append({"role": "user", "content": user_message})
        conversation_history.append({"role": "assistant", "content": response})

        print("-" * 70)

    print("\n" + "=" * 70)
    print("  Demo Complete!")
    print("=" * 70)
    print("\nThe agent now proactively suggests next steps, creating a flowing")
    print("conversation that guides users through data cleaning.\n")


if __name__ == "__main__":
    run_demo()
