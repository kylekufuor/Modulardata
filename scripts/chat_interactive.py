#!/usr/bin/env python3
# =============================================================================
# scripts/chat_interactive.py - Interactive Chat with Data Assistant
# =============================================================================
# A conversational data cleaning assistant - like Parabola but in your terminal.
# The agent proactively suggests next steps after each response.
#
# Usage:
#   poetry run python scripts/chat_interactive.py <path_to_csv>
#   poetry run python scripts/chat_interactive.py                  # Uses demo data
#
# Commands:
#   /quit or /exit - Exit the chat
#   /profile       - Show the current data profile
#   /queue         - Show pending transformations
#   /clear         - Clear pending transformations
#   /apply         - Apply all pending transformations
#   /help          - Show help
# =============================================================================

import os
import sys
from pathlib import Path
from unittest.mock import patch

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

# Check for API key
if not os.getenv("OPENAI_API_KEY"):
    print("ERROR: OPENAI_API_KEY not found in environment")
    print("Please set it in your .env file or environment")
    sys.exit(1)

import pandas as pd
from openai import OpenAI
from agents.strategist import StrategistAgent, StrategyError
from agents.engineer import EngineerAgent
from agents.tester import TesterAgent
from lib.memory import ConversationContext, ChatMessage
from lib.profiler import profile_from_file, generate_profile, read_csv_safe
from core.models.profile import DataProfile, ColumnProfile, SemanticType

# Initialize OpenAI client
client = OpenAI()


def create_sample_context():
    """Create a sample data context for demo mode."""
    import numpy as np

    # Create actual sample DataFrame for demo
    np.random.seed(42)
    n_rows = 1000

    sample_df = pd.DataFrame({
        'id': range(1, n_rows + 1),
        'name': [f"Person {i}" if i % 100 != 0 else None for i in range(1, n_rows + 1)],  # 10 nulls
        'email': [f"user{i}@test.com" if i % 20 != 0 else None for i in range(1, n_rows + 1)],  # 50 nulls
        'age': [np.random.randint(18, 80) if i % 40 != 0 else None for i in range(1, n_rows + 1)],  # 25 nulls
        'created_at': [f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}" for i in range(1, n_rows + 1)],
    })

    # Generate profile from actual data
    mock_profile = generate_profile(sample_df)

    context = ConversationContext(
        session_id="550e8400-e29b-41d4-a716-446655440000",
        current_node_id="660e8400-e29b-41d4-a716-446655440001",
        parent_node_id="660e8400-e29b-41d4-a716-446655440000",
        current_profile=mock_profile,
        messages=[],
        recent_transformations=[],
        original_filename="customers.csv",
        current_row_count=len(sample_df),
        current_column_count=len(sample_df.columns),
    )

    return context, sample_df


def load_csv_context(file_path: str):
    """Load a real CSV file and create context from it."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    if not path.suffix.lower() == ".csv":
        raise ValueError(f"File must be a CSV: {file_path}")

    # Load the actual DataFrame
    print(f"\n  Loading and analyzing: {path.name}...")
    df, encoding, delimiter = read_csv_safe(str(path))

    # Generate profile from the DataFrame
    profile = generate_profile(df)

    context = ConversationContext(
        session_id="550e8400-e29b-41d4-a716-446655440000",
        current_node_id="660e8400-e29b-41d4-a716-446655440001",
        parent_node_id="660e8400-e29b-41d4-a716-446655440000",
        current_profile=profile,
        messages=[],
        recent_transformations=[],
        original_filename=path.name,
        current_row_count=profile.row_count,
        current_column_count=profile.column_count,
    )

    return context, df


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


def get_post_transform_suggestion(context, plan):
    """Generate a suggestion after a transformation."""
    issues = get_data_issues(context)

    trans_type = plan.transformation_type
    if hasattr(trans_type, 'value'):
        trans_type = trans_type.value

    columns = plan.get_target_column_names()
    affected_col = columns[0] if columns else "the data"

    if plan.is_undo():
        return "\n\nI've restored the previous version. What would you like to do now?"

    # Check for remaining issues (after transformation was simulated)
    if issues:
        next_issue = issues[0]
        return f"\n\nDone! Now '{next_issue['column']}' has {next_issue['count']} missing values. Want me to fix those?"
    else:
        return f"\n\nDone! Your data now has {context.current_row_count} rows and looks clean. Anything else?"


def get_data_summary(context):
    """Get a text summary of the data for the AI."""
    lines = [
        f"The user has uploaded a file called '{context.original_filename}' with {context.current_row_count:,} rows and {context.current_column_count} columns.",
        "",
        "Columns:",
    ]

    if context.current_profile:
        for col in context.current_profile.columns:
            null_info = f" - {col.null_count} missing values ({col.null_percent:.1f}%)" if col.null_count > 0 else " - no missing values"
            lines.append(f"  - {col.name} ({col.dtype}, {col.semantic_type.value}){null_info}")

    # Add issues summary
    issues = []
    if context.current_profile:
        for col in context.current_profile.columns:
            if col.null_count > 0:
                issues.append(f"{col.name} has {col.null_count} missing values")

    if issues:
        lines.append("")
        lines.append("Data quality issues:")
        for issue in issues:
            lines.append(f"  - {issue}")

    return "\n".join(lines)


def is_affirmative(message):
    """Check if the message is a short affirmative response."""
    message_lower = message.lower().strip().rstrip('!').rstrip('.')

    # Exact matches for short affirmatives
    exact_matches = [
        "yes", "yeah", "yep", "yup", "sure", "okay", "ok", "k",
        "go ahead", "do it", "proceed", "please do", "sounds good",
        "let's do it", "yes please", "sure thing", "absolutely",
        "definitely", "please", "y", "yea", "alright", "fine",
        "go for it", "that works", "perfect", "great"
    ]

    if message_lower in exact_matches:
        return True

    # Also check if it starts with yes/sure + something short
    if len(message_lower) < 20:
        if message_lower.startswith(("yes", "sure", "okay", "ok ", "yeah", "yep")):
            return True

    return False


def is_transformation_request(message):
    """Check if the message is asking for a data transformation."""
    transform_keywords = [
        "remove", "delete", "drop", "clean", "fill", "replace", "rename",
        "convert", "format", "deduplicate", "duplicate", "merge", "split",
        "trim", "standardize", "fix", "change", "update", "undo", "filter",
        "keep only", "get rid of", "blank", "null", "empty", "missing"
    ]
    message_lower = message.lower()
    return any(keyword in message_lower for keyword in transform_keywords)


def get_conversational_response(message, context, conversation_history):
    """Get a conversational response using OpenAI with proactive suggestions."""

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

    # Add conversation history (last 10 messages)
    for msg in conversation_history[-10:]:
        messages.append(msg)

    # Add current message
    messages.append({"role": "user", "content": message})

    response = client.chat.completions.create(
        model="gpt-4-turbo",
        messages=messages,
        temperature=0.7,
        max_tokens=350,
    )

    return response.choices[0].message.content


def get_transformation_response(message, context, plan):
    """Generate a conversational response for a transformation with proactive suggestions."""

    trans_type = plan.transformation_type
    if hasattr(trans_type, 'value'):
        trans_type = trans_type.value

    columns = plan.get_target_column_names()
    confidence = plan.confidence

    # Build a natural response
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

    # Action descriptions
    action_templates = {
        "drop_rows": f"{conf_phrase} remove rows where {', '.join(columns) if columns else 'certain conditions are met'}",
        "filter_rows": f"{conf_phrase} keep only the rows that match your criteria",
        "deduplicate": f"{conf_phrase} remove duplicate rows from your data",
        "drop_columns": f"{conf_phrase} remove the {', '.join(columns)} column{'s' if len(columns) > 1 else ''}",
        "rename_column": f"{conf_phrase} rename '{columns[0] if columns else 'the column'}' to '{plan.parameters.get('new_name', 'new name')}'",
        "fill_nulls": f"{conf_phrase} fill the missing values in {', '.join(columns)}",
        "replace_values": f"{conf_phrase} replace values in {', '.join(columns)}",
        "standardize": f"{conf_phrase} standardize the format of {', '.join(columns)}",
        "trim_whitespace": f"{conf_phrase} clean up extra spaces in {', '.join(columns)}",
        "change_case": f"{conf_phrase} change the text case in {', '.join(columns)}",
        "parse_date": f"{conf_phrase} convert {', '.join(columns)} to a proper date format",
        "format_date": f"{conf_phrase} reformat the dates in {', '.join(columns)}",
        "convert_type": f"{conf_phrase} convert the data type of {', '.join(columns)}",
    }

    base_response = action_templates.get(trans_type, plan.explanation)

    # Add details based on conditions or parameters
    details = []
    if plan.conditions:
        for c in plan.conditions:
            op = c.operator.value if hasattr(c.operator, 'value') else c.operator
            if op == "isnull":
                details.append(f"where {c.column} is blank or missing")
            elif op == "equals":
                details.append(f"where {c.column} equals '{c.value}'")
            elif op == "contains":
                details.append(f"where {c.column} contains '{c.value}'")

    if details:
        base_response += " " + " and ".join(details)

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


def print_header(context, is_demo=False):
    """Print the welcome message and data profile summary."""
    print("\n" + "=" * 60)
    print("  Welcome to ModularData!")
    print("=" * 60)

    print("\nI'm your data transformation assistant. I help you clean,")
    print("transform, and prepare your data through conversation.")

    print("\n" + "-" * 60)
    print("  HOW IT WORKS")
    print("-" * 60)
    print("  1. Tell me what you want to fix or change")
    print("  2. I'll queue up each transformation step")
    print("  3. Say 'apply' to create the transformation batch")
    print("\n  Each module is a checkpoint you can branch from or undo.")
    print("  You control how many steps go into each module.")

    # Data profile summary
    print("\n" + "=" * 60)
    if is_demo:
        print(f"  Your Data: {context.original_filename} (DEMO)")
    else:
        print(f"  Your Data: {context.original_filename}")
    print("=" * 60)
    print(f"  Rows: {context.current_row_count:,}  |  Columns: {context.current_column_count}")

    # Show columns
    print("\n  Columns:")
    for col in context.current_profile.columns:
        semantic = col.semantic_type.value if col.semantic_type else "unknown"
        null_info = f" ({col.null_count} missing)" if col.null_count > 0 else ""
        print(f"    • {col.name} [{semantic}]{null_info}")

    # Show issues if any
    issues = get_data_issues(context)
    if issues:
        print("\n  Issues Detected:")
        for issue in issues[:5]:  # Show top 5 issues
            pct = (issue['count'] / context.current_row_count * 100) if context.current_row_count > 0 else 0
            print(f"    • {issue['count']:,} missing {issue['column']} ({pct:.1f}%)")
    else:
        print("\n  No obvious issues detected - your data looks clean!")

    print("\n" + "-" * 60)

    # Follow-up question based on issues
    if issues:
        top_issue = issues[0]
        print(f"\n  What would you like to tackle first?")
        print(f"  (I noticed {top_issue['column']} has the most missing values)")
    else:
        print(f"\n  What would you like to do with this data?")

    print("\n")


def print_help():
    """Print help message."""
    print("\n" + "-" * 40)
    print("COMMANDS:")
    print("  /help    - Show this help")
    print("  /queue   - Show pending changes")
    print("  /clear   - Clear pending changes")
    print("  /profile - Show data details")
    print("  /apply   - Apply all pending changes")
    print("  /quit    - Exit")
    print("\nTry saying things like:")
    print('  "Remove rows with missing emails"')
    print('  "Fill missing ages with 0"')
    print('  "apply" - to create the transformation batch')
    print("-" * 40 + "\n")


def print_queue(pending_plans, context):
    """Print the pending transformation queue."""
    if not pending_plans:
        print("\n  No pending changes. Your data is unchanged.")
        print("  Tell me what you'd like to fix!\n")
        return

    print("\n" + "-" * 50)
    print("  PENDING CHANGES (not yet applied)")
    print("-" * 50)

    total_rows_affected = 0
    for i, plan in enumerate(pending_plans, 1):
        trans_type = plan.transformation_type
        if hasattr(trans_type, 'value'):
            trans_type = trans_type.value

        cols = plan.get_target_column_names()
        col_str = ", ".join(cols) if cols else "all columns"

        # Estimate impact
        impact = ""
        if trans_type == "drop_rows" and cols:
            for col in context.current_profile.columns:
                if col.name == cols[0]:
                    impact = f" (~{col.null_count} rows)"
                    total_rows_affected += col.null_count
                    break

        print(f"  {i}. {trans_type.upper()} on {col_str}{impact}")

    print("-" * 50)
    print(f"  Total: {len(pending_plans)} changes queued")
    if total_rows_affected > 0:
        print(f"  Estimated rows affected: ~{total_rows_affected}")
    print("\n  Say 'apply' to create this transformation batch.")
    print("  Say '/clear' to discard pending changes.\n")


def apply_all_changes(pending_plans, context, current_df):
    """Apply all pending changes using the Engineer and Tester agents."""
    if not pending_plans:
        return context, current_df, "No changes to apply."

    # Initialize agents
    engineer = EngineerAgent()
    tester = TesterAgent()

    changes_made = []
    all_code = []
    all_validations = []
    df = current_df.copy()

    # Execute each transformation
    for plan in pending_plans:
        try:
            before_df = df.copy()
            result_df, code = engineer.execute_on_dataframe(df, plan)

            # Validate the transformation
            test_result = tester.validate(before_df, result_df, plan)
            all_validations.append(test_result)

            df = result_df

            trans_type = plan.transformation_type
            if hasattr(trans_type, 'value'):
                trans_type = trans_type.value
            cols = plan.get_target_column_names()
            changes_made.append(f"{trans_type} on {', '.join(cols) if cols else 'data'}")
            all_code.append(code)

        except Exception as e:
            return context, current_df, f"Error applying transformation: {e}"

    # Update context with new profile
    new_profile = generate_profile(df)
    context.current_profile = new_profile
    context.current_row_count = len(df)
    context.current_column_count = len(df.columns)

    # Create summary
    summary = f"✓ Applied {len(pending_plans)} transformation(s):\n"
    for i, change in enumerate(changes_made, 1):
        summary += f"  {i}. {change}\n"
    summary += f"\nData now has {len(df):,} rows × {len(df.columns)} columns."

    # Show quality check results
    all_passed = all(v.passed for v in all_validations)
    has_warnings = any(v.has_warnings() for v in all_validations)

    if all_passed and not has_warnings:
        summary += "\n\n✅ Quality Check: All validations passed"
    elif all_passed and has_warnings:
        summary += "\n\n⚠️  Quality Check: Passed with warnings"
        for v in all_validations:
            for issue in v.issues:
                if issue.severity.value == "warning":
                    summary += f"\n  - {issue.message}"
    else:
        summary += "\n\n❌ Quality Check: Issues detected"
        for v in all_validations:
            for issue in v.issues:
                if issue.severity.value in ["warning", "error"]:
                    summary += f"\n  - {issue.message}"

    # Show generated code
    summary += "\n\nGenerated pandas code:"
    for code in all_code:
        summary += f"\n  {code}"

    return context, df, summary


def print_profile(context):
    """Print the data profile."""
    print("\n" + "-" * 40)
    print(f"FILE: {context.original_filename}")
    print(f"SIZE: {context.current_row_count:,} rows x {context.current_column_count} columns")
    print("\nCOLUMNS:")
    if context.current_profile:
        for col in context.current_profile.columns:
            null_info = f" ({col.null_count} missing)" if col.null_count > 0 else ""
            print(f"  {col.name}: {col.dtype}{null_info}")
    print("-" * 40 + "\n")


def simulate_transformation(context, plan):
    """Simulate applying a transformation by updating the context state."""
    trans_type = plan.transformation_type
    if hasattr(trans_type, 'value'):
        trans_type = trans_type.value

    columns = plan.get_target_column_names()

    if trans_type == "drop_rows" and columns:
        # Simulate removing rows with nulls in target column
        for col_name in columns:
            for col in context.current_profile.columns:
                if col.name == col_name:
                    # Reduce row count by the number of nulls
                    rows_removed = col.null_count
                    context.current_row_count -= rows_removed
                    # Set null count to 0 for this column
                    col.null_count = 0
                    col.null_percent = 0.0
                    break

    elif trans_type == "fill_nulls" and columns:
        # Simulate filling nulls
        for col_name in columns:
            for col in context.current_profile.columns:
                if col.name == col_name:
                    col.null_count = 0
                    col.null_percent = 0.0
                    break

    elif trans_type == "drop_columns" and columns:
        # Simulate removing columns
        context.current_profile.columns = [
            col for col in context.current_profile.columns
            if col.name not in columns
        ]
        context.current_column_count = len(context.current_profile.columns)

    elif trans_type == "rename_column" and columns:
        # Simulate renaming
        new_name = plan.parameters.get("new_name", "renamed")
        for col in context.current_profile.columns:
            if col.name == columns[0]:
                col.name = new_name
                break

    return context


def extract_suggestion_from_response(response):
    """Extract the actionable suggestion from the last assistant response."""
    response_lower = response.lower()

    # Check for email-related suggestions
    if any(phrase in response_lower for phrase in [
        "missing email", "email entries", "fix the email", "clean up the email",
        "address the email", "handle the email", "email column", "50 missing"
    ]):
        return "remove rows where email is missing"

    # Check for age-related suggestions
    if any(phrase in response_lower for phrase in [
        "missing age", "age column", "fill the age", "25 missing"
    ]):
        return "fill missing ages with 0"

    # Check for name-related suggestions
    if any(phrase in response_lower for phrase in [
        "missing name", "name column", "10 missing name"
    ]):
        return "fill missing names with Unknown"

    # Check for duplicate suggestions
    if "duplicate" in response_lower:
        return "remove duplicate rows"

    # Check for general "missing values" when context suggests starting with emails
    if "missing values" in response_lower and "email" in response_lower:
        return "remove rows where email is missing"

    # Default: if asking about addressing/fixing/cleaning and we have issues
    if any(phrase in response_lower for phrase in [
        "start by", "begin with", "address these", "fix these", "clean these"
    ]):
        return "remove rows where email is missing"  # Start with biggest issue

    return None


def is_apply_request(message):
    """Check if user wants to apply pending changes."""
    apply_words = ["apply", "save", "execute", "commit", "do it all", "run it", "make it happen"]
    msg_lower = message.lower().strip()
    return msg_lower in apply_words or msg_lower.startswith("apply")


def create_fresh_context(is_demo: bool, csv_path: str = None):
    """Create a fresh context based on mode. Returns (context, dataframe)."""
    if is_demo:
        return create_sample_context()
    else:
        return load_csv_context(csv_path)


def main():
    """Main chat loop."""
    # Check for CSV file argument
    is_demo = False
    csv_path = None

    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
        try:
            context, current_df = load_csv_context(csv_path)
        except FileNotFoundError as e:
            print(f"\nError: {e}")
            print("Usage: poetry run python scripts/chat_interactive.py <path_to_csv>")
            sys.exit(1)
        except ValueError as e:
            print(f"\nError: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"\nError loading CSV: {e}")
            sys.exit(1)
    else:
        # Demo mode with sample data
        is_demo = True
        context, current_df = create_sample_context()

    # Store original DataFrame for /clear
    original_df = current_df.copy()

    print_header(context, is_demo=is_demo)
    conversation_history = []
    pending_plans = []  # Queue of transformations to apply
    last_suggestion = None

    while True:
        try:
            # Get user input
            user_input = input("You: ").strip()

            if not user_input:
                continue

            # Handle commands
            if user_input.lower() in ["/quit", "/exit", "/q"]:
                if pending_plans:
                    print(f"\n  Warning: You have {len(pending_plans)} pending changes that will be lost.")
                print("\nAssistant: Goodbye! Happy data cleaning!\n")
                break

            if user_input.lower() == "/help":
                print_help()
                continue

            if user_input.lower() == "/profile":
                print_profile(context)
                continue

            if user_input.lower() in ["/queue", "/pending"]:
                print_queue(pending_plans, context)
                continue

            if user_input.lower() == "/clear":
                if pending_plans:
                    pending_plans = []
                    context, current_df = create_fresh_context(is_demo, csv_path)
                    print("\n  Cleared all pending changes. Starting fresh!\n")
                else:
                    print("\n  No pending changes to clear.\n")
                continue

            if user_input.lower() == "/apply" or is_apply_request(user_input):
                if pending_plans:
                    # Apply all transformations using the Engineer agent
                    context, current_df, summary = apply_all_changes(pending_plans, context, current_df)
                    print(f"\nAssistant: {summary}")
                    print("\n  What would you like to do next?\n")
                    pending_plans = []
                    last_suggestion = None
                else:
                    print("\nAssistant: No pending changes to apply. Tell me what you'd like to fix!\n")
                continue

            print()  # Blank line before response

            # Handle affirmative responses by including context
            effective_message = user_input
            if is_affirmative(user_input) and last_suggestion:
                effective_message = last_suggestion
                print(f"  [Understanding: \"{last_suggestion}\"]\n")

            # Check if this is a transformation request
            if is_transformation_request(effective_message) or (is_affirmative(user_input) and last_suggestion):
                # Use the Strategist agent
                with patch("agents.strategist.build_conversation_context") as mock_build:
                    mock_build.return_value = context

                    try:
                        agent = StrategistAgent()
                        plan = agent.create_plan(
                            session_id=context.session_id,
                            user_message=effective_message
                        )

                        # Add to pending queue instead of applying immediately
                        pending_plans.append(plan)

                        # Preview the effect (simulate for display only)
                        context = simulate_transformation(context, plan)

                        # Build response
                        trans_type = plan.transformation_type
                        if hasattr(trans_type, 'value'):
                            trans_type = trans_type.value
                        cols = plan.get_target_column_names()

                        response = f"Queued: {trans_type.upper()} on {', '.join(cols) if cols else 'data'}."
                        response += f"\n\n[{len(pending_plans)} change(s) pending]"

                        # Check for remaining issues
                        issues = get_data_issues(context)
                        if issues:
                            next_issue = issues[0]
                            response += f"\n\n'{next_issue['column']}' still has {next_issue['count']} missing values. Fix those too, or say 'apply' to create this transformation batch?"
                            # Set suggestion for next affirmative
                            if next_issue['column'] == 'age':
                                last_suggestion = "fill missing ages with the average"
                            elif next_issue['column'] == 'name':
                                last_suggestion = "fill missing names with Unknown"
                            elif next_issue['column'] == 'email':
                                last_suggestion = "remove rows where email is missing"
                            else:
                                last_suggestion = f"remove rows where {next_issue['column']} is missing"
                        else:
                            response += "\n\nNo more issues detected. Say 'apply' to create this transformation batch!"
                            last_suggestion = None

                        print(f"Assistant: {response}\n")

                        # Add to history
                        conversation_history.append({"role": "user", "content": user_input})
                        conversation_history.append({"role": "assistant", "content": response})

                    except StrategyError as e:
                        print(f"Assistant: I had trouble understanding that. {e.message}")
                        if e.suggestion:
                            print(f"           {e.suggestion}")
                        print()

            else:
                # General conversation - use OpenAI directly for chat
                try:
                    response = get_conversational_response(user_input, context, conversation_history)
                    print(f"Assistant: {response}\n")

                    # Extract suggestion for next turn
                    last_suggestion = extract_suggestion_from_response(response)

                    # Add to history
                    conversation_history.append({"role": "user", "content": user_input})
                    conversation_history.append({"role": "assistant", "content": response})

                except Exception as e:
                    print(f"Assistant: Sorry, I had trouble responding. {str(e)}\n")

        except KeyboardInterrupt:
            print("\n\nAssistant: Goodbye!\n")
            break
        except EOFError:
            print("\n\nAssistant: Goodbye!\n")
            break


if __name__ == "__main__":
    main()
