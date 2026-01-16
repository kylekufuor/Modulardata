# =============================================================================
# agents/response_generator.py - Conversational AI Response Generator
# =============================================================================
# Generates friendly, conversational responses for the chat interface.
# Uses OpenAI to create natural language responses with proactive suggestions.
# =============================================================================

import logging

logger = logging.getLogger(__name__)

# Lazy-loaded OpenAI client
_client = None


def get_openai_client():
    """Get or create OpenAI client (lazy initialization)."""
    global _client
    if _client is None:
        from openai import OpenAI
        from app.config import settings
        _client = OpenAI(api_key=settings.OPENAI_API_KEY)
    return _client


def build_data_summary(profile_data: dict, filename: str = "data.csv") -> str:
    """
    Build a text summary of the data for the AI context.

    Args:
        profile_data: Profile dict from the node
        filename: Original filename

    Returns:
        Text summary for the AI
    """
    if not profile_data:
        return "No data profile available."

    row_count = profile_data.get("row_count", 0)
    column_count = profile_data.get("column_count", 0)
    columns = profile_data.get("columns", [])

    lines = [
        f"The user has data with {row_count:,} rows and {column_count} columns.",
        "",
        "Columns:",
    ]

    issues = []
    for col in columns:
        name = col.get("name", "unknown")
        dtype = col.get("dtype", "unknown")
        semantic_type = col.get("semantic_type", "unknown")
        null_count = col.get("null_count", 0)
        null_percent = col.get("null_percent", 0)

        null_info = f" - {null_count} missing values ({null_percent:.1f}%)" if null_count > 0 else " - no missing values"
        lines.append(f"  - {name} ({dtype}, {semantic_type}){null_info}")

        if null_count > 0:
            issues.append({
                "column": name,
                "count": null_count,
                "percent": null_percent,
            })

    if issues:
        lines.append("")
        lines.append("Data quality issues:")
        for issue in sorted(issues, key=lambda x: x["count"], reverse=True)[:5]:
            lines.append(f"  - {issue['column']} has {issue['count']} missing values ({issue['percent']:.1f}%)")

    return "\n".join(lines)


def generate_conversational_response(
    message: str,
    profile_data: dict,
    conversation_history: list[dict] | None = None,
    filename: str = "data.csv",
) -> str:
    """
    Generate a friendly conversational response for general questions.

    Args:
        message: User's message
        profile_data: Current data profile
        conversation_history: Previous messages
        filename: Original filename

    Returns:
        Friendly AI response
    """
    data_summary = build_data_summary(profile_data, filename)

    system_prompt = f"""You are a friendly data cleaning assistant. You help users understand and clean their data through natural conversation.

Current data context:
{data_summary}

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
    if conversation_history:
        for msg in conversation_history[-10:]:
            messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})

    # Add current message
    messages.append({"role": "user", "content": message})

    try:
        client = get_openai_client()
        response = client.chat.completions.create(
            model="gpt-4-turbo",
            messages=messages,
            temperature=0.7,
            max_tokens=350,
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        # Fallback to a simple response based on data summary
        return _generate_fallback_response(profile_data)


def _generate_fallback_response(profile_data: dict) -> str:
    """Generate a fallback response when OpenAI is not available."""
    if not profile_data:
        return "I can see your data is loaded. What would you like to do with it?"

    row_count = profile_data.get("row_count", 0)
    column_count = profile_data.get("column_count", 0)
    columns = profile_data.get("columns", [])

    # Find issues
    issues = []
    for col in columns:
        null_count = col.get("null_count", 0)
        if null_count > 0:
            issues.append(f"{col.get('name', 'unknown')} has {null_count} missing values")

    response = f"Your data has {row_count:,} rows and {column_count} columns."

    if issues:
        response += f" I noticed some data quality issues: {issues[0]}."
        response += " Would you like me to help clean that up?"
    else:
        response += " The data looks clean! What would you like to do with it?"

    return response


def generate_transformation_response(
    plan_explanation: str,
    transformation_type: str,
    target_columns: list[str],
    confidence: float,
    profile_data: dict,
    clarification_needed: str | None = None,
) -> str:
    """
    Generate a friendly response for a transformation being added to the plan.

    Args:
        plan_explanation: The plan's explanation (should be used as the primary response)
        transformation_type: Type of transformation
        target_columns: Columns being transformed
        confidence: Confidence level (0-1)
        profile_data: Current data profile
        clarification_needed: Clarification question if any

    Returns:
        Friendly response with proactive suggestion
    """
    if clarification_needed:
        return f"I need a bit more information. {clarification_needed}"

    # Build confidence phrase
    if confidence >= 0.9:
        conf_phrase = "I'll"
    elif confidence >= 0.7:
        conf_phrase = "I think I should"
    else:
        conf_phrase = "I'm not entirely sure, but I could"

    # Use the plan_explanation directly - it contains the specific user request
    # Only fall back to templates for very generic cases
    if plan_explanation and len(plan_explanation) > 10:
        # Use the actual explanation from the Strategist
        base_response = f"{conf_phrase} {plan_explanation[0].lower()}{plan_explanation[1:]}"
    else:
        # Fallback to simple templates only when explanation is missing
        cols_str = ", ".join(target_columns) if target_columns else "the data"
        action_templates = {
            "drop_rows": f"{conf_phrase} remove the matching rows",
            "filter_rows": f"{conf_phrase} keep only the rows that match your criteria",
            "deduplicate": f"{conf_phrase} remove duplicate rows from your data",
            "drop_columns": f"{conf_phrase} remove the {cols_str} column{'s' if len(target_columns) > 1 else ''}",
            "rename_column": f"{conf_phrase} rename '{target_columns[0] if target_columns else 'the column'}'",
            "fill_nulls": f"{conf_phrase} fill the missing values in {cols_str}",
            "replace_values": f"{conf_phrase} replace values in {cols_str}",
            "standardize": f"{conf_phrase} standardize the format of {cols_str}",
            "trim_whitespace": f"{conf_phrase} clean up extra spaces in {cols_str}",
            "change_case": f"{conf_phrase} change the text case in {cols_str}",
            "parse_date": f"{conf_phrase} convert {cols_str} to a proper date format",
            "format_date": f"{conf_phrase} reformat the dates in {cols_str}",
            "convert_type": f"{conf_phrase} convert the data type of {cols_str}",
            "undo": "I'll undo the last change and restore your previous version",
        }
        base_response = action_templates.get(transformation_type, f"{conf_phrase} apply the transformation")

    # Add period if not already there
    if not base_response.endswith("."):
        base_response += "."

    # Add proactive suggestion based on remaining issues
    suggestion = get_proactive_suggestion(profile_data, target_columns)
    if suggestion:
        base_response += f"\n\n{suggestion}"

    return base_response


def get_proactive_suggestion(profile_data: dict, just_fixed_columns: list[str] | None = None) -> str:
    """
    Generate a proactive suggestion for next steps.

    Args:
        profile_data: Current data profile
        just_fixed_columns: Columns that were just addressed

    Returns:
        Suggestion string or empty string
    """
    if not profile_data:
        return "What else would you like to do?"

    columns = profile_data.get("columns", [])

    # Find columns with issues (excluding ones just fixed)
    issues = []
    for col in columns:
        name = col.get("name", "")
        null_count = col.get("null_count", 0)

        if null_count > 0 and name not in (just_fixed_columns or []):
            issues.append({
                "column": name,
                "count": null_count,
            })

    if not issues:
        # Don't say "data is clean" - transformations haven't been applied yet
        # Just ask what else they want to do
        return "What else would you like to do?"

    # Suggest fixing the biggest issue
    issues.sort(key=lambda x: x["count"], reverse=True)
    top_issue = issues[0]

    return f"'{top_issue['column']}' has {top_issue['count']} missing values. Want me to fix those too?"


def generate_plan_added_response(
    plan_explanation: str,
    transformation_type: str,
    target_columns: list[str],
    confidence: float,
    step_count: int,
    profile_data: dict,
    should_suggest_apply: bool = False,
) -> str:
    """
    Generate a friendly response when a transformation is added to the plan.

    Args:
        plan_explanation: The plan's explanation
        transformation_type: Type of transformation
        target_columns: Target columns
        confidence: Confidence level
        step_count: Current number of steps in plan
        profile_data: Current data profile
        should_suggest_apply: Whether to suggest applying the plan

    Returns:
        Friendly assistant response
    """
    # Get the transformation response
    trans_response = generate_transformation_response(
        plan_explanation=plan_explanation,
        transformation_type=transformation_type,
        target_columns=target_columns,
        confidence=confidence,
        profile_data=profile_data,
    )

    # Add plan status - only suggest applying when 3+ steps
    if should_suggest_apply:
        plan_status = f"\n\nAdded to your plan ({step_count} steps queued). Ready to apply them all, or want to add more?"
    else:
        plan_status = f"\n\nAdded to your plan ({step_count} step{'s' if step_count > 1 else ''} queued)."

    return trans_response + plan_status
