# =============================================================================
# agents/chat_handler.py - Chat Request Handler
# =============================================================================
# This module provides the main entry point for processing chat requests.
# It handles both plan mode and transform mode:
#
# - Plan Mode: Creates a plan and returns it for user review
# - Transform Mode: Creates plan, generates code, executes, returns result
#
# Usage:
#   from agents.chat_handler import handle_chat_request
#   response = handle_chat_request(session_id, message, mode="plan")
# =============================================================================

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from core.models.chat import ChatMode, ChatRequest, PlanResponse
from agents.strategist import StrategistAgent, StrategyError
from agents.models.technical_plan import TechnicalPlan

# Set up logging for this module
logger = logging.getLogger(__name__)


# =============================================================================
# Chat Handler
# =============================================================================

def handle_chat_request(
    session_id: str | UUID,
    message: str,
    mode: ChatMode | str = ChatMode.PLAN,
) -> PlanResponse | dict[str, Any]:
    """
    Process a chat request in the specified mode.

    This is the main entry point for the chat system.
    It routes requests based on the mode:

    - PLAN: Creates and returns the transformation plan for review
    - TRANSFORM: Full execution (requires Engineer/Tester - not yet implemented)

    Args:
        session_id: The session UUID
        message: User's natural language request
        mode: ChatMode.PLAN or ChatMode.TRANSFORM

    Returns:
        PlanResponse for plan mode, or TaskResult for transform mode

    Raises:
        StrategyError: If plan creation fails
        NotImplementedError: If transform mode is requested (not yet implemented)

    Example:
        # Plan mode - preview the transformation
        response = handle_chat_request(
            session_id="550e8400-...",
            message="remove rows where email is blank",
            mode="plan"
        )
        print(response.plan)
        print(response.assistant_message)

        # If user approves, send with transform mode
        result = handle_chat_request(
            session_id="550e8400-...",
            message="remove rows where email is blank",
            mode="transform"
        )
    """
    session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id

    # Normalize mode to enum
    if isinstance(mode, str):
        mode = ChatMode(mode)

    logger.info(f"Processing chat request in {mode.value} mode: '{message[:50]}...'")

    # ---------------------------------------------------------------------
    # Step 1: Create the Strategist Agent and get the plan
    # ---------------------------------------------------------------------
    agent = StrategistAgent()

    try:
        plan = agent.create_plan(session_id=session_id_str, user_message=message)
    except StrategyError as e:
        logger.error(f"Strategy error: {e}")
        raise

    # ---------------------------------------------------------------------
    # Step 2: Route based on mode
    # ---------------------------------------------------------------------
    if mode == ChatMode.PLAN:
        return _handle_plan_mode(session_id_str, plan)
    else:
        return _handle_transform_mode(session_id_str, plan)


def _handle_plan_mode(
    session_id: str,
    plan: TechnicalPlan,
) -> PlanResponse:
    """
    Handle plan mode - return the plan for user review.

    In plan mode:
    - Shows what transformation will be applied
    - Shows the confidence level
    - If confidence is low, asks for clarification
    - Does NOT execute the transformation

    Args:
        session_id: The session UUID
        plan: TechnicalPlan from the Strategist

    Returns:
        PlanResponse with plan details and assistant message
    """
    # Build assistant message based on confidence
    if plan.needs_clarification():
        assistant_message = (
            f"I need a bit more information. {plan.clarification_needed}"
        )
        can_execute = False
    else:
        assistant_message = _build_assistant_message(plan)
        can_execute = True

    # Build estimated impact (basic for now, can be enhanced)
    estimated_impact = None
    if plan.conditions:
        estimated_impact = {
            "description": f"Will affect rows matching {len(plan.conditions)} condition(s)",
            "conditions": [
                f"{c.column} {c.operator}" for c in plan.conditions
            ]
        }

    return PlanResponse(
        session_id=session_id,
        mode=ChatMode.PLAN,
        plan=plan.model_dump(),
        generated_code=None,  # Engineer not implemented yet
        estimated_impact=estimated_impact,
        assistant_message=assistant_message,
        can_execute=can_execute,
        clarification_needed=plan.clarification_needed,
    )


def _handle_transform_mode(
    session_id: str,
    plan: TechnicalPlan,
) -> dict[str, Any]:
    """
    Handle transform mode - execute the transformation.

    In transform mode:
    - Plan is passed to Engineer to generate code
    - Code is passed to Tester to validate and execute
    - New node is created with transformed data

    NOTE: This is not yet implemented - requires Engineer and Tester agents.

    Args:
        session_id: The session UUID
        plan: TechnicalPlan from the Strategist

    Returns:
        TaskResult with new node ID and transformation details

    Raises:
        NotImplementedError: Engineer/Tester not yet implemented
    """
    # Check if clarification is needed
    if plan.needs_clarification():
        raise StrategyError(
            message="Cannot execute transformation - clarification needed",
            code="CLARIFICATION_REQUIRED",
            suggestion=plan.clarification_needed,
            details={"plan": plan.model_dump()}
        )

    # TODO: Implement when Engineer and Tester are ready
    # For now, return a placeholder indicating what would happen
    raise NotImplementedError(
        "Transform mode requires Engineer and Tester agents (Milestone 4). "
        "Use plan mode to preview transformations. "
        f"Plan would execute: {plan.transformation_type} on {plan.get_target_column_names()}"
    )


def _build_assistant_message(plan: TechnicalPlan) -> str:
    """
    Build a natural language message explaining the plan.

    Args:
        plan: The TechnicalPlan

    Returns:
        Human-friendly message describing what will happen
    """
    # Handle undo specially
    if plan.is_undo():
        return "I'll undo the last transformation and restore the previous version of your data."

    # Build message based on transformation type
    trans_type = plan.transformation_type
    if hasattr(trans_type, 'value'):
        trans_type = trans_type.value

    columns = plan.get_target_column_names()
    columns_str = ", ".join(columns) if columns else "the data"

    messages = {
        "drop_rows": f"I'll remove rows from {columns_str} based on your conditions.",
        "filter_rows": f"I'll keep only the rows that match your conditions in {columns_str}.",
        "deduplicate": f"I'll remove duplicate rows from your data.",
        "drop_columns": f"I'll remove the column(s): {columns_str}.",
        "rename_column": f"I'll rename {columns_str}.",
        "fill_nulls": f"I'll fill missing values in {columns_str}.",
        "replace_values": f"I'll replace specific values in {columns_str}.",
        "standardize": f"I'll standardize the format of {columns_str}.",
        "trim_whitespace": f"I'll remove extra whitespace from {columns_str}.",
        "change_case": f"I'll change the case of text in {columns_str}.",
        "parse_date": f"I'll parse {columns_str} as dates.",
        "format_date": f"I'll reformat the dates in {columns_str}.",
        "convert_type": f"I'll convert the data type of {columns_str}.",
    }

    base_message = messages.get(trans_type, plan.explanation)

    # Add confidence note if not very high
    if plan.confidence < 0.9:
        confidence_pct = int(plan.confidence * 100)
        base_message += f" (Confidence: {confidence_pct}%)"

    return base_message


# =============================================================================
# Convenience Functions
# =============================================================================

def preview_transformation(
    session_id: str | UUID,
    message: str,
) -> PlanResponse:
    """
    Preview a transformation without executing.

    Shorthand for handle_chat_request with mode="plan".

    Args:
        session_id: The session UUID
        message: User's natural language request

    Returns:
        PlanResponse with plan details

    Example:
        preview = preview_transformation(
            session_id="550e8400-...",
            message="remove blank emails"
        )
        if preview.can_execute:
            print(f"Will do: {preview.assistant_message}")
    """
    return handle_chat_request(session_id, message, mode=ChatMode.PLAN)


def chat(
    session_id: str | UUID,
    message: str,
    execute: bool = False,
) -> PlanResponse | dict[str, Any]:
    """
    Chat with the agent.

    Simpler interface for chat requests.

    Args:
        session_id: The session UUID
        message: User's natural language request
        execute: If True, execute the transformation; if False, just preview

    Returns:
        PlanResponse (preview) or TaskResult (execute)

    Example:
        # Preview
        response = chat(session_id, "remove blank rows", execute=False)

        # Execute (when implemented)
        result = chat(session_id, "remove blank rows", execute=True)
    """
    mode = ChatMode.TRANSFORM if execute else ChatMode.PLAN
    return handle_chat_request(session_id, message, mode=mode)
