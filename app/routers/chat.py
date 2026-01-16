# =============================================================================
# app/routers/chat.py - Conversational Data Transformation Endpoints
# =============================================================================
# Handles the chat interface for Plan Mode data transformation.
#
# Flow:
# 1. User sends chat message -> Strategist creates plan step
# 2. Plan accumulates (default: suggest apply at 3 steps)
# 3. User applies plan -> Celery worker executes transformations
# =============================================================================

import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Path, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel, Field

from app.auth import get_current_user, AuthUser
from app.exceptions import SessionNotFoundError
from core.services.session_service import SessionService
from core.services.node_service import NodeService
from core.services.plan_service import PlanService
from core.models.plan import (
    ApplyPlanRequest,
    ApplyPlanResponse,
    PlanStatus,
    SessionPlanResponse,
)
from agents.response_generator import (
    generate_plan_added_response,
    generate_conversational_response,
)
from agents.guardrails import check_message

logger = logging.getLogger(__name__)

router = APIRouter()


# =============================================================================
# Request/Response Models
# =============================================================================

class ChatRequest(BaseModel):
    """Request to send a chat message."""
    message: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        description="Natural language instruction for data transformation",
        examples=[
            "remove rows where email is blank",
            "trim whitespace from all columns",
            "convert date column to YYYY-MM-DD format",
            "deduplicate based on email column",
            "fill missing values in age with the average",
        ]
    )
    mode: str = Field(
        default="plan",
        description="Chat mode: 'plan' to queue transformations, 'transform' to execute immediately",
        examples=["plan", "transform"]
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"message": "remove rows where email is blank", "mode": "plan"},
                {"message": "trim whitespace from the name column", "mode": "transform"},
            ]
        }
    }


class ChatResponse(BaseModel):
    """Response from chat endpoint in Plan Mode."""
    session_id: str = Field(..., example="550e8400-e29b-41d4-a716-446655440000")
    message: str = Field(..., example="remove rows where email is blank")
    plan: SessionPlanResponse
    assistant_response: str = Field(
        ...,
        example="Added to plan: Remove rows where email column is empty or null.\n\nPlan now has 1 transformation(s). Keep adding or say 'apply' when ready."
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "message": "remove rows where email is blank",
                "plan": {
                    "id": "plan_123",
                    "status": "draft",
                    "step_count": 1,
                    "steps": [
                        {
                            "step_number": 1,
                            "transformation_type": "drop_rows",
                            "explanation": "Remove rows where email column is empty or null",
                            "target_columns": ["email"]
                        }
                    ]
                },
                "assistant_response": "Added to plan: Remove rows where email column is empty or null.\n\nPlan now has 1 transformation(s). Keep adding or say 'apply' when ready."
            }
        }
    }


# =============================================================================
# Chat Endpoint
# =============================================================================

@router.post("/{session_id}/chat", response_model=ChatResponse)
async def chat(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    request: ChatRequest,
    user: AuthUser = Depends(get_current_user),
):
    """
    Send a chat message to transform data (Plan Mode).

    In Plan Mode:
    1. Your message is analyzed by the Strategist AI
    2. A transformation step is added to your plan
    3. After 3 steps, we suggest applying the plan
    4. Use POST /plan/apply to execute all transformations

    Examples:
    - "remove rows where email is blank"
    - "trim whitespace from all columns"
    - "convert date column to YYYY-MM-DD format"
    - "show me the plan" (view current plan)
    - "clear the plan" (start over)
    - "apply" (execute planned transformations)

    User must own the session.
    """
    session_id_str = str(session_id)

    # Verify session exists and user owns it
    try:
        SessionService.get_session(session_id_str, user_id=user.id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    message_lower = request.message.lower().strip()

    # Handle special commands using flexible matching
    # Show plan commands
    if _matches_command(message_lower, ["show plan", "show me the plan", "what's the plan", "view plan"]):
        return await _handle_show_plan(session_id_str, request.message)

    # Clear plan commands
    if _matches_command(message_lower, ["clear", "clear plan", "start over", "start fresh", "reset", "reset plan"]):
        return await _handle_clear_plan(session_id_str, request.message)

    # Apply plan commands
    if _matches_command(message_lower, ["apply", "apply plan", "apply changes", "execute", "do it", "run it"]):
        # Redirect to apply endpoint
        raise HTTPException(
            status_code=400,
            detail="Use POST /api/v1/sessions/{session_id}/plan/apply to execute the plan"
        )

    # Process with Strategist
    return await _handle_chat_message(session_id_str, request.message)


def _matches_command(message: str, patterns: list[str]) -> bool:
    """
    Check if message matches any command pattern using flexible matching.

    Supports:
    - Exact match: "clear plan"
    - Contains match: "clear the plan and start fresh" matches "clear" and "plan"
    - Prefix match: "clear plan please" starts with "clear plan"
    """
    # Exact match
    if message in patterns:
        return True

    # Check for key phrases contained in message
    for pattern in patterns:
        # If pattern is in message and it's a significant part
        if pattern in message:
            return True

        # Check for multi-word pattern components
        pattern_words = pattern.split()
        if len(pattern_words) > 1:
            # All words from pattern should be in message
            if all(word in message for word in pattern_words):
                return True

    return False


async def _handle_show_plan(session_id: str, user_message: str) -> ChatResponse:
    """Handle 'show plan' command."""
    plan = PlanService.get_or_create_plan(session_id)

    if not plan.steps:
        assistant_response = "You don't have any transformations planned yet. What would you like to do with your data? I can help you clean up missing values, remove duplicates, standardize formats, and more!"
    else:
        summary = plan.to_summary()
        assistant_response = f"Here's what I have queued up for you:\n\n{summary}\n\nWant to add more changes, or shall I apply these?"

    _save_chat_messages(session_id, user_message, assistant_response)

    return ChatResponse(
        session_id=session_id,
        message=user_message,
        plan=SessionPlanResponse.from_plan(plan),
        assistant_response=assistant_response,
    )


async def _handle_clear_plan(session_id: str, user_message: str) -> ChatResponse:
    """Handle 'clear plan' command."""
    plan = PlanService.clear_plan(session_id)
    assistant_response = "All cleared! We're starting fresh. What would you like to do with your data?"

    _save_chat_messages(session_id, user_message, assistant_response)

    return ChatResponse(
        session_id=session_id,
        message=user_message,
        plan=SessionPlanResponse.from_plan(plan),
        assistant_response=assistant_response,
    )


def _save_chat_messages(session_id: str, user_message: str, assistant_response: str) -> None:
    """Save both user message and assistant response to chat_logs."""
    from lib.supabase_client import SupabaseClient

    try:
        # Save user message
        SupabaseClient.insert_chat_message(
            session_id=session_id,
            role="user",
            content=user_message,
        )

        # Save assistant response
        SupabaseClient.insert_chat_message(
            session_id=session_id,
            role="assistant",
            content=assistant_response,
        )

        logger.debug(f"Saved chat messages for session {session_id}")
    except Exception as e:
        logger.warning(f"Failed to save chat messages: {e}")


async def _handle_chat_message(session_id: str, message: str) -> ChatResponse:
    """Process a chat message through the Strategist with conversational AI."""
    from agents.strategist import StrategistAgent

    try:
        # Get current data profile for context
        from lib.supabase_client import SupabaseClient
        current_node = SupabaseClient.fetch_current_node(session_id)

        if not current_node:
            raise HTTPException(
                status_code=400,
                detail="No data uploaded. Please upload a CSV file first."
            )

        # Get profile data for conversational context
        profile_data = current_node.get("profile_json", {})

        # =====================================================================
        # GUARDRAILS: Check if message is on-topic
        # =====================================================================
        is_on_topic, redirect_response = check_message(message, profile_data)

        if not is_on_topic and redirect_response:
            # Return redirect response for off-topic messages
            plan = PlanService.get_or_create_plan(session_id)
            _save_chat_messages(session_id, message, redirect_response)
            return ChatResponse(
                session_id=session_id,
                message=message,
                plan=SessionPlanResponse.from_plan(plan),
                assistant_response=redirect_response,
            )

        # Check if this is a general question vs transformation request
        is_question = _is_general_question(message)

        if is_question:
            # Use conversational AI for general questions
            plan = PlanService.get_or_create_plan(session_id)
            assistant_response = generate_conversational_response(
                message=message,
                profile_data=profile_data,
            )
            _save_chat_messages(session_id, message, assistant_response)
            return ChatResponse(
                session_id=session_id,
                message=message,
                plan=SessionPlanResponse.from_plan(plan),
                assistant_response=assistant_response,
            )

        # Run Strategist to create plan
        strategist = StrategistAgent()
        technical_plan = strategist.create_plan(session_id, message)

        if not technical_plan:
            # Strategist couldn't understand - use conversational AI
            plan = PlanService.get_or_create_plan(session_id)
            assistant_response = generate_conversational_response(
                message=message,
                profile_data=profile_data,
            )
            _save_chat_messages(session_id, message, assistant_response)
            return ChatResponse(
                session_id=session_id,
                message=message,
                plan=SessionPlanResponse.from_plan(plan),
                assistant_response=assistant_response,
            )

        # Add step to plan
        trans_type = technical_plan.transformation_type
        if hasattr(trans_type, 'value'):
            trans_type = trans_type.value

        target_columns = [tc.column_name for tc in technical_plan.target_columns] if technical_plan.target_columns else []

        plan = PlanService.add_step(
            session_id=session_id,
            transformation_type=str(trans_type),
            explanation=technical_plan.explanation,
            target_columns=target_columns,
            parameters=technical_plan.parameters,
            estimated_rows_affected=None,
            code_preview=None,
        )

        # Generate friendly conversational response
        assistant_response = generate_plan_added_response(
            plan_explanation=technical_plan.explanation,
            transformation_type=str(trans_type),
            target_columns=target_columns,
            confidence=technical_plan.confidence,
            step_count=len(plan.steps),
            profile_data=profile_data,
            should_suggest_apply=plan.should_suggest_apply(),
        )

        # Save messages to database
        _save_chat_messages(session_id, message, assistant_response)

        return ChatResponse(
            session_id=session_id,
            message=message,
            plan=SessionPlanResponse.from_plan(plan),
            assistant_response=assistant_response,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Chat processing failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to process message: {str(e)}")


def _is_general_question(message: str) -> bool:
    """
    Check if the message is a general question vs transformation request.

    General questions are handled by conversational AI.
    Transformation requests go through the Strategist.
    """
    message_lower = message.lower().strip()

    # Question patterns
    question_starters = [
        "what is", "what are", "what's", "whats",
        "how many", "how much",
        "tell me about", "describe", "explain",
        "show me", "can you tell",
        "what does", "what do",
        "is there", "are there",
        "do i have", "does this",
    ]

    # Check if it starts with a question pattern
    for starter in question_starters:
        if message_lower.startswith(starter):
            return True

    # Check if it ends with a question mark but doesn't have transformation keywords
    if message_lower.endswith("?"):
        transform_keywords = [
            "remove", "delete", "drop", "clean", "fill", "replace", "rename",
            "convert", "format", "deduplicate", "merge", "split", "trim",
            "standardize", "fix", "change", "update", "undo", "filter",
        ]
        if not any(keyword in message_lower for keyword in transform_keywords):
            return True

    return False


# =============================================================================
# Plan Management Endpoints
# =============================================================================

@router.get("/{session_id}/plan", response_model=SessionPlanResponse)
async def get_plan(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    user: AuthUser = Depends(get_current_user),
):
    """
    Get the current plan for a session.

    Returns the accumulated transformation steps and status.
    User must own the session.
    """
    session_id_str = str(session_id)

    try:
        SessionService.get_session(session_id_str, user_id=user.id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    plan = PlanService.get_or_create_plan(session_id_str)
    return SessionPlanResponse.from_plan(plan)


@router.post("/{session_id}/plan/apply", response_model=ApplyPlanResponse)
async def apply_plan(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    request: ApplyPlanRequest | None = None,
    background_tasks: BackgroundTasks = None,
    user: AuthUser = Depends(get_current_user),
):
    """
    Apply the current plan to create a new data version.

    Modes:
    - "all": Apply all steps at once (default)
    - "one_by_one": Each step creates its own node
    - "steps": Apply specific step numbers only

    Returns a task ID for tracking progress.
    User must own the session.
    """
    session_id_str = str(session_id)

    try:
        SessionService.get_session(session_id_str, user_id=user.id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    plan = PlanService.get_or_create_plan(session_id_str)

    if not plan.steps:
        return ApplyPlanResponse(
            success=False,
            message="No transformations to apply. Add some changes first!",
        )

    # Determine which steps to apply
    mode = request.mode if request else "all"
    steps_to_apply = plan.steps

    if mode == "steps" and request and request.step_numbers:
        steps_to_apply = [s for s in plan.steps if s.step_number in request.step_numbers]
        if not steps_to_apply:
            return ApplyPlanResponse(
                success=False,
                message=f"No valid steps found for numbers: {request.step_numbers}",
            )

    # Submit to Celery worker
    try:
        from workers.tasks import process_plan_apply

        # Convert steps to dict for serialization
        steps_data = [
            {
                "step_number": s.step_number,
                "transformation_type": s.transformation_type,
                "target_columns": s.target_columns,
                "parameters": s.parameters,
                "explanation": s.explanation,
            }
            for s in steps_to_apply
        ]

        # Submit async task
        result = process_plan_apply.delay(
            session_id=session_id_str,
            plan_id=plan.id,
            steps=steps_data,
            mode=mode,
        )

        return ApplyPlanResponse(
            success=True,
            message=f"Applying {len(steps_to_apply)} transformation(s). Task ID: {result.id}",
        )

    except Exception as e:
        logger.exception(f"Failed to submit plan: {e}")

        # Fallback: Try synchronous execution for testing
        return ApplyPlanResponse(
            success=False,
            error=str(e),
            message="Failed to submit plan. Is Redis running?",
        )


@router.post("/{session_id}/plan/clear", response_model=SessionPlanResponse)
async def clear_plan(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    user: AuthUser = Depends(get_current_user),
):
    """
    Clear all steps from the current plan.

    Use this to start over with a fresh plan.
    User must own the session.
    """
    session_id_str = str(session_id)

    try:
        SessionService.get_session(session_id_str, user_id=user.id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    plan = PlanService.clear_plan(session_id_str)
    return SessionPlanResponse.from_plan(plan)
