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

from fastapi import APIRouter, Path, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from app.exceptions import SessionNotFoundError
from core.services.session_service import SessionService
from core.services.plan_service import PlanService
from core.models.plan import (
    ApplyPlanRequest,
    ApplyPlanResponse,
    PlanStatus,
    SessionPlanResponse,
)

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

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"message": "remove rows where email is blank"},
                {"message": "trim whitespace from the name column"},
                {"message": "convert prices to integers"},
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
    """
    session_id_str = str(session_id)

    # Verify session exists
    try:
        SessionService.get_session(session_id_str)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    message_lower = request.message.lower().strip()

    # Handle special commands
    if message_lower in ["show plan", "show me the plan", "what's the plan", "plan"]:
        return await _handle_show_plan(session_id_str)

    if message_lower in ["clear", "clear plan", "start over", "reset"]:
        return await _handle_clear_plan(session_id_str)

    if message_lower in ["apply", "do it", "execute", "apply plan", "apply changes"]:
        # Redirect to apply endpoint
        raise HTTPException(
            status_code=400,
            detail="Use POST /api/v1/sessions/{session_id}/plan/apply to execute the plan"
        )

    # Process with Strategist
    return await _handle_chat_message(session_id_str, request.message)


async def _handle_show_plan(session_id: str) -> ChatResponse:
    """Handle 'show plan' command."""
    plan = PlanService.get_or_create_plan(session_id)

    if not plan.steps:
        assistant_response = "You don't have any transformations planned yet. Tell me what you'd like to do with your data!"
    else:
        assistant_response = plan.to_summary()

    return ChatResponse(
        session_id=session_id,
        message="show plan",
        plan=SessionPlanResponse.from_plan(plan),
        assistant_response=assistant_response,
    )


async def _handle_clear_plan(session_id: str) -> ChatResponse:
    """Handle 'clear plan' command."""
    plan = PlanService.clear_plan(session_id)

    return ChatResponse(
        session_id=session_id,
        message="clear plan",
        plan=SessionPlanResponse.from_plan(plan),
        assistant_response="Plan cleared. What would you like to do with your data?",
    )


async def _handle_chat_message(session_id: str, message: str) -> ChatResponse:
    """Process a chat message through the Strategist."""
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

        # Run Strategist to create plan
        strategist = StrategistAgent()
        technical_plan = strategist.create_plan(session_id, message)

        if not technical_plan:
            # Strategist couldn't understand - return clarification
            plan = PlanService.get_or_create_plan(session_id)
            return ChatResponse(
                session_id=session_id,
                message=message,
                plan=SessionPlanResponse.from_plan(plan),
                assistant_response="I'm not sure what transformation you'd like. Could you be more specific? For example: 'remove rows where email is blank' or 'trim whitespace from all columns'.",
            )

        # Add step to plan
        # Note: transformation_type may be enum or string depending on use_enum_values
        trans_type = technical_plan.transformation_type
        if hasattr(trans_type, 'value'):
            trans_type = trans_type.value

        plan = PlanService.add_step(
            session_id=session_id,
            transformation_type=str(trans_type),
            explanation=technical_plan.explanation,
            target_columns=[tc.column_name for tc in technical_plan.target_columns] if technical_plan.target_columns else [],
            parameters=technical_plan.parameters,
            estimated_rows_affected=None,  # TODO: Estimate from profile
            code_preview=None,  # TODO: Generate preview
        )

        # Build response
        step_count = len(plan.steps)
        if plan.should_suggest_apply():
            assistant_response = (
                f"Added to plan: {technical_plan.explanation}\n\n"
                f"You now have {step_count} transformation(s) planned. "
                f"Ready to apply them? Use 'apply' or add more changes."
            )
        else:
            assistant_response = (
                f"Added to plan: {technical_plan.explanation}\n\n"
                f"Plan now has {step_count} transformation(s). "
                f"Keep adding or say 'apply' when ready."
            )

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


# =============================================================================
# Plan Management Endpoints
# =============================================================================

@router.get("/{session_id}/plan", response_model=SessionPlanResponse)
async def get_plan(
    session_id: Annotated[UUID, Path(description="Session UUID")],
):
    """
    Get the current plan for a session.

    Returns the accumulated transformation steps and status.
    """
    session_id_str = str(session_id)

    try:
        SessionService.get_session(session_id_str)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    plan = PlanService.get_or_create_plan(session_id_str)
    return SessionPlanResponse.from_plan(plan)


@router.post("/{session_id}/plan/apply", response_model=ApplyPlanResponse)
async def apply_plan(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    request: ApplyPlanRequest | None = None,
    background_tasks: BackgroundTasks = None,
):
    """
    Apply the current plan to create a new data version.

    Modes:
    - "all": Apply all steps at once (default)
    - "one_by_one": Each step creates its own node
    - "steps": Apply specific step numbers only

    Returns a task ID for tracking progress.
    """
    session_id_str = str(session_id)

    try:
        SessionService.get_session(session_id_str)
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
):
    """
    Clear all steps from the current plan.

    Use this to start over with a fresh plan.
    """
    session_id_str = str(session_id)

    try:
        SessionService.get_session(session_id_str)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id_str}")

    plan = PlanService.clear_plan(session_id_str)
    return SessionPlanResponse.from_plan(plan)
