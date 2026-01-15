# =============================================================================
# core/services/plan_service.py - Session Plan Business Logic
# =============================================================================
# Handles Plan Mode operations: creating, updating, and applying plans.
# =============================================================================

import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from lib.supabase_client import SupabaseClient
from core.models.plan import (
    PlanStatus,
    SessionPlan,
    TransformationStep,
)
from app.exceptions import SessionNotFoundError

logger = logging.getLogger(__name__)


class PlanNotFoundError(Exception):
    """Raised when a plan is not found."""
    pass


class PlanService:
    """
    Service for session plan operations.

    Handles creating, updating, and applying transformation plans.
    """

    @staticmethod
    def get_or_create_plan(session_id: str | UUID) -> SessionPlan:
        """
        Get the active plan for a session, or create one if none exists.

        Args:
            session_id: Session UUID

        Returns:
            SessionPlan object

        Raises:
            SessionNotFoundError: If session doesn't exist
        """
        session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id
        client = SupabaseClient.get_client()

        # Verify session exists
        session = SupabaseClient.fetch_session(session_id_str)
        if not session:
            raise SessionNotFoundError(session_id_str)

        # Try to find existing active plan
        try:
            response = (
                client.table("session_plans")
                .select("*")
                .eq("session_id", session_id_str)
                .eq("status", PlanStatus.PLANNING.value)
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            )

            if response.data:
                return PlanService._dict_to_plan(response.data[0])

        except Exception as e:
            logger.warning(f"Error fetching plan: {e}")

        # Create new plan
        return PlanService.create_plan(session_id_str)

    @staticmethod
    def create_plan(session_id: str | UUID, suggest_apply_at: int = 3) -> SessionPlan:
        """
        Create a new plan for a session.

        Args:
            session_id: Session UUID
            suggest_apply_at: Number of steps before suggesting apply

        Returns:
            New SessionPlan object
        """
        session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id
        client = SupabaseClient.get_client()

        data = {
            "session_id": session_id_str,
            "steps": [],
            "status": PlanStatus.PLANNING.value,
            "suggest_apply_at": suggest_apply_at,
        }

        try:
            response = (
                client.table("session_plans")
                .insert(data)
                .execute()
            )

            if response.data:
                plan = PlanService._dict_to_plan(response.data[0])
                logger.info(f"Created plan {plan.id} for session {session_id_str}")
                return plan

            raise Exception("Insert returned no data")

        except Exception as e:
            logger.error(f"Failed to create plan: {e}")
            raise

    @staticmethod
    def get_plan(plan_id: str | UUID) -> SessionPlan:
        """
        Get a plan by ID.

        Args:
            plan_id: Plan UUID

        Returns:
            SessionPlan object

        Raises:
            PlanNotFoundError: If plan doesn't exist
        """
        plan_id_str = str(plan_id) if isinstance(plan_id, UUID) else plan_id
        client = SupabaseClient.get_client()

        try:
            response = (
                client.table("session_plans")
                .select("*")
                .eq("id", plan_id_str)
                .single()
                .execute()
            )

            if response.data:
                return PlanService._dict_to_plan(response.data)

        except Exception as e:
            logger.warning(f"Plan not found: {plan_id_str}")

        raise PlanNotFoundError(f"Plan not found: {plan_id_str}")

    @staticmethod
    def add_step(
        session_id: str | UUID,
        transformation_type: str,
        explanation: str,
        target_columns: list[str] | None = None,
        parameters: dict[str, Any] | None = None,
        estimated_rows_affected: int | None = None,
        code_preview: str | None = None,
    ) -> SessionPlan:
        """
        Add a transformation step to the active plan.

        Args:
            session_id: Session UUID
            transformation_type: Type of transformation
            explanation: Human-readable explanation
            target_columns: Affected columns
            parameters: Transformation parameters
            estimated_rows_affected: Estimated row impact
            code_preview: Preview of pandas code

        Returns:
            Updated SessionPlan
        """
        # Get or create active plan
        plan = PlanService.get_or_create_plan(session_id)

        # Create new step
        step = TransformationStep(
            step_number=len(plan.steps) + 1,
            transformation_type=transformation_type,
            target_columns=target_columns or [],
            parameters=parameters or {},
            explanation=explanation,
            estimated_rows_affected=estimated_rows_affected,
            code_preview=code_preview,
        )

        # Add to plan
        plan.steps.append(step)

        # Update in database
        return PlanService._update_plan(plan)

    @staticmethod
    def clear_plan(session_id: str | UUID) -> SessionPlan:
        """
        Clear all steps from the active plan.

        Args:
            session_id: Session UUID

        Returns:
            Updated SessionPlan with empty steps
        """
        plan = PlanService.get_or_create_plan(session_id)
        plan.steps = []
        return PlanService._update_plan(plan)

    @staticmethod
    def cancel_plan(session_id: str | UUID) -> SessionPlan:
        """
        Cancel the active plan.

        Args:
            session_id: Session UUID

        Returns:
            Cancelled SessionPlan
        """
        plan = PlanService.get_or_create_plan(session_id)
        plan.status = PlanStatus.CANCELLED
        return PlanService._update_plan(plan)

    @staticmethod
    def mark_applied(
        plan_id: str | UUID,
        result_node_id: str,
    ) -> SessionPlan:
        """
        Mark a plan as applied and record the result node.

        Args:
            plan_id: Plan UUID
            result_node_id: ID of the node created

        Returns:
            Updated SessionPlan
        """
        plan = PlanService.get_plan(plan_id)
        plan.status = PlanStatus.APPLIED
        plan.result_node_id = result_node_id
        return PlanService._update_plan(plan)

    @staticmethod
    def _update_plan(plan: SessionPlan) -> SessionPlan:
        """Update a plan in the database."""
        if not plan.id:
            raise ValueError("Plan has no ID")

        client = SupabaseClient.get_client()

        # Convert steps to JSON-serializable format
        steps_json = [
            {
                "step_number": s.step_number,
                "transformation_type": s.transformation_type,
                "target_columns": s.target_columns,
                "parameters": s.parameters,
                "explanation": s.explanation,
                "estimated_rows_affected": s.estimated_rows_affected,
                "code_preview": s.code_preview,
                "created_at": s.created_at.isoformat() if s.created_at else None,
            }
            for s in plan.steps
        ]

        data = {
            "steps": steps_json,
            "status": plan.status.value if isinstance(plan.status, PlanStatus) else plan.status,
            "result_node_id": plan.result_node_id,
        }

        try:
            response = (
                client.table("session_plans")
                .update(data)
                .eq("id", plan.id)
                .execute()
            )

            if response.data:
                return PlanService._dict_to_plan(response.data[0])

            raise Exception("Update returned no data")

        except Exception as e:
            logger.error(f"Failed to update plan: {e}")
            raise

    @staticmethod
    def _dict_to_plan(data: dict[str, Any]) -> SessionPlan:
        """Convert database dict to SessionPlan object."""
        # Parse steps from JSON
        steps = []
        for s in data.get("steps", []):
            step = TransformationStep(
                step_number=s.get("step_number", 1),
                transformation_type=s.get("transformation_type", "unknown"),
                target_columns=s.get("target_columns", []),
                parameters=s.get("parameters", {}),
                explanation=s.get("explanation", ""),
                estimated_rows_affected=s.get("estimated_rows_affected"),
                code_preview=s.get("code_preview"),
            )
            steps.append(step)

        return SessionPlan(
            id=data.get("id"),
            session_id=data.get("session_id"),
            steps=steps,
            status=PlanStatus(data.get("status", "planning")),
            suggest_apply_at=data.get("suggest_apply_at", 3),
            result_node_id=data.get("result_node_id"),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.utcnow(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else datetime.utcnow(),
        )
