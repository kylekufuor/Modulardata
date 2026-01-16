# =============================================================================
# core/models/plan.py - Session Plan Schemas
# =============================================================================
# These models support Plan Mode - where transformations accumulate before
# being applied as a batch to create a new node.
#
# Flow:
# 1. User chats -> Strategist adds transformation to plan
# 2. Plan accumulates (default: suggest apply at 3 transformations)
# 3. User says "apply" -> All transformations execute -> New node created
#
# Benefits:
# - Users can review before committing
# - Natural "undo checkpoints" at each batch
# - Flexible: apply all, one-by-one, or custom selection
# =============================================================================

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class PlanStatus(str, Enum):
    """
    Status of a session plan.

    - planning: Actively accumulating transformations
    - ready: User indicated ready to apply
    - applied: All transformations executed, node created
    - cancelled: User discarded the plan
    """
    PLANNING = "planning"
    READY = "ready"
    APPLIED = "applied"
    CANCELLED = "cancelled"


class TransformationStep(BaseModel):
    """
    Single transformation step in a plan.

    This is what gets accumulated as the user chats.
    Each step represents one transformation that will be applied.
    """

    step_number: int = Field(
        ...,
        ge=1,
        description="Position in the plan (1-indexed)"
    )

    transformation_type: str = Field(
        ...,
        description="Type of transformation (e.g., 'drop_rows', 'trim_whitespace')"
    )

    target_columns: list[str] = Field(
        default_factory=list,
        description="Columns this transformation affects"
    )

    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Transformation-specific parameters"
    )

    explanation: str = Field(
        ...,
        description="Human-readable description of what this step does"
    )

    estimated_rows_affected: int | None = Field(
        default=None,
        ge=0,
        description="Estimated number of rows that will be affected"
    )

    code_preview: str | None = Field(
        default=None,
        description="Preview of the pandas code that will be executed"
    )

    # Tracking
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When this step was added to the plan"
    )


class SessionPlan(BaseModel):
    """
    Accumulated transformation plan for a session.

    In Plan Mode, transformations don't execute immediately.
    Instead, they accumulate here until the user applies them.
    """

    # Identity
    id: str | None = Field(
        default=None,
        description="Plan UUID (set by database)"
    )

    session_id: str = Field(
        ...,
        description="Session this plan belongs to"
    )

    # Transformation steps
    steps: list[TransformationStep] = Field(
        default_factory=list,
        description="Accumulated transformation steps"
    )

    # Status
    status: PlanStatus = Field(
        default=PlanStatus.PLANNING,
        description="Current status of the plan"
    )

    # Configuration
    suggest_apply_at: int = Field(
        default=3,
        ge=1,
        description="Suggest applying after this many steps (default: 3)"
    )

    # Result tracking (after apply)
    result_node_id: str | None = Field(
        default=None,
        description="Node ID created when plan was applied"
    )

    # Timestamps
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When the plan was created"
    )

    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When the plan was last modified"
    )

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def should_suggest_apply(self) -> bool:
        """Check if we've hit the suggestion threshold."""
        return len(self.steps) >= self.suggest_apply_at

    def add_step(
        self,
        transformation_type: str,
        explanation: str,
        target_columns: list[str] | None = None,
        parameters: dict[str, Any] | None = None,
        estimated_rows_affected: int | None = None,
        code_preview: str | None = None,
    ) -> TransformationStep:
        """Add a new transformation step to the plan."""
        step = TransformationStep(
            step_number=len(self.steps) + 1,
            transformation_type=transformation_type,
            target_columns=target_columns or [],
            parameters=parameters or {},
            explanation=explanation,
            estimated_rows_affected=estimated_rows_affected,
            code_preview=code_preview,
        )
        self.steps.append(step)
        self.updated_at = datetime.utcnow()
        return step

    def clear_steps(self) -> None:
        """Clear all steps from the plan."""
        self.steps = []
        self.updated_at = datetime.utcnow()

    def get_step(self, step_number: int) -> TransformationStep | None:
        """Get a specific step by number."""
        for step in self.steps:
            if step.step_number == step_number:
                return step
        return None

    def to_summary(self) -> str:
        """Generate a human-readable summary of the plan."""
        if not self.steps:
            return "No transformations planned yet."

        lines = [f"Plan with {len(self.steps)} transformation(s):"]
        for step in self.steps:
            affected = f" (~{step.estimated_rows_affected} rows)" if step.estimated_rows_affected else ""
            lines.append(f"  {step.step_number}. {step.explanation}{affected}")

        if self.should_suggest_apply():
            lines.append("")
            lines.append(f"💡 You have {len(self.steps)} transformations. Ready to apply?")

        return "\n".join(lines)


# =============================================================================
# API Request/Response Models
# =============================================================================

class SessionPlanResponse(BaseModel):
    """Response when a session plan is updated or retrieved."""

    session_id: str
    plan_id: str | None
    status: PlanStatus
    steps: list[TransformationStep]
    step_count: int
    should_suggest_apply: bool
    suggestion_message: str | None = None

    @classmethod
    def from_plan(cls, plan: SessionPlan) -> "SessionPlanResponse":
        """Create response from a SessionPlan."""
        suggestion = None
        if plan.should_suggest_apply():
            suggestion = f"You have {len(plan.steps)} transformations planned. Ready to apply them?"

        return cls(
            session_id=plan.session_id,
            plan_id=plan.id,
            status=plan.status,
            steps=plan.steps,
            step_count=len(plan.steps),
            should_suggest_apply=plan.should_suggest_apply(),
            suggestion_message=suggestion,
        )


class ApplyPlanRequest(BaseModel):
    """Request to apply a plan."""

    mode: str = Field(
        default="all",
        description="How to apply: 'all', 'one_by_one', or 'steps' with step_numbers"
    )

    step_numbers: list[int] | None = Field(
        default=None,
        description="Specific steps to apply (only if mode='steps')"
    )

    confirmed: bool = Field(
        default=False,
        description="Set to true to confirm risky operations"
    )


class RiskPreview(BaseModel):
    """Preview of what a risky transformation will change."""

    rows_before: int
    cols_before: int
    rows_after: int | None = None
    rows_removed: int | None = None
    removal_percent: float | None = None
    columns_removed: list[str] | None = None
    sample_removed: list[dict] | None = None


class ApplyPlanResponse(BaseModel):
    """Response after applying a plan."""

    success: bool
    node_id: str | None = None
    transformations_applied: int = 0
    rows_before: int | None = None
    rows_after: int | None = None
    message: str = ""
    error: str | None = None

    # Risk assessment fields
    requires_confirmation: bool = False
    is_risky: bool = False
    risk_level: str | None = None  # "none", "moderate", "high"
    risk_reasons: list[str] | None = None
    risk_preview: RiskPreview | None = None
    confirmation_message: str | None = None
