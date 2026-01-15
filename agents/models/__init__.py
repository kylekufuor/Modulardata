# =============================================================================
# agents/models/ - Agent Communication Schemas
# =============================================================================
# This package contains Pydantic models that define the contracts between agents:
# - technical_plan.py: TechnicalPlan schema (Strategist -> Engineer contract)
#
# These models ensure type-safe communication between agents and provide
# clear documentation of what each agent expects and produces.
# =============================================================================

from agents.models.technical_plan import (
    TransformationType,
    ColumnTarget,
    FilterCondition,
    TechnicalPlan,
)

__all__ = [
    "TransformationType",
    "ColumnTarget",
    "FilterCondition",
    "TechnicalPlan",
]
