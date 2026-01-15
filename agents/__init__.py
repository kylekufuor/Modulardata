# =============================================================================
# agents/ - AI Agent Definitions
# =============================================================================
# This package contains the 3-Agent pipeline for AI data transformation:
# - strategist.py: Agent A - Interprets user intent, creates technical plan
# - engineer.py: Agent B - Generates Pandas code from the plan (Milestone 4)
# - tester.py: Agent C - Executes and validates code (Milestone 4)
# - crew.py: Orchestrates the agents in sequence (Milestone 4)
#
# The agents work together: Strategist -> Engineer -> Tester -> Execute
#
# Models:
# - models/technical_plan.py: TechnicalPlan schema (Strategist -> Engineer)
#
# Prompts:
# - prompts/strategist_system.py: System prompt for Agent A
# =============================================================================

from agents.strategist import StrategistAgent, StrategyError
from agents.models.technical_plan import (
    TechnicalPlan,
    TransformationType,
    ColumnTarget,
    FilterCondition,
    FilterOperator,
)
from agents.chat_handler import (
    handle_chat_request,
    preview_transformation,
    chat,
)

__all__ = [
    # Agent
    "StrategistAgent",
    "StrategyError",
    # Models
    "TechnicalPlan",
    "TransformationType",
    "ColumnTarget",
    "FilterCondition",
    "FilterOperator",
    # Chat Handler
    "handle_chat_request",
    "preview_transformation",
    "chat",
]
