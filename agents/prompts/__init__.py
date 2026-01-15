# =============================================================================
# agents/prompts/ - System Prompts for AI Agents
# =============================================================================
# This package contains system prompts for each agent:
# - strategist_system.py: Context Strategist (Agent A) prompt
#
# Prompts follow Anthropic's "Right Altitude" principle:
# - Specific enough to guide behavior effectively
# - Flexible enough to provide strong heuristics
#
# Organized with XML tags for clear structure.
# =============================================================================

from agents.prompts.strategist_system import (
    STRATEGIST_SYSTEM_PROMPT,
    build_strategist_prompt,
)

__all__ = [
    "STRATEGIST_SYSTEM_PROMPT",
    "build_strategist_prompt",
]
