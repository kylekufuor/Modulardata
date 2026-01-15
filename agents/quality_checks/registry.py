# =============================================================================
# agents/quality_checks/registry.py - Quality Check Registry
# =============================================================================
# Registry pattern for quality checks, similar to transformation registry.
#
# Usage:
#   from agents.quality_checks.registry import register_check, get_checks_for_type
#
#   @register_check("row_count", applies_to=["drop_rows", "filter_rows"])
#   def check_row_count(before_df, after_df, plan):
#       # Return list of QualityIssue
#       return issues
#
#   # Get checks for a transformation type
#   checks = get_checks_for_type("drop_rows")
# =============================================================================

from __future__ import annotations

from typing import Callable, Any
import pandas as pd

from agents.models.technical_plan import TechnicalPlan, TransformationType
from agents.models.test_result import QualityIssue

# Type alias for check functions
# Takes: before_df, after_df, plan
# Returns: list of QualityIssue
CheckFunc = Callable[[pd.DataFrame, pd.DataFrame, TechnicalPlan], list[QualityIssue]]

# Registry: check_name -> (func, applies_to_types)
CHECK_REGISTRY: dict[str, tuple[CheckFunc, list[str] | None]] = {}

# Universal checks run on every transformation
UNIVERSAL_CHECKS: list[str] = []


def register_check(
    name: str,
    applies_to: list[str] | None = None,
    universal: bool = False
):
    """
    Decorator to register a quality check function.

    Args:
        name: Unique name for this check
        applies_to: List of transformation types this check applies to.
                   If None, only runs when explicitly requested.
        universal: If True, runs on ALL transformation types.

    Example:
        @register_check("row_count", applies_to=["drop_rows", "filter_rows"])
        def check_row_count(before_df, after_df, plan):
            issues = []
            # Check logic
            return issues

        @register_check("schema_valid", universal=True)
        def check_schema(before_df, after_df, plan):
            # Runs on every transformation
            return []
    """
    def decorator(func: CheckFunc) -> CheckFunc:
        CHECK_REGISTRY[name] = (func, applies_to)
        if universal:
            UNIVERSAL_CHECKS.append(name)
        return func
    return decorator


def get_check(name: str) -> CheckFunc | None:
    """Get a specific check function by name."""
    entry = CHECK_REGISTRY.get(name)
    return entry[0] if entry else None


def get_checks_for_type(transformation_type: str | TransformationType) -> list[tuple[str, CheckFunc]]:
    """
    Get all checks that should run for a given transformation type.

    Returns list of (check_name, check_func) tuples.
    """
    if isinstance(transformation_type, TransformationType):
        trans_type = transformation_type.value
    else:
        trans_type = transformation_type

    checks = []

    for name, (func, applies_to) in CHECK_REGISTRY.items():
        # Universal checks always apply
        if name in UNIVERSAL_CHECKS:
            checks.append((name, func))
        # Type-specific checks
        elif applies_to and trans_type in applies_to:
            checks.append((name, func))

    return checks


def get_all_checks() -> list[tuple[str, CheckFunc]]:
    """Get all registered checks."""
    return [(name, entry[0]) for name, entry in CHECK_REGISTRY.items()]


def list_checks() -> dict[str, list[str] | None]:
    """List all registered checks and what types they apply to."""
    return {
        name: applies_to
        for name, (_, applies_to) in CHECK_REGISTRY.items()
    }
