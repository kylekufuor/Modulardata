# =============================================================================
# agents/quality_checks/__init__.py - Quality Checks Package
# =============================================================================
# This package contains quality validation checks for data transformations.
#
# Usage:
#   from agents.quality_checks import get_checks_for_type, run_checks
#
#   checks = get_checks_for_type("drop_rows")
#   for name, check_func in checks:
#       issues = check_func(before_df, after_df, plan)
# =============================================================================

from agents.quality_checks.registry import (
    register_check,
    get_check,
    get_checks_for_type,
    get_all_checks,
    list_checks,
    CHECK_REGISTRY,
    UNIVERSAL_CHECKS,
)

# Import all check modules to trigger registration
from agents.quality_checks import schema
from agents.quality_checks import rows
from agents.quality_checks import nulls
from agents.quality_checks import duplicates
from agents.quality_checks import values


__all__ = [
    # Registry functions
    "register_check",
    "get_check",
    "get_checks_for_type",
    "get_all_checks",
    "list_checks",
    "CHECK_REGISTRY",
    "UNIVERSAL_CHECKS",
]
