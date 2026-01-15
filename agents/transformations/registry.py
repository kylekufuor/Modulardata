# =============================================================================
# agents/transformations/registry.py - Transformation Registry
# =============================================================================
# Maps TransformationType enum values to handler functions.
#
# Each transformation function takes (df, plan) and returns (df, code).
# Functions are registered using the @register decorator.
#
# Example:
#   @register(TransformationType.DROP_ROWS)
#   def drop_rows(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
#       mask = build_condition_mask(df, plan.conditions)
#       result = df[~mask].copy()
#       code = "df = df[~condition_mask]"
#       return result, code
# =============================================================================

from typing import Callable, Any
import pandas as pd

from agents.models.technical_plan import TechnicalPlan, TransformationType


# Type alias for transformation functions
# Takes (DataFrame, TechnicalPlan) and returns (DataFrame, code_string)
TransformFunc = Callable[[pd.DataFrame, TechnicalPlan], tuple[pd.DataFrame, str]]

# Global registry mapping TransformationType -> handler function
REGISTRY: dict[TransformationType, TransformFunc] = {}


def register(trans_type: TransformationType):
    """
    Decorator to register a transformation function.

    Usage:
        @register(TransformationType.DROP_ROWS)
        def drop_rows(df, plan):
            ...
            return result, code
    """
    def decorator(func: TransformFunc) -> TransformFunc:
        # Handle both enum and string values
        key = trans_type
        if hasattr(trans_type, 'value'):
            # Also register by string value for flexibility
            REGISTRY[trans_type.value] = func
        REGISTRY[key] = func
        return func
    return decorator


def get_transformer(trans_type: TransformationType | str) -> TransformFunc | None:
    """
    Get the transformation function for a type.

    Args:
        trans_type: TransformationType enum or string value

    Returns:
        The registered transformation function, or None if not found
    """
    # Try direct lookup first
    if trans_type in REGISTRY:
        return REGISTRY[trans_type]

    # Try string value if enum
    if hasattr(trans_type, 'value'):
        return REGISTRY.get(trans_type.value)

    return None


def list_transformations() -> list[str]:
    """List all registered transformation types."""
    return [
        k.value if hasattr(k, 'value') else k
        for k in REGISTRY.keys()
        if not isinstance(k, str)  # Avoid duplicates from string keys
    ]
