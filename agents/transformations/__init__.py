# =============================================================================
# agents/transformations/__init__.py - Transformation Functions Package
# =============================================================================
# This package contains all transformation functions used by the Engineer agent.
#
# Each transformation is registered with the registry and can be looked up
# by TransformationType enum value.
#
# Usage:
#   from agents.transformations import get_transformer, execute_transform
#
#   transformer = get_transformer(TransformationType.DROP_ROWS)
#   result_df, code = transformer(df, plan)
# =============================================================================

from agents.transformations.registry import (
    REGISTRY,
    get_transformer,
    register,
    list_transformations,
)
from agents.transformations.utils import (
    build_condition_mask,
    conditions_to_code,
)

# Import all transformation modules to register them
from agents.transformations import cleaning
from agents.transformations import restructuring
from agents.transformations import column_math
from agents.transformations import filtering
from agents.transformations import aggregation
from agents.transformations import string_ops
from agents.transformations import date_ops
from agents.transformations import validation
from agents.transformations import advanced_ops

__all__ = [
    "REGISTRY",
    "get_transformer",
    "register",
    "list_transformations",
    "build_condition_mask",
    "conditions_to_code",
]
