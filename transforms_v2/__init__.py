# =============================================================================
# transforms_v2 - Deterministic Transformation Library
# =============================================================================
# A closed, typed, testable transformation library for ModularData.
#
# Key principles:
# - Every primitive is deterministic (same input = same output)
# - Every primitive is typed (parameters, input schema, output schema)
# - Every primitive has test prompts (for Strategist training)
# - No dynamic code generation
#
# Architecture:
#   User Intent → Planner → Plan (DAG of primitives) → Engine → Result
#
# Usage:
#   from transforms_v2 import Engine, get_primitive
#
#   engine = Engine()
#   plan = [
#       {"op": "remove_duplicates", "params": {"subset": ["email"]}},
#       {"op": "change_text_casing", "params": {"column": "name", "case": "title"}}
#   ]
#   result = engine.execute(df, plan)
# =============================================================================

from transforms_v2.registry import (
    register_primitive,
    get_primitive,
    list_primitives,
    get_primitive_info,
    get_all_test_prompts,
    export_primitives_documentation,
    PRIMITIVE_REGISTRY,
)
from transforms_v2.engine import Engine, ExecutionResult
from transforms_v2.types import (
    Primitive,
    PrimitiveResult,
    Condition,
    ValidationResult,
)

# Import primitives to register them
# This must come after registry imports
from transforms_v2 import primitives  # noqa: F401, E402

__version__ = "2.0.0"

__all__ = [
    # Registry
    "register_primitive",
    "get_primitive",
    "list_primitives",
    "get_primitive_info",
    "get_all_test_prompts",
    "export_primitives_documentation",
    "PRIMITIVE_REGISTRY",
    # Engine
    "Engine",
    "ExecutionResult",
    # Types
    "Primitive",
    "PrimitiveResult",
    "Condition",
    "ValidationResult",
]
