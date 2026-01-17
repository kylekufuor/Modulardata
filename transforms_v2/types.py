# =============================================================================
# transforms_v2/types.py - Core Types and Schemas
# =============================================================================
# Defines all types used throughout the transformation library.
# These ensure type safety and deterministic behavior.
# =============================================================================

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Literal

import pandas as pd


# =============================================================================
# Enums
# =============================================================================

class CaseType(str, Enum):
    """Text casing options."""
    LOWER = "lower"
    UPPER = "upper"
    TITLE = "title"
    SENTENCE = "sentence"


class FillMethod(str, Enum):
    """Methods for filling null values."""
    VALUE = "value"
    MEAN = "mean"
    MEDIAN = "median"
    MODE = "mode"
    FORWARD = "forward"
    BACKWARD = "backward"


class FilterOperator(str, Enum):
    """Operators for row filtering."""
    EQ = "eq"           # Equal
    NE = "ne"           # Not equal
    GT = "gt"           # Greater than
    LT = "lt"           # Less than
    GTE = "gte"         # Greater than or equal
    LTE = "lte"         # Less than or equal
    ISNULL = "isnull"   # Is null
    NOTNULL = "notnull" # Is not null
    CONTAINS = "contains"
    STARTSWITH = "startswith"
    ENDSWITH = "endswith"
    REGEX = "regex"
    IN = "in"
    NOT_IN = "not_in"


class JoinType(str, Enum):
    """Join types for combining tables."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"


# =============================================================================
# Condition (for filtering)
# =============================================================================

@dataclass
class Condition:
    """
    A single filter condition.

    Examples:
        Condition(column="age", operator="gt", value=18)
        Condition(column="email", operator="isnull")
        Condition(column="status", operator="in", value=["active", "pending"])
    """
    column: str
    operator: FilterOperator | str
    value: Any = None
    case_sensitive: bool = False

    def __post_init__(self):
        if isinstance(self.operator, str):
            self.operator = FilterOperator(self.operator)

    def to_dict(self) -> dict:
        return {
            "column": self.column,
            "operator": self.operator.value if isinstance(self.operator, FilterOperator) else self.operator,
            "value": self.value,
            "case_sensitive": self.case_sensitive,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Condition":
        return cls(
            column=d["column"],
            operator=d["operator"],
            value=d.get("value"),
            case_sensitive=d.get("case_sensitive", False),
        )


# =============================================================================
# Primitive Result
# =============================================================================

@dataclass
class PrimitiveResult:
    """
    Result of executing a single primitive.

    Contains the transformed DataFrame and metadata about the operation.
    """
    success: bool
    df: pd.DataFrame | None = None
    rows_before: int = 0
    rows_after: int = 0
    cols_before: int = 0
    cols_after: int = 0
    error: str | None = None
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def rows_changed(self) -> int:
        return self.rows_after - self.rows_before

    @property
    def cols_changed(self) -> int:
        return self.cols_after - self.cols_before


# =============================================================================
# Validation Result
# =============================================================================

@dataclass
class ValidationResult:
    """Result of running a validation assertion."""
    passed: bool
    assertion_type: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


# =============================================================================
# Test Prompt (for Strategist training)
# =============================================================================

@dataclass
class TestPrompt:
    """
    A test prompt for training the Strategist to recognize this primitive.

    Each primitive should have 3+ test prompts covering different phrasings.
    """
    prompt: str
    expected_params: dict[str, Any]
    description: str = ""


# =============================================================================
# Parameter Definition
# =============================================================================

@dataclass
class ParamDef:
    """
    Definition of a primitive parameter.

    Used for validation and documentation.
    """
    name: str
    type: type | str
    required: bool = True
    default: Any = None
    description: str = ""
    choices: list[Any] | None = None  # For enum-like params

    def validate(self, value: Any) -> tuple[bool, str]:
        """Validate a value against this parameter definition."""
        if value is None:
            if self.required:
                return False, f"Parameter '{self.name}' is required"
            return True, ""

        # Type checking (simplified)
        if self.choices and value not in self.choices:
            return False, f"Parameter '{self.name}' must be one of {self.choices}"

        return True, ""


# =============================================================================
# Primitive Base Class
# =============================================================================

@dataclass
class PrimitiveInfo:
    """
    Metadata about a primitive.

    This is registered with the primitive for documentation and validation.
    """
    name: str
    category: str
    description: str
    params: list[ParamDef]
    test_prompts: list[TestPrompt]

    # Schema information
    input_columns: list[str] | None = None  # None = any columns
    output_columns_added: list[str] | None = None
    output_columns_removed: list[str] | None = None
    may_change_row_count: bool = False
    may_change_col_count: bool = False


class Primitive(ABC):
    """
    Abstract base class for all primitives.

    Every primitive must implement:
    - execute(df, params) -> PrimitiveResult
    - info() -> PrimitiveInfo (class method)

    Example:
        class RemoveDuplicates(Primitive):
            @classmethod
            def info(cls) -> PrimitiveInfo:
                return PrimitiveInfo(
                    name="remove_duplicates",
                    category="rows",
                    description="Remove duplicate rows",
                    params=[...],
                    test_prompts=[...]
                )

            def execute(self, df: pd.DataFrame, params: dict) -> PrimitiveResult:
                # Implementation
                ...
    """

    @classmethod
    @abstractmethod
    def info(cls) -> PrimitiveInfo:
        """Return metadata about this primitive."""
        pass

    @abstractmethod
    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        """
        Execute this primitive on a DataFrame.

        Args:
            df: Input DataFrame (will not be modified)
            params: Parameters for this primitive

        Returns:
            PrimitiveResult with success/failure and transformed DataFrame
        """
        pass

    def validate_params(self, params: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate parameters against the primitive's param definitions."""
        errors = []
        for param_def in self.info().params:
            value = params.get(param_def.name, param_def.default)
            valid, error = param_def.validate(value)
            if not valid:
                errors.append(error)
        return len(errors) == 0, errors
