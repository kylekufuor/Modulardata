# =============================================================================
# agents/models/technical_plan.py - Technical Plan Schema
# =============================================================================
# This module defines the TechnicalPlan schema - the contract between the
# Strategist agent (Agent A) and the Engineer agent (Agent B).
#
# The Strategist transforms vague user requests into structured TechnicalPlans.
# The Engineer uses these plans to generate Python/pandas code.
#
# Design follows Anthropic's "Poka-yoke" principle:
# - Clear, unambiguous parameter names
# - Enums for all categorical values
# - Required fields have no defaults
# - Optional fields have sensible defaults
#
# Example flow:
#   User: "remove rows where email is blank"
#   Strategist outputs:
#   {
#       "transformation_type": "drop_rows",
#       "target_columns": [{"column_name": "email"}],
#       "conditions": [{"column": "email", "operator": "isnull"}],
#       "explanation": "Remove all rows where the email column is null"
#   }
# =============================================================================

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


# =============================================================================
# Enums
# =============================================================================

class TransformationType(str, Enum):
    """
    Types of transformations the Engineer agent can execute.

    Organized by category:
    - Row operations: Affect which rows are in the dataset
    - Column operations: Affect which/how columns exist
    - Value transformations: Change cell values
    - Type operations: Change data types
    - Special: Undo, custom code
    """

    # -------------------------------------------------------------------------
    # Row Operations
    # -------------------------------------------------------------------------
    DROP_ROWS = "drop_rows"
    """Remove rows matching a condition."""

    FILTER_ROWS = "filter_rows"
    """Keep only rows matching a condition (inverse of drop_rows)."""

    DEDUPLICATE = "deduplicate"
    """Remove duplicate rows, keeping first/last occurrence."""

    SORT_ROWS = "sort_rows"
    """Sort rows by one or more columns."""

    # -------------------------------------------------------------------------
    # Column Operations
    # -------------------------------------------------------------------------
    DROP_COLUMNS = "drop_columns"
    """Remove one or more columns from the dataset."""

    RENAME_COLUMN = "rename_column"
    """Rename a column to a new name."""

    REORDER_COLUMNS = "reorder_columns"
    """Change the order of columns."""

    ADD_COLUMN = "add_column"
    """Add a new calculated column."""

    # -------------------------------------------------------------------------
    # Value Transformations
    # -------------------------------------------------------------------------
    FILL_NULLS = "fill_nulls"
    """Fill missing/null values with a specified value or method."""

    REPLACE_VALUES = "replace_values"
    """Find and replace specific values."""

    STANDARDIZE = "standardize"
    """Standardize format (trim whitespace, change case, etc.)."""

    TRIM_WHITESPACE = "trim_whitespace"
    """Remove leading/trailing whitespace from string columns."""

    CHANGE_CASE = "change_case"
    """Convert text to upper, lower, or title case."""

    SANITIZE_HEADERS = "sanitize_headers"
    """Convert column names to snake_case or camelCase, remove special characters."""

    # -------------------------------------------------------------------------
    # Type Operations
    # -------------------------------------------------------------------------
    CONVERT_TYPE = "convert_type"
    """Convert column to a different data type."""

    PARSE_DATE = "parse_date"
    """Parse string column as datetime."""

    FORMAT_DATE = "format_date"
    """Reformat datetime column to a different string format."""

    # -------------------------------------------------------------------------
    # Numeric Operations
    # -------------------------------------------------------------------------
    ROUND_NUMBERS = "round_numbers"
    """Round numeric values to specified precision."""

    HANDLE_OUTLIERS = "handle_outliers"
    """Identify and handle outlier values (cap, remove, flag)."""

    NORMALIZE = "normalize"
    """Normalize/scale numerical data (min-max or z-score)."""

    # -------------------------------------------------------------------------
    # Text Operations
    # -------------------------------------------------------------------------
    EXTRACT_PATTERN = "extract_pattern"
    """Extract text matching a regex pattern."""

    SPLIT_COLUMN = "split_column"
    """Split a column into multiple columns."""

    MERGE_COLUMNS = "merge_columns"
    """Combine multiple columns into one."""

    # -------------------------------------------------------------------------
    # Data Restructuring
    # -------------------------------------------------------------------------
    PIVOT = "pivot"
    """Transform rows into columns (long to wide format)."""

    MELT = "melt"
    """Transform columns into rows (wide to long format)."""

    TRANSPOSE = "transpose"
    """Flip the entire table's axes."""

    # -------------------------------------------------------------------------
    # Filtering & Selection
    # -------------------------------------------------------------------------
    SELECT_COLUMNS = "select_columns"
    """Keep only specific columns and discard the rest."""

    SLICE_ROWS = "slice_rows"
    """Get the first N or last N rows."""

    SAMPLE_ROWS = "sample_rows"
    """Get a random sample of rows."""

    # -------------------------------------------------------------------------
    # Aggregation & Enrichment
    # -------------------------------------------------------------------------
    GROUP_BY = "group_by"
    """Aggregate data (Sum, Mean, Count, Min, Max) based on a category."""

    CUMULATIVE = "cumulative"
    """Calculate a running total or cumulative operation for a column."""

    JOIN = "join"
    """Merge data from another source based on a common key (VLOOKUP)."""

    RANK = "rank"
    """Assign ranks to values in a column."""

    # -------------------------------------------------------------------------
    # Special Operations
    # -------------------------------------------------------------------------
    UNDO = "undo"
    """Rollback to a previous version (parent node)."""

    CUSTOM = "custom"
    """Custom pandas code (for operations not covered above)."""


class FilterOperator(str, Enum):
    """
    Operators for filter conditions.

    These map to pandas operations:
    - eq: df[col] == value
    - ne: df[col] != value
    - gt: df[col] > value
    - etc.
    """

    # Comparison operators
    EQ = "eq"           # Equal to
    NE = "ne"           # Not equal to
    GT = "gt"           # Greater than
    LT = "lt"           # Less than
    GTE = "gte"         # Greater than or equal
    LTE = "lte"         # Less than or equal

    # Null checks
    ISNULL = "isnull"   # Is null/NaN
    NOTNULL = "notnull" # Is not null/NaN

    # String operations
    CONTAINS = "contains"       # Contains substring
    STARTSWITH = "startswith"   # Starts with
    ENDSWITH = "endswith"       # Ends with
    REGEX = "regex"             # Matches regex pattern

    # Set operations
    IN = "in"           # Value is in list
    NOT_IN = "not_in"   # Value is not in list

    # Type checks
    IS_NUMERIC = "is_numeric"   # Can be converted to number
    IS_DATE = "is_date"         # Can be parsed as date


class CaseType(str, Enum):
    """Case conversion types for CHANGE_CASE operation."""
    UPPER = "upper"
    LOWER = "lower"
    TITLE = "title"
    SENTENCE = "sentence"


class FillMethod(str, Enum):
    """Methods for filling null values."""
    VALUE = "value"         # Fill with static value
    MEAN = "mean"           # Fill with column mean (numeric)
    MEDIAN = "median"       # Fill with column median (numeric)
    MODE = "mode"           # Fill with most common value
    FORWARD = "forward"     # Fill with previous value
    BACKWARD = "backward"   # Fill with next value
    INTERPOLATE = "interpolate"  # Linear interpolation


class OutlierMethod(str, Enum):
    """Methods for handling outliers."""
    CAP = "cap"             # Cap at threshold
    REMOVE = "remove"       # Remove outlier rows
    FLAG = "flag"           # Add a boolean flag column
    REPLACE_NULL = "replace_null"  # Replace with null


# =============================================================================
# Component Models
# =============================================================================

class ColumnTarget(BaseModel):
    """
    Specifies which column(s) to operate on.

    Most operations target a single column, but some (like merge)
    need multiple columns.

    Example:
        {"column_name": "email"}
        {"column_name": "first_name", "secondary_column": "last_name"}
    """

    column_name: str = Field(
        ...,
        min_length=1,
        description="Exact column name from the data profile"
    )

    secondary_column: str | None = Field(
        default=None,
        description="Secondary column for operations needing two columns (e.g., merge)"
    )


class FilterCondition(BaseModel):
    """
    Condition for filtering/dropping rows.

    Multiple conditions are combined with AND logic.
    For OR logic, use separate filter operations.

    Examples:
        # Null check
        {"column": "email", "operator": "isnull"}

        # Value comparison
        {"column": "age", "operator": "gt", "value": 18}

        # String contains
        {"column": "name", "operator": "contains", "value": "Smith"}

        # Value in list
        {"column": "status", "operator": "in", "value": ["active", "pending"]}
    """

    column: str = Field(
        ...,
        min_length=1,
        description="Column name to check"
    )

    operator: FilterOperator = Field(
        ...,
        description="Comparison operator"
    )

    value: Any = Field(
        default=None,
        description="Value to compare against (None for isnull/notnull)"
    )

    case_sensitive: bool = Field(
        default=False,
        description="Case-sensitive comparison for string operations"
    )


# =============================================================================
# Main Schema
# =============================================================================

class TechnicalPlan(BaseModel):
    """
    Structured output from the Strategist agent.

    This is the contract between Strategist (Agent A) and Engineer (Agent B).
    The Engineer uses this plan to generate Python/pandas code.

    Design principles:
    - Every plan has a transformation_type (required)
    - target_columns specifies which columns to affect
    - conditions specify row filtering criteria
    - parameters hold type-specific options
    - explanation provides human-readable description
    - confidence indicates certainty of interpretation
    - clarification_needed asks user for more info if uncertain

    Example - Drop rows with null email:
    {
        "transformation_type": "drop_rows",
        "target_columns": [{"column_name": "email"}],
        "conditions": [{"column": "email", "operator": "isnull"}],
        "parameters": {},
        "explanation": "Remove all rows where the email column is null",
        "confidence": 0.95
    }

    Example - Fill nulls with mean:
    {
        "transformation_type": "fill_nulls",
        "target_columns": [{"column_name": "price"}],
        "conditions": [],
        "parameters": {"method": "mean"},
        "explanation": "Fill missing price values with the column mean",
        "confidence": 0.90
    }

    Example - Undo:
    {
        "transformation_type": "undo",
        "target_columns": [],
        "conditions": [],
        "parameters": {},
        "explanation": "Rollback to the previous version",
        "confidence": 1.0,
        "rollback_to_node_id": "550e8400-..."
    }
    """

    # -------------------------------------------------------------------------
    # Core Fields
    # -------------------------------------------------------------------------

    transformation_type: TransformationType = Field(
        ...,
        description="The type of transformation to execute"
    )

    target_columns: list[ColumnTarget] = Field(
        default_factory=list,
        description="Columns affected by this transformation"
    )

    conditions: list[FilterCondition] = Field(
        default_factory=list,
        description="Conditions for row filtering (combined with AND)"
    )

    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Type-specific parameters (fill_value, case_type, etc.)"
    )

    # -------------------------------------------------------------------------
    # Human-Readable Fields
    # -------------------------------------------------------------------------

    explanation: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Human-readable explanation of the transformation"
    )

    # -------------------------------------------------------------------------
    # Confidence & Clarification
    # -------------------------------------------------------------------------

    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Strategist's confidence in this interpretation (0.0-1.0)"
    )

    clarification_needed: str | None = Field(
        default=None,
        description="Question to ask user if confidence < 0.7"
    )

    # -------------------------------------------------------------------------
    # Special Fields
    # -------------------------------------------------------------------------

    rollback_to_node_id: str | None = Field(
        default=None,
        description="Target node ID for UNDO operations"
    )

    # -------------------------------------------------------------------------
    # Validators
    # -------------------------------------------------------------------------

    @field_validator("confidence")
    @classmethod
    def validate_confidence(cls, v: float) -> float:
        """Ensure confidence is between 0 and 1."""
        return max(0.0, min(1.0, v))

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def needs_clarification(self) -> bool:
        """Check if user clarification is needed."""
        return self.confidence < 0.7 and self.clarification_needed is not None

    def is_undo(self) -> bool:
        """Check if this is an undo operation."""
        # Handle both enum and string values (use_enum_values=True stores as string)
        trans_type = self.transformation_type
        if hasattr(trans_type, 'value'):
            return trans_type == TransformationType.UNDO
        return trans_type == TransformationType.UNDO.value

    def get_target_column_names(self) -> list[str]:
        """Get list of target column names."""
        return [t.column_name for t in self.target_columns]

    def get_affected_columns(self) -> list[str]:
        """Get all columns affected (targets + condition columns)."""
        columns = set(self.get_target_column_names())
        for cond in self.conditions:
            columns.add(cond.column)
        return list(columns)

    def to_engineer_prompt(self) -> str:
        """
        Format this plan as instructions for the Engineer agent.

        Produces a clear, structured prompt that the Engineer
        can use to generate pandas code.
        """
        # Handle both enum and string values (use_enum_values=True stores as string)
        trans_type = self.transformation_type
        if hasattr(trans_type, 'value'):
            trans_type = trans_type.value

        lines = [
            "## Technical Plan",
            f"**Operation**: {trans_type}",
            f"**Description**: {self.explanation}",
        ]

        if self.target_columns:
            cols = ", ".join(self.get_target_column_names())
            lines.append(f"**Target Columns**: {cols}")

        if self.conditions:
            lines.append("**Conditions**:")
            for cond in self.conditions:
                op = cond.operator
                if hasattr(op, 'value'):
                    op = op.value
                lines.append(f"  - {cond.column} {op} {cond.value}")

        if self.parameters:
            lines.append("**Parameters**:")
            for key, val in self.parameters.items():
                lines.append(f"  - {key}: {val}")

        if self.rollback_to_node_id:
            lines.append(f"**Rollback Target**: {self.rollback_to_node_id}")

        return "\n".join(lines)

    class Config:
        """Pydantic configuration."""
        use_enum_values = True  # Serialize enums as their values
