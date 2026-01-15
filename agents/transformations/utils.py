# =============================================================================
# agents/transformations/utils.py - Shared Transformation Utilities
# =============================================================================
# Common utilities used across transformation functions.
#
# Key functions:
# - build_condition_mask: Creates boolean mask from FilterConditions
# - conditions_to_code: Generates pandas code string from conditions
# =============================================================================

import pandas as pd
import numpy as np
from typing import Any

from agents.models.technical_plan import FilterCondition, FilterOperator


def build_condition_mask(df: pd.DataFrame, conditions: list[FilterCondition]) -> pd.Series:
    """
    Build a boolean mask from a list of filter conditions.

    Multiple conditions are combined with AND logic.
    Supports regex patterns for string operations.

    Args:
        df: DataFrame to filter
        conditions: List of FilterCondition objects

    Returns:
        Boolean Series where True = row matches all conditions

    Example:
        conditions = [
            FilterCondition(column="email", operator=FilterOperator.ISNULL),
            FilterCondition(column="age", operator=FilterOperator.GT, value=18)
        ]
        mask = build_condition_mask(df, conditions)
        filtered_df = df[mask]
    """
    if not conditions:
        # No conditions = all rows match
        return pd.Series([True] * len(df), index=df.index)

    mask = pd.Series([True] * len(df), index=df.index)

    for cond in conditions:
        col = df[cond.column]
        op = cond.operator
        val = cond.value
        case_sensitive = cond.case_sensitive

        # Handle both enum and string values
        if hasattr(op, 'value'):
            op = op.value

        # Build condition mask based on operator
        if op == "isnull":
            m = col.isna()

        elif op == "notnull":
            m = col.notna()

        elif op == "eq":
            m = col == val

        elif op == "ne":
            m = col != val

        elif op == "gt":
            m = col > val

        elif op == "lt":
            m = col < val

        elif op == "gte":
            m = col >= val

        elif op == "lte":
            m = col <= val

        elif op == "contains":
            # Supports regex via case_sensitive flag or if pattern looks like regex
            m = col.astype(str).str.contains(
                val,
                case=case_sensitive,
                na=False,
                regex=True  # Always enable regex for contains
            )

        elif op == "startswith":
            m = col.astype(str).str.startswith(val, na=False)

        elif op == "endswith":
            m = col.astype(str).str.endswith(val, na=False)

        elif op == "regex":
            # Explicit regex pattern matching
            m = col.astype(str).str.match(val, na=False)

        elif op == "in":
            m = col.isin(val if isinstance(val, list) else [val])

        elif op == "not_in":
            m = ~col.isin(val if isinstance(val, list) else [val])

        elif op == "is_numeric":
            m = pd.to_numeric(col, errors='coerce').notna()

        elif op == "is_date":
            m = pd.to_datetime(col, errors='coerce').notna()

        else:
            # Unknown operator - default to True (no filtering)
            m = pd.Series([True] * len(df), index=df.index)

        # AND with existing mask
        mask = mask & m

    return mask


def conditions_to_code(conditions: list[FilterCondition]) -> str:
    """
    Convert filter conditions to pandas code string.

    Args:
        conditions: List of FilterCondition objects

    Returns:
        String representation of pandas boolean expression

    Example:
        conditions = [
            FilterCondition(column="email", operator=FilterOperator.ISNULL),
            FilterCondition(column="age", operator=FilterOperator.GT, value=18)
        ]
        code = conditions_to_code(conditions)
        # Returns: "(df['email'].isna()) & (df['age'] > 18)"
    """
    if not conditions:
        return "True"

    code_parts = []

    for cond in conditions:
        col = cond.column
        op = cond.operator
        val = cond.value

        # Handle both enum and string values
        if hasattr(op, 'value'):
            op = op.value

        if op == "isnull":
            code_parts.append(f"df['{col}'].isna()")

        elif op == "notnull":
            code_parts.append(f"df['{col}'].notna()")

        elif op == "eq":
            code_parts.append(f"df['{col}'] == {repr(val)}")

        elif op == "ne":
            code_parts.append(f"df['{col}'] != {repr(val)}")

        elif op == "gt":
            code_parts.append(f"df['{col}'] > {repr(val)}")

        elif op == "lt":
            code_parts.append(f"df['{col}'] < {repr(val)}")

        elif op == "gte":
            code_parts.append(f"df['{col}'] >= {repr(val)}")

        elif op == "lte":
            code_parts.append(f"df['{col}'] <= {repr(val)}")

        elif op == "contains":
            code_parts.append(f"df['{col}'].str.contains({repr(val)}, na=False, regex=True)")

        elif op == "startswith":
            code_parts.append(f"df['{col}'].str.startswith({repr(val)}, na=False)")

        elif op == "endswith":
            code_parts.append(f"df['{col}'].str.endswith({repr(val)}, na=False)")

        elif op == "regex":
            code_parts.append(f"df['{col}'].str.match({repr(val)}, na=False)")

        elif op == "in":
            code_parts.append(f"df['{col}'].isin({repr(val)})")

        elif op == "not_in":
            code_parts.append(f"~df['{col}'].isin({repr(val)})")

        elif op == "is_numeric":
            code_parts.append(f"pd.to_numeric(df['{col}'], errors='coerce').notna()")

        elif op == "is_date":
            code_parts.append(f"pd.to_datetime(df['{col}'], errors='coerce').notna()")

    # Join with AND
    if len(code_parts) == 1:
        return code_parts[0]
    return " & ".join(f"({part})" for part in code_parts)


def safe_column_list(columns: list[str]) -> str:
    """
    Format a list of columns for code output.

    Args:
        columns: List of column names

    Returns:
        String representation suitable for pandas code
    """
    if len(columns) == 1:
        return repr(columns[0])
    return repr(columns)
