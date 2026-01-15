# =============================================================================
# agents/transformations/advanced_ops.py - Advanced Operations
# =============================================================================
# Advanced data transformation operations:
# - conditional_replace: If-then-else logic (CASE/WHEN)
# - coalesce: First non-null value from multiple columns
# - explode: Expand list column into multiple rows
# - lag_lead: Shift values up/down by N rows
# =============================================================================

import pandas as pd
import numpy as np
from typing import Any

from agents.models.technical_plan import TechnicalPlan, TransformationType
from agents.transformations.registry import register


@register(TransformationType.CONDITIONAL_REPLACE)
def conditional_replace(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    If-then-else logic for replacing values (similar to SQL CASE/WHEN).

    Parameters (from plan.parameters):
        conditions: List of {condition, value} dicts to evaluate in order
                   condition: String expression like "df['col'] > 10"
                   value: Value to assign if condition is True
        default: Value to use if no conditions match (default: None/keep original)
        target_column: Column to modify (or new column name)
        source_column: Optional source column for conditions

    Example:
        conditions=[
            {"condition": "df['age'] < 18", "value": "Minor"},
            {"condition": "df['age'] < 65", "value": "Adult"},
        ],
        default="Senior"
        -> Creates categories based on age

    Example (simpler):
        conditions=[
            {"when": "< 0", "then": "Negative"},
            {"when": ">= 0", "then": "Non-negative"},
        ]
        with source_column="amount"
    """
    conditions = plan.parameters.get("conditions", [])
    default = plan.parameters.get("default")
    target_column = plan.parameters.get("target_column", "result")
    source_column = plan.parameters.get("source_column")

    result = df.copy()

    # Start with default value or copy of existing column
    if default is not None:
        result[target_column] = default
    elif target_column in result.columns:
        pass  # Keep existing values as default
    else:
        result[target_column] = np.nan

    code_parts = [f"df['{target_column}'] = {repr(default)}  # default"]

    # Apply conditions in reverse order (last has highest priority)
    for cond_spec in reversed(conditions):
        condition = cond_spec.get("condition") or cond_spec.get("when")
        value = cond_spec.get("value") or cond_spec.get("then")

        if not condition:
            continue

        # Handle simplified "when" syntax with source_column
        if source_column and not condition.startswith("df["):
            # Convert "< 0" to "df['source_col'] < 0"
            condition = f"df['{source_column}'] {condition}"

        try:
            # Create safe evaluation context
            safe_context = {'df': result, 'pd': pd, 'np': np}
            mask = eval(condition, {"__builtins__": {}}, safe_context)
            result.loc[mask, target_column] = value
            code_parts.append(f"df.loc[{condition}, '{target_column}'] = {repr(value)}")
        except Exception as e:
            continue

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.COALESCE)
def coalesce(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Return first non-null value from multiple columns.

    Parameters (from plan.parameters):
        target_columns: Columns to coalesce (in priority order)
        new_column: Name for result column (default: "coalesced")

    Example:
        email_primary=null, email_secondary="test@test.com", email_backup="backup@test.com"
        -> "test@test.com" (first non-null)
    """
    columns = plan.get_target_column_names()
    new_column = plan.parameters.get("new_column", "coalesced")

    result = df.copy()

    if not columns:
        return result, "# No columns specified for coalesce"

    # Start with first column
    result[new_column] = result[columns[0]]

    # Fill nulls from subsequent columns
    for col in columns[1:]:
        if col in result.columns:
            result[new_column] = result[new_column].fillna(result[col])

    code = f"df['{new_column}'] = df[{columns}].bfill(axis=1).iloc[:, 0]"
    return result, code


@register(TransformationType.EXPLODE)
def explode(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Expand list/array column into multiple rows.

    Parameters (from plan.parameters):
        target_columns: Column(s) containing lists to explode
        ignore_index: Reset index after exploding (default: True)

    Example:
        id=1, tags=["A", "B", "C"]
        -> Three rows: (1, "A"), (1, "B"), (1, "C")
    """
    columns = plan.get_target_column_names()
    ignore_index = plan.parameters.get("ignore_index", True)

    result = df.copy()

    if not columns:
        return result, "# No columns specified for explode"

    # Explode each column in sequence
    for col in columns:
        if col in result.columns:
            result = result.explode(col, ignore_index=ignore_index)

    code = f"df = df.explode({columns if len(columns) > 1 else repr(columns[0])}, ignore_index={ignore_index})"
    return result, code


@register(TransformationType.CUSTOM)
def custom(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Execute custom pandas code.

    Parameters (from plan.parameters):
        code: Python/pandas code to execute (must use 'df' variable)

    Security: Only allows safe operations (pd, np, basic math).

    Example:
        code="df['total'] = df['price'] * df['quantity']"
    """
    code_str = plan.parameters.get("code", "")

    if not code_str:
        return df.copy(), "# No code provided"

    result = df.copy()

    # Create safe evaluation context
    safe_context = {
        'df': result,
        'pd': pd,
        'np': np,
        'abs': abs,
        'round': round,
        'min': min,
        'max': max,
        'sum': sum,
        'len': len,
    }

    try:
        # Execute the code
        exec(code_str, {"__builtins__": {}}, safe_context)
        # Get the modified df
        result = safe_context['df']
    except Exception as e:
        raise ValueError(f"Custom code execution failed: {str(e)}")

    return result, code_str


@register(TransformationType.LAG_LEAD)
def lag_lead(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Shift column values up or down by N rows.

    Parameters (from plan.parameters):
        periods: Number of periods to shift (positive=lag/previous, negative=lead/next)
        target_columns: Columns to shift
        partition_by: Optional column(s) to shift within groups
        suffix: Suffix for new columns (default: "_lag{n}" or "_lead{n}")
        fill_value: Value to fill shifted gaps (default: None/NaN)

    Example (lag):
        values [A, B, C, D], periods=1 -> [NaN, A, B, C] (previous row values)

    Example (lead):
        values [A, B, C, D], periods=-1 -> [B, C, D, NaN] (next row values)
    """
    columns = plan.get_target_column_names()
    periods = plan.parameters.get("periods", 1)
    partition_by = plan.parameters.get("partition_by")
    suffix = plan.parameters.get("suffix")
    fill_value = plan.parameters.get("fill_value")

    # Generate default suffix based on direction
    if suffix is None:
        if periods >= 0:
            suffix = f"_lag{periods}"
        else:
            suffix = f"_lead{abs(periods)}"

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        new_col = f"{col}{suffix}"

        if partition_by:
            # Shift within groups
            result[new_col] = result.groupby(partition_by)[col].shift(periods, fill_value=fill_value)
            code_parts.append(f"df['{new_col}'] = df.groupby({partition_by})['{col}'].shift({periods})")
        else:
            # Global shift
            result[new_col] = result[col].shift(periods, fill_value=fill_value)
            code_parts.append(f"df['{new_col}'] = df['{col}'].shift({periods})")

    code = "\n".join(code_parts)
    return result, code
