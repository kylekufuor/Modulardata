# =============================================================================
# agents/transformations/filtering.py - Filtering & Sorting Operations
# =============================================================================
# Row and column filtering operations:
# - filter_rows: Keep rows matching conditions
# - drop_rows: Remove rows matching conditions
# - sort_rows: Sort by column(s)
# - select_columns: Keep only specified columns
# - drop_columns: Remove specified columns
# - slice_rows: Get first/last N rows
# =============================================================================

import pandas as pd
import numpy as np
from typing import Any

from agents.models.technical_plan import TechnicalPlan, TransformationType
from agents.transformations.registry import register
from agents.transformations.utils import build_condition_mask, conditions_to_code


@register(TransformationType.FILTER_ROWS)
def filter_rows(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Keep only rows matching the specified conditions.

    Parameters (from plan.conditions):
        conditions: List of FilterCondition objects (combined with AND)
        Each condition has: column, operator, value, case_sensitive

    Supported operators:
        eq, ne, gt, lt, gte, lte, isnull, notnull,
        contains (regex), startswith, endswith, regex,
        in, not_in, is_numeric, is_date

    Example:
        Keep rows where age > 18 AND status == "active"
    """
    mask = build_condition_mask(df, plan.conditions)
    result = df[mask].copy()

    code = f"df = df[{conditions_to_code(plan.conditions)}]"
    return result, code


@register(TransformationType.DROP_ROWS)
def drop_rows(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Remove rows matching the specified conditions.

    This is the inverse of filter_rows - removes matching rows instead of keeping them.

    Parameters (from plan.conditions):
        conditions: List of FilterCondition objects

    If conditions are empty but target_columns are provided, we infer:
        - Remove rows where any target column is null OR empty string

    Example:
        Remove rows where email is null
        conditions = [FilterCondition(column="email", operator="isnull")]
    """
    from agents.models.technical_plan import FilterCondition, FilterOperator

    # If no conditions but target_columns are provided, infer null/empty check
    conditions = plan.conditions
    if not conditions and plan.target_columns:
        # Build conditions for "isnull or empty" for each target column
        conditions = [
            FilterCondition(column=tc.column_name, operator=FilterOperator.ISNULL)
            for tc in plan.target_columns
        ]

        # Build mask for null OR empty
        target_cols = plan.get_target_column_names()
        mask = pd.Series([False] * len(df), index=df.index)

        for col in target_cols:
            if col in df.columns:
                # Match: null OR empty string (for string columns)
                col_null = df[col].isna()
                col_empty = df[col].astype(str).str.strip() == ''
                mask = mask | col_null | col_empty

        result = df[~mask].copy()
        code_parts = [f"(df['{col}'].isna() | (df['{col}'].astype(str).str.strip() == ''))"
                      for col in target_cols]
        condition_code = " | ".join(code_parts)
        code = f"df = df[~({condition_code})]"
        return result, code

    # Standard path with explicit conditions
    mask = build_condition_mask(df, conditions)
    result = df[~mask].copy()

    condition_code = conditions_to_code(conditions)
    code = f"df = df[~({condition_code})]"
    return result, code


@register(TransformationType.SORT_ROWS)
def sort_rows(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Sort rows by one or more columns.

    Parameters (from plan.parameters):
        ascending: True for ascending, False for descending
                   Can be a list for multiple columns: [True, False]
        na_position: "first" or "last" (default: "last")
        target_columns: Columns to sort by (in order of priority)

    Example:
        Sort by date descending, then by name ascending
    """
    columns = plan.get_target_column_names()
    ascending = plan.parameters.get("ascending", True)
    na_position = plan.parameters.get("na_position", "last")

    if not columns:
        return df.copy(), "# No columns specified for sorting"

    result = df.sort_values(
        by=columns,
        ascending=ascending,
        na_position=na_position
    ).reset_index(drop=True)

    code = f"df = df.sort_values(by={columns}, ascending={ascending}).reset_index(drop=True)"
    return result, code


@register(TransformationType.SELECT_COLUMNS)
def select_columns(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Keep only the specified columns, discarding all others.

    Parameters (from plan):
        target_columns: List of columns to keep

    Example:
        Select only ["name", "email", "age"] from a wider dataset
    """
    columns = plan.get_target_column_names()

    if not columns:
        return df.copy(), "# No columns specified"

    # Only include columns that exist
    valid_columns = [c for c in columns if c in df.columns]

    result = df[valid_columns].copy()

    code = f"df = df[{valid_columns}]"
    return result, code


@register(TransformationType.DROP_COLUMNS)
def drop_columns(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Remove the specified columns.

    Parameters (from plan):
        target_columns: List of columns to remove

    Example:
        Drop ["internal_id", "created_at", "updated_at"] columns
    """
    columns = plan.get_target_column_names()

    if not columns:
        return df.copy(), "# No columns specified"

    # Only drop columns that exist
    columns_to_drop = [c for c in columns if c in df.columns]

    result = df.drop(columns=columns_to_drop)

    code = f"df = df.drop(columns={columns_to_drop})"
    return result, code


@register(TransformationType.SLICE_ROWS)
def slice_rows(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Get the first N or last N rows.

    Parameters (from plan.parameters):
        n: Number of rows to return (default: 10)
        position: "head" (first N) or "tail" (last N)

    Example:
        Get top 100 rows: n=100, position="head"
        Get bottom 50 rows: n=50, position="tail"
    """
    n = plan.parameters.get("n", 10)
    position = plan.parameters.get("position", "head")

    if position == "head":
        result = df.head(n).copy()
        code = f"df = df.head({n})"
    elif position == "tail":
        result = df.tail(n).copy()
        code = f"df = df.tail({n})"
    else:
        # Default to head
        result = df.head(n).copy()
        code = f"df = df.head({n})"

    return result, code


@register(TransformationType.SAMPLE_ROWS)
def sample_rows(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Get a random sample of rows.

    Parameters (from plan.parameters):
        n: Number of rows to sample (use n or frac, not both)
        frac: Fraction of rows to sample (0.0-1.0)
        random_state: Seed for reproducibility (optional)
        replace: Allow sampling with replacement (default: False)

    Example:
        Sample 100 random rows: n=100
        Sample 10% of data: frac=0.1
    """
    n = plan.parameters.get("n")
    frac = plan.parameters.get("frac")
    random_state = plan.parameters.get("random_state")
    replace = plan.parameters.get("replace", False)

    if frac is not None:
        result = df.sample(frac=frac, random_state=random_state, replace=replace)
        code = f"df = df.sample(frac={frac}, random_state={random_state})"
    elif n is not None:
        # Don't sample more than available rows
        n = min(n, len(df))
        result = df.sample(n=n, random_state=random_state, replace=replace)
        code = f"df = df.sample(n={n}, random_state={random_state})"
    else:
        result = df.copy()
        code = "# No sample size specified"

    return result, code
