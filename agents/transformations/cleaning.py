# =============================================================================
# agents/transformations/cleaning.py - Cleaning Operations
# =============================================================================
# Basic cleaning and formatting transformations:
# - trim_whitespace: Remove leading/trailing whitespace
# - change_case: Convert to upper/lower/title case
# - deduplicate: Remove duplicate rows
# - replace_values: Find and replace (with regex support)
# - fill_nulls: Fill missing values
# - format_date: Format datetime columns
# - sanitize_headers: Clean column names
# - standardize: Trim + lowercase (convenience)
# =============================================================================

import re
import pandas as pd
import numpy as np
from typing import Any

from agents.models.technical_plan import TechnicalPlan, TransformationType
from agents.transformations.registry import register


@register(TransformationType.TRIM_WHITESPACE)
def trim_whitespace(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Remove leading and trailing whitespace from string columns.

    Parameters (from plan):
        target_columns: Columns to trim. If empty, trims all string columns.

    Example:
        Input:  "  Alice  " -> "Alice"
    """
    columns = plan.get_target_column_names()

    # If no columns specified, use all object (string) columns
    if not columns:
        columns = [c for c in df.columns if df[c].dtype == 'object']

    result = df.copy()
    for col in columns:
        if col in result.columns and result[col].dtype == 'object':
            result[col] = result[col].str.strip()

    code = f"df[{columns}] = df[{columns}].apply(lambda x: x.str.strip() if x.dtype == 'object' else x)"
    return result, code


@register(TransformationType.CHANGE_CASE)
def change_case(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Convert text columns to upper, lower, title, or sentence case.

    Parameters (from plan.parameters):
        case_type: "lower", "upper", "title", or "sentence"
        target_columns: Columns to transform

    Example:
        "ALICE SMITH" (lower) -> "alice smith"
        "alice smith" (title) -> "Alice Smith"
    """
    columns = plan.get_target_column_names()
    case_type = plan.parameters.get("case_type", "lower")

    result = df.copy()
    for col in columns:
        if col in result.columns and result[col].dtype == 'object':
            if case_type == "lower":
                result[col] = result[col].str.lower()
            elif case_type == "upper":
                result[col] = result[col].str.upper()
            elif case_type == "title":
                result[col] = result[col].str.title()
            elif case_type == "sentence":
                # Capitalize first letter of each cell
                result[col] = result[col].str.capitalize()

    code = f"df[{columns}] = df[{columns}].str.{case_type}()"
    return result, code


@register(TransformationType.DEDUPLICATE)
def deduplicate(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Remove duplicate rows.

    Parameters (from plan.parameters):
        keep: "first", "last", or False (remove all duplicates)
        target_columns: Columns to check for duplicates. If empty, checks all.

    Example:
        Rows [A, B, A, C] with keep="first" -> [A, B, C]
    """
    columns = plan.get_target_column_names() or None
    keep = plan.parameters.get("keep", "first")

    # Handle keep=False (drop all duplicates)
    if keep == "false" or keep is False:
        keep = False

    result = df.drop_duplicates(subset=columns, keep=keep)

    subset_str = f"subset={columns}, " if columns else ""
    code = f"df = df.drop_duplicates({subset_str}keep={repr(keep)})"
    return result, code


@register(TransformationType.REPLACE_VALUES)
def replace_values(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Find and replace values with regex support.

    Parameters (from plan.parameters):
        old_value: Value or pattern to find
        new_value: Replacement value
        regex: If True, treat old_value as regex pattern (default: False)
        target_columns: Columns to transform. If empty, applies to all.

    Example (regex=True):
        Pattern: r"\\d{3}-\\d{4}" finds phone numbers like "555-1234"
        Pattern: r"^\\s+|\\s+$" removes leading/trailing whitespace
    """
    columns = plan.get_target_column_names()
    old_value = plan.parameters.get("old_value")
    new_value = plan.parameters.get("new_value", "")
    use_regex = plan.parameters.get("regex", False)

    result = df.copy()

    if columns:
        for col in columns:
            if col in result.columns:
                result[col] = result[col].replace(old_value, new_value, regex=use_regex)
    else:
        # Apply to all columns
        result = result.replace(old_value, new_value, regex=use_regex)

    col_str = f"[{columns}]" if columns else ""
    code = f"df{col_str} = df{col_str}.replace({repr(old_value)}, {repr(new_value)}, regex={use_regex})"
    return result, code


@register(TransformationType.FILL_NULLS)
def fill_nulls(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Fill missing/null values.

    Parameters (from plan.parameters):
        method: "value", "mean", "median", "mode", "forward", "backward", "interpolate"
        value: Static value to fill with (when method="value")
        target_columns: Columns to fill. If empty, applies to all.

    Example:
        method="mean" fills nulls with column average
        method="forward" fills with previous non-null value
    """
    columns = plan.get_target_column_names()
    method = plan.parameters.get("method", "value")
    value = plan.parameters.get("value")
    fill_value = plan.parameters.get("fill_value", value)  # Also check fill_value

    result = df.copy()

    # If no columns specified, use all columns with nulls
    if not columns:
        columns = [c for c in df.columns if df[c].isna().any()]

    # Validate: if method is "value", we need a value
    if method == "value" and fill_value is None:
        raise ValueError("Must specify a fill 'value' or 'method' (mean, median, mode, forward, backward).")

    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        if method == "value":
            result[col] = result[col].fillna(fill_value)
            code_parts.append(f"df['{col}'] = df['{col}'].fillna({repr(fill_value)})")

        elif method == "mean":
            fill_val = result[col].mean()
            result[col] = result[col].fillna(fill_val)
            code_parts.append(f"df['{col}'] = df['{col}'].fillna(df['{col}'].mean())")

        elif method == "median":
            fill_val = result[col].median()
            result[col] = result[col].fillna(fill_val)
            code_parts.append(f"df['{col}'] = df['{col}'].fillna(df['{col}'].median())")

        elif method == "mode":
            mode_val = result[col].mode()
            if len(mode_val) > 0:
                result[col] = result[col].fillna(mode_val.iloc[0])
            code_parts.append(f"df['{col}'] = df['{col}'].fillna(df['{col}'].mode().iloc[0])")

        elif method == "forward":
            result[col] = result[col].ffill()
            code_parts.append(f"df['{col}'] = df['{col}'].ffill()")

        elif method == "backward":
            result[col] = result[col].bfill()
            code_parts.append(f"df['{col}'] = df['{col}'].bfill()")

        elif method == "interpolate":
            result[col] = result[col].interpolate()
            code_parts.append(f"df['{col}'] = df['{col}'].interpolate()")

    code = "\n".join(code_parts) if code_parts else f"# No null values found in {columns}"
    return result, code


@register(TransformationType.FORMAT_DATE)
def format_date(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Reformat datetime columns to a specific string format.

    Parameters (from plan.parameters):
        output_format: strftime format string (e.g., "%Y-%m-%d", "%m/%d/%Y")
        target_columns: Columns to format

    Example:
        "2024-01-15 10:30:00" with "%m/%d/%Y" -> "01/15/2024"
    """
    columns = plan.get_target_column_names()
    output_format = plan.parameters.get("output_format", "%Y-%m-%d")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col in result.columns:
            # First convert to datetime, then format
            result[col] = pd.to_datetime(result[col], errors='coerce').dt.strftime(output_format)
            code_parts.append(
                f"df['{col}'] = pd.to_datetime(df['{col}'], errors='coerce').dt.strftime('{output_format}')"
            )

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.SANITIZE_HEADERS)
def sanitize_headers(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Clean and standardize column names.

    Parameters (from plan.parameters):
        style: "snake_case" or "camelCase"

    Example:
        "First Name" (snake_case) -> "first_name"
        "First Name" (camelCase) -> "firstName"
    """
    style = plan.parameters.get("style", "snake_case")

    def to_snake_case(name: str) -> str:
        """Convert column name to snake_case."""
        # Remove special characters except spaces and underscores
        name = re.sub(r'[^\w\s]', '', str(name))
        # Replace spaces with underscores
        name = re.sub(r'\s+', '_', name)
        # Convert camelCase to snake_case
        name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
        # Lowercase and clean up multiple underscores
        name = re.sub(r'_+', '_', name.lower())
        return name.strip('_')

    def to_camel_case(name: str) -> str:
        """Convert column name to camelCase."""
        # Remove special characters
        name = re.sub(r'[^\w\s]', '', str(name))
        # Split on spaces or underscores
        words = re.split(r'[\s_]+', name)
        # First word lowercase, rest title case
        if not words:
            return name
        return words[0].lower() + ''.join(w.title() for w in words[1:])

    result = df.copy()

    if style == "snake_case":
        result.columns = [to_snake_case(c) for c in result.columns]
    elif style == "camelCase":
        result.columns = [to_camel_case(c) for c in result.columns]

    code = f"df.columns = [to_{style.replace('C', '_c')}(c) for c in df.columns]"
    return result, code


@register(TransformationType.STANDARDIZE)
def standardize(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Standardize text columns (trim + lowercase).

    Convenience function combining TRIM_WHITESPACE and CHANGE_CASE.

    Parameters (from plan):
        target_columns: Columns to standardize. If empty, all string columns.
    """
    columns = plan.get_target_column_names()

    # If no columns specified, use all object columns
    if not columns:
        columns = [c for c in df.columns if df[c].dtype == 'object']

    result = df.copy()
    for col in columns:
        if col in result.columns and result[col].dtype == 'object':
            result[col] = result[col].str.strip().str.lower()

    code = f"df[{columns}] = df[{columns}].apply(lambda x: x.str.strip().str.lower() if x.dtype == 'object' else x)"
    return result, code
