# =============================================================================
# agents/transformations/aggregation.py - Aggregation & Enrichment Operations
# =============================================================================
# Data aggregation and enrichment operations:
# - group_by: Aggregate data by groups
# - cumulative: Running totals and cumulative operations
# - join: Merge data from another source (VLOOKUP-like)
# =============================================================================

import pandas as pd
import numpy as np
from typing import Any

from agents.models.technical_plan import TechnicalPlan, TransformationType
from agents.transformations.registry import register


@register(TransformationType.GROUP_BY)
def group_by(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Aggregate data by grouping columns.

    Parameters (from plan.parameters):
        by_columns: Column(s) to group by (can also use target_columns)
        aggregations: Dict of {column: function} or {column: [functions]}
                     Functions: "sum", "mean", "count", "min", "max", "first", "last", "std", "var"
        as_index: If False, grouped columns become regular columns (default: False)

    Example:
        Group by "category" and get sum of "sales", count of "orders"
        by_columns=["category"]
        aggregations={"sales": "sum", "orders": "count"}
    """
    by_columns = plan.parameters.get("by_columns", [])

    # Also support using target_columns for grouping
    if not by_columns:
        by_columns = plan.get_target_column_names()

    aggregations = plan.parameters.get("aggregations", {})
    as_index = plan.parameters.get("as_index", False)

    if not by_columns:
        return df.copy(), "# No grouping columns specified"

    if not aggregations:
        # Default: count all columns
        aggregations = {col: "count" for col in df.columns if col not in by_columns}

    result = df.groupby(by_columns, as_index=as_index).agg(aggregations)

    # Flatten column names if MultiIndex
    if isinstance(result.columns, pd.MultiIndex):
        result.columns = ['_'.join(col).strip('_') for col in result.columns.values]

    if as_index:
        result = result.reset_index()

    code = f"df = df.groupby({by_columns}).agg({aggregations}).reset_index()"
    return result, code


@register(TransformationType.CUMULATIVE)
def cumulative(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Calculate cumulative/running operations.

    Parameters (from plan.parameters):
        operation: "sum", "count", "mean", "min", "max", "prod"
        target_columns: Columns to apply cumulative operation
        partition_by: Optional column(s) to partition/reset the cumulative calculation
        suffix: Suffix for new columns (default: "_cum{operation}")

    Example:
        Running total of sales by category:
        operation="sum", target_columns=["sales"], partition_by=["category"]
    """
    columns = plan.get_target_column_names()
    operation = plan.parameters.get("operation", "sum")
    partition_by = plan.parameters.get("partition_by")
    suffix = plan.parameters.get("suffix", f"_cum{operation}")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        new_col = f"{col}{suffix}"

        if partition_by:
            # Cumulative within groups
            if operation == "sum":
                result[new_col] = result.groupby(partition_by)[col].cumsum()
            elif operation == "count":
                result[new_col] = result.groupby(partition_by).cumcount() + 1
            elif operation == "mean":
                result[new_col] = result.groupby(partition_by)[col].expanding().mean().reset_index(level=0, drop=True)
            elif operation == "min":
                result[new_col] = result.groupby(partition_by)[col].cummin()
            elif operation == "max":
                result[new_col] = result.groupby(partition_by)[col].cummax()
            elif operation == "prod":
                result[new_col] = result.groupby(partition_by)[col].cumprod()
            code_parts.append(f"df['{new_col}'] = df.groupby({partition_by})['{col}'].cum{operation}()")
        else:
            # Global cumulative
            if operation == "sum":
                result[new_col] = result[col].cumsum()
            elif operation == "count":
                result[new_col] = range(1, len(result) + 1)
            elif operation == "mean":
                result[new_col] = result[col].expanding().mean()
            elif operation == "min":
                result[new_col] = result[col].cummin()
            elif operation == "max":
                result[new_col] = result[col].cummax()
            elif operation == "prod":
                result[new_col] = result[col].cumprod()
            code_parts.append(f"df['{new_col}'] = df['{col}'].cum{operation}()")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.JOIN)
def join(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Merge/join with another dataset (VLOOKUP-like operation).

    Parameters (from plan.parameters):
        right_data: Path to CSV file or DataFrame to join with
        on: Column(s) to join on (same name in both)
        left_on: Left join column(s) if different names
        right_on: Right join column(s) if different names
        how: "left", "right", "inner", "outer" (default: "left")
        suffix: Suffix for overlapping columns (default: "_right")

    Example:
        Join customer data to orders on customer_id:
        on="customer_id", how="left"

    Note: In production, right_data would be loaded from storage or passed in.
    This implementation expects right_data as a dict/list or DataFrame.
    """
    on = plan.parameters.get("on")
    left_on = plan.parameters.get("left_on", on)
    right_on = plan.parameters.get("right_on", on)
    how = plan.parameters.get("how", "left")
    suffix = plan.parameters.get("suffix", "_right")
    right_data = plan.parameters.get("right_data")

    # Handle different right_data formats
    if right_data is None:
        # Placeholder - in production, this would load from storage
        return df.copy(), "# No right_data provided for join"

    if isinstance(right_data, dict):
        # Convert dict to DataFrame
        right_df = pd.DataFrame(right_data)
    elif isinstance(right_data, list):
        # List of dicts
        right_df = pd.DataFrame(right_data)
    elif isinstance(right_data, pd.DataFrame):
        right_df = right_data
    elif isinstance(right_data, str):
        # Path to CSV - would need to load
        # In production: right_df = pd.read_csv(right_data)
        return df.copy(), f"# Would load right_data from: {right_data}"
    else:
        return df.copy(), "# Invalid right_data format"

    # Perform the merge
    if on:
        result = df.merge(right_df, on=on, how=how, suffixes=('', suffix))
        code = f"df = df.merge(right_df, on={repr(on)}, how='{how}')"
    else:
        result = df.merge(right_df, left_on=left_on, right_on=right_on, how=how, suffixes=('', suffix))
        code = f"df = df.merge(right_df, left_on={repr(left_on)}, right_on={repr(right_on)}, how='{how}')"

    return result, code


@register(TransformationType.RANK)
def rank_values(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Assign ranks to values in a column.

    Parameters (from plan.parameters):
        method: "average", "min", "max", "first", "dense" (default: "average")
        ascending: True for lowest=1, False for highest=1 (default: True)
        partition_by: Optional column(s) to rank within groups
        suffix: Suffix for rank column (default: "_rank")
        target_columns: Columns to rank

    Example:
        Rank employees by sales within each department:
        target_columns=["sales"], partition_by=["department"]
    """
    columns = plan.get_target_column_names()
    method = plan.parameters.get("method", "average")
    ascending = plan.parameters.get("ascending", True)
    partition_by = plan.parameters.get("partition_by")
    suffix = plan.parameters.get("suffix", "_rank")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        new_col = f"{col}{suffix}"

        if partition_by:
            result[new_col] = result.groupby(partition_by)[col].rank(method=method, ascending=ascending)
            code_parts.append(f"df['{new_col}'] = df.groupby({partition_by})['{col}'].rank(method='{method}', ascending={ascending})")
        else:
            result[new_col] = result[col].rank(method=method, ascending=ascending)
            code_parts.append(f"df['{new_col}'] = df['{col}'].rank(method='{method}', ascending={ascending})")

    code = "\n".join(code_parts)
    return result, code
