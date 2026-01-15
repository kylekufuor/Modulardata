# =============================================================================
# agents/transformations/restructuring.py - Data Restructuring Operations
# =============================================================================
# Structural transformations:
# - split_column: Split one column into multiple
# - merge_columns: Combine columns into one
# - pivot: Long to wide format
# - melt: Wide to long format
# - transpose: Flip rows and columns
# - reorder_columns: Change column order
# =============================================================================

import pandas as pd
import numpy as np
from typing import Any

from agents.models.technical_plan import TechnicalPlan, TransformationType
from agents.transformations.registry import register


@register(TransformationType.SPLIT_COLUMN)
def split_column(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Split a column into multiple columns based on a delimiter.

    Parameters (from plan.parameters):
        delimiter: String to split on (e.g., ",", " ", "-")
        new_columns: List of names for the new columns
        expand: If True, create separate columns. If False, create a list.
        max_splits: Maximum number of splits (optional)
        target_columns[0]: The column to split

    Example:
        "John,Doe" split by "," -> ["John", "Doe"] in columns ["first", "last"]
    """
    column = plan.target_columns[0].column_name
    delimiter = plan.parameters.get("delimiter", ",")
    new_columns = plan.parameters.get("new_columns", [])
    expand = plan.parameters.get("expand", True)
    max_splits = plan.parameters.get("max_splits", -1)

    result = df.copy()

    if expand:
        # Split and expand into separate columns
        n_splits = max_splits if max_splits > 0 else -1
        split_df = result[column].astype(str).str.split(delimiter, n=n_splits, expand=True)

        # Assign to new column names or auto-generate
        if new_columns:
            for i, new_col in enumerate(new_columns):
                if i < len(split_df.columns):
                    result[new_col] = split_df[i]
        else:
            # Auto-generate column names
            for i in range(len(split_df.columns)):
                result[f"{column}_{i+1}"] = split_df[i]
    else:
        # Create a list column
        result[f"{column}_split"] = result[column].astype(str).str.split(delimiter)

    new_cols_str = new_columns if new_columns else f"['{column}_1', '{column}_2', ...]"
    code = f"df[{new_cols_str}] = df['{column}'].str.split('{delimiter}', expand=True)"
    return result, code


@register(TransformationType.MERGE_COLUMNS)
def merge_columns(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Combine multiple columns into one.

    Parameters (from plan.parameters):
        separator: String to join values with (default: " ")
        new_column_name: Name for the merged column
        target_columns: Columns to merge

    Example:
        ["John", "Doe"] with separator=" " -> "John Doe"
    """
    columns = plan.get_target_column_names()
    separator = plan.parameters.get("separator", " ")
    new_name = plan.parameters.get("new_column_name", "merged")

    result = df.copy()

    # Convert to string and join
    result[new_name] = result[columns].astype(str).agg(separator.join, axis=1)

    code = f"df['{new_name}'] = df[{columns}].astype(str).agg('{separator}'.join, axis=1)"
    return result, code


@register(TransformationType.PIVOT)
def pivot(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Transform data from long to wide format (pivot table).

    Parameters (from plan.parameters):
        index: Column(s) to use as row labels
        columns: Column to use for new column headers
        values: Column(s) to aggregate
        aggfunc: Aggregation function (default: "first")
                 Options: "first", "sum", "mean", "count", "min", "max"

    Example:
        Long format with dates as rows, categories as pivot column
        -> Wide format with categories as columns
    """
    index = plan.parameters.get("index")
    columns = plan.parameters.get("columns")
    values = plan.parameters.get("values")
    aggfunc = plan.parameters.get("aggfunc", "first")

    result = df.pivot_table(
        index=index,
        columns=columns,
        values=values,
        aggfunc=aggfunc
    )

    # Reset index to make it a regular DataFrame
    result = result.reset_index()

    # Flatten column names if MultiIndex
    if isinstance(result.columns, pd.MultiIndex):
        result.columns = ['_'.join(str(c) for c in col).strip('_') for col in result.columns]

    code = (
        f"df = df.pivot_table(index={repr(index)}, columns={repr(columns)}, "
        f"values={repr(values)}, aggfunc='{aggfunc}').reset_index()"
    )
    return result, code


@register(TransformationType.MELT)
def melt(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Transform data from wide to long format (unpivot).

    Parameters (from plan.parameters):
        id_vars: Columns to keep as identifiers
        value_vars: Columns to unpivot (if None, uses all non-id columns)
        var_name: Name for the variable column (default: "variable")
        value_name: Name for the value column (default: "value")

    Example:
        Wide: id, jan_sales, feb_sales
        -> Long: id, month, sales
    """
    id_vars = plan.parameters.get("id_vars", [])
    value_vars = plan.parameters.get("value_vars")
    var_name = plan.parameters.get("var_name", "variable")
    value_name = plan.parameters.get("value_name", "value")

    result = df.melt(
        id_vars=id_vars if id_vars else None,
        value_vars=value_vars,
        var_name=var_name,
        value_name=value_name
    )

    code = (
        f"df = df.melt(id_vars={repr(id_vars)}, value_vars={repr(value_vars)}, "
        f"var_name='{var_name}', value_name='{value_name}')"
    )
    return result, code


@register(TransformationType.TRANSPOSE)
def transpose(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Flip the DataFrame's rows and columns.

    Parameters (from plan.parameters):
        use_first_column_as_header: If True, use first column values as new headers

    Note: This converts all data to object type to handle mixed types.
    """
    use_first_col = plan.parameters.get("use_first_column_as_header", False)

    if use_first_col and len(df.columns) > 0:
        # Set first column as index before transposing
        result = df.set_index(df.columns[0]).T.reset_index()
        result.columns.name = None
    else:
        result = df.T.reset_index()
        result.columns = ['index'] + [f'col_{i}' for i in range(len(result.columns) - 1)]

    code = "df = df.T.reset_index()"
    return result, code


@register(TransformationType.REORDER_COLUMNS)
def reorder_columns(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Change the order of columns.

    Parameters (from plan.parameters):
        order: List of column names in desired order
               Columns not in the list will be dropped!
        keep_unlisted: If True, append unlisted columns at the end (default: False)

    Example:
        order=["name", "age", "email"] reorders to that sequence
    """
    order = plan.parameters.get("order", [])
    keep_unlisted = plan.parameters.get("keep_unlisted", False)

    if not order:
        # No order specified, return as-is
        return df.copy(), "# No column order specified"

    if keep_unlisted:
        # Add any columns not in order to the end
        remaining = [c for c in df.columns if c not in order]
        final_order = order + remaining
    else:
        final_order = order

    # Only include columns that exist
    final_order = [c for c in final_order if c in df.columns]

    result = df[final_order].copy()

    code = f"df = df[{final_order}]"
    return result, code


@register(TransformationType.RENAME_COLUMN)
def rename_column(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Rename one or more columns.

    Parameters (from plan.parameters):
        rename_map: Dictionary of {old_name: new_name}
        OR
        target_columns[0]: Old column name
        new_name: New column name

    Example:
        {"old_col": "new_col"} or target_columns + new_name parameter
    """
    rename_map = plan.parameters.get("rename_map", {})

    # Also support single column rename via target_columns
    if not rename_map and plan.target_columns:
        old_name = plan.target_columns[0].column_name
        new_name = plan.parameters.get("new_name", old_name)
        rename_map = {old_name: new_name}

    result = df.rename(columns=rename_map)

    code = f"df = df.rename(columns={rename_map})"
    return result, code
