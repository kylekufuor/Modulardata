# =============================================================================
# agents/transformations/column_math.py - Column Math Operations
# =============================================================================
# Column-level mathematical and type operations:
# - add_column: Create calculated columns
# - convert_type: Change column data types
# - round_numbers: Round numeric values
# - normalize: Scale/normalize data (min-max or z-score)
# - extract_pattern: Extract regex patterns
# - parse_date: Parse strings as dates
# =============================================================================

import re
import pandas as pd
import numpy as np
from typing import Any

from agents.models.technical_plan import TechnicalPlan, TransformationType
from agents.transformations.registry import register


# Safe operations allowed in add_column expressions
SAFE_OPERATIONS = {
    'pd': pd,
    'np': np,
    'abs': abs,
    'round': round,
    'min': min,
    'max': max,
    'sum': sum,
    'len': len,
}


@register(TransformationType.ADD_COLUMN)
def add_column(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Add a new calculated column.

    Parameters (from plan.parameters):
        name: Name for the new column
        expression: Python/pandas expression using 'df' to reference data
                   Example: "df['price'] * df['quantity']"
                   Example: "df['first_name'] + ' ' + df['last_name']"

    Security: Only allows safe operations (pd, np, basic math).

    Example:
        expression="df['total'] / df['quantity']" creates unit_price column
    """
    name = plan.parameters.get("name", "new_column")
    expression = plan.parameters.get("expression", "")

    result = df.copy()

    # Create safe evaluation context
    safe_context = SAFE_OPERATIONS.copy()
    safe_context['df'] = result

    try:
        # Evaluate expression with limited scope
        result[name] = eval(expression, {"__builtins__": {}}, safe_context)
    except Exception as e:
        raise ValueError(f"Invalid expression '{expression}': {str(e)}")

    code = f"df['{name}'] = {expression}"
    return result, code


@register(TransformationType.CONVERT_TYPE)
def convert_type(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Convert column(s) to a different data type.

    Parameters (from plan.parameters):
        target_type: "int", "float", "str", "bool", "datetime", "category"
        errors: "coerce" (invalid -> NaN), "ignore", "raise" (default: "coerce")

    Example:
        Convert "123" (string) to 123 (int)
        Convert "true"/"false" to True/False
    """
    columns = plan.get_target_column_names()
    target_type = plan.parameters.get("target_type", "str")
    errors = plan.parameters.get("errors", "coerce")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        if target_type in ("int", "int64", "Int64"):
            # Use nullable Int64 to handle NaN values
            result[col] = pd.to_numeric(result[col], errors=errors)
            if errors == "coerce":
                result[col] = result[col].astype('Int64')  # Nullable integer
            else:
                result[col] = result[col].astype(int)
            code_parts.append(f"df['{col}'] = pd.to_numeric(df['{col}'], errors='{errors}').astype('Int64')")

        elif target_type in ("float", "float64"):
            result[col] = pd.to_numeric(result[col], errors=errors)
            code_parts.append(f"df['{col}'] = pd.to_numeric(df['{col}'], errors='{errors}')")

        elif target_type in ("str", "string"):
            result[col] = result[col].astype(str)
            code_parts.append(f"df['{col}'] = df['{col}'].astype(str)")

        elif target_type == "bool":
            # Handle common boolean representations
            bool_map = {
                'true': True, 'false': False,
                'yes': True, 'no': False,
                '1': True, '0': False,
                1: True, 0: False
            }
            result[col] = result[col].map(lambda x: bool_map.get(str(x).lower(), bool(x)))
            code_parts.append(f"df['{col}'] = df['{col}'].astype(bool)")

        elif target_type == "datetime":
            result[col] = pd.to_datetime(result[col], errors=errors)
            code_parts.append(f"df['{col}'] = pd.to_datetime(df['{col}'], errors='{errors}')")

        elif target_type == "category":
            result[col] = result[col].astype('category')
            code_parts.append(f"df['{col}'] = df['{col}'].astype('category')")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.ROUND_NUMBERS)
def round_numbers(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Round numeric columns to specified decimal places.

    Parameters (from plan.parameters):
        decimals: Number of decimal places (default: 0 for integers)
        target_columns: Columns to round

    Example:
        3.14159 with decimals=2 -> 3.14
    """
    columns = plan.get_target_column_names()
    decimals = plan.parameters.get("decimals", 0)

    result = df.copy()

    for col in columns:
        if col in result.columns:
            result[col] = result[col].round(decimals)

    code = f"df[{columns}] = df[{columns}].round({decimals})"
    return result, code


@register(TransformationType.NORMALIZE)
def normalize(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Normalize/scale numeric columns.

    Parameters (from plan.parameters):
        method: "minmax" (0-1 range) or "zscore" (mean=0, std=1)
        target_columns: Columns to normalize
        suffix: Suffix for new columns (default: None, overwrites original)

    Example:
        minmax: (x - min) / (max - min) -> values between 0 and 1
        zscore: (x - mean) / std -> values with mean=0, std=1
    """
    columns = plan.get_target_column_names()
    method = plan.parameters.get("method", "minmax")
    suffix = plan.parameters.get("suffix")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        new_col = f"{col}{suffix}" if suffix else col

        if method == "minmax":
            min_val = result[col].min()
            max_val = result[col].max()
            if max_val != min_val:
                result[new_col] = (result[col] - min_val) / (max_val - min_val)
            else:
                result[new_col] = 0.0
            code_parts.append(
                f"df['{new_col}'] = (df['{col}'] - df['{col}'].min()) / (df['{col}'].max() - df['{col}'].min())"
            )

        elif method == "zscore":
            mean = result[col].mean()
            std = result[col].std()
            if std != 0:
                result[new_col] = (result[col] - mean) / std
            else:
                result[new_col] = 0.0
            code_parts.append(
                f"df['{new_col}'] = (df['{col}'] - df['{col}'].mean()) / df['{col}'].std()"
            )

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.EXTRACT_PATTERN)
def extract_pattern(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Extract text matching a regex pattern.

    Parameters (from plan.parameters):
        pattern: Regular expression pattern with optional capture groups
        group: Which capture group to extract (0=whole match, 1=first group, etc.)
        new_column: Name for the extracted column (default: {original}_extracted)

    Example:
        Pattern: r"(\d{3})-(\d{4})" extracts area code (group 1) or full number (group 0)
        Pattern: r"@(\w+)\.com" extracts domain name from email
    """
    column = plan.target_columns[0].column_name
    pattern = plan.parameters.get("pattern", "")
    new_column = plan.parameters.get("new_column", f"{column}_extracted")

    result = df.copy()

    # Check if pattern has capture groups
    has_groups = '(' in pattern and ')' in pattern

    if has_groups:
        # Extract using capture groups
        extracted = result[column].astype(str).str.extract(pattern, expand=False)
        if isinstance(extracted, pd.DataFrame):
            # Multiple groups - take first by default
            result[new_column] = extracted.iloc[:, 0]
        else:
            result[new_column] = extracted
    else:
        # No groups - extract whole match
        result[new_column] = result[column].astype(str).str.extract(f"({pattern})", expand=False)

    code = f"df['{new_column}'] = df['{column}'].str.extract(r'{pattern}')"
    return result, code


@register(TransformationType.PARSE_DATE)
def parse_date(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Parse string columns as datetime.

    Parameters (from plan.parameters):
        format: strptime format string (e.g., "%Y-%m-%d", "%m/%d/%Y")
                If not provided, pandas will infer the format
        target_columns: Columns to parse
        errors: "coerce" (invalid -> NaT), "raise", "ignore"

    Example:
        "01/15/2024" with format="%m/%d/%Y" -> datetime(2024, 1, 15)
    """
    columns = plan.get_target_column_names()
    date_format = plan.parameters.get("format")
    errors = plan.parameters.get("errors", "coerce")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col in result.columns:
            if date_format:
                result[col] = pd.to_datetime(result[col], format=date_format, errors=errors)
                code_parts.append(f"df['{col}'] = pd.to_datetime(df['{col}'], format='{date_format}', errors='{errors}')")
            else:
                result[col] = pd.to_datetime(result[col], errors=errors)
                code_parts.append(f"df['{col}'] = pd.to_datetime(df['{col}'], errors='{errors}')")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.HANDLE_OUTLIERS)
def handle_outliers(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Identify and handle outlier values.

    Parameters (from plan.parameters):
        method: "cap" (cap at threshold), "remove" (drop rows), "flag" (add bool column), "replace_null"
        threshold_method: "iqr" (1.5*IQR), "zscore" (default: |z| > 3), "percentile"
        lower_percentile: For percentile method (default: 1)
        upper_percentile: For percentile method (default: 99)
        target_columns: Columns to check for outliers

    Example:
        method="cap" with IQR caps values at Q1-1.5*IQR and Q3+1.5*IQR
    """
    columns = plan.get_target_column_names()
    method = plan.parameters.get("method", "cap")
    threshold_method = plan.parameters.get("threshold_method", "iqr")
    lower_pct = plan.parameters.get("lower_percentile", 1)
    upper_pct = plan.parameters.get("upper_percentile", 99)

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        series = result[col]

        if threshold_method == "iqr":
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

        elif threshold_method == "zscore":
            mean = series.mean()
            std = series.std()
            lower_bound = mean - 3 * std
            upper_bound = mean + 3 * std

        elif threshold_method == "percentile":
            lower_bound = series.quantile(lower_pct / 100)
            upper_bound = series.quantile(upper_pct / 100)

        else:
            continue

        # Identify outliers
        outlier_mask = (series < lower_bound) | (series > upper_bound)

        if method == "cap":
            result[col] = series.clip(lower=lower_bound, upper=upper_bound)
            code_parts.append(f"df['{col}'] = df['{col}'].clip(lower={lower_bound:.2f}, upper={upper_bound:.2f})")

        elif method == "remove":
            result = result[~outlier_mask]
            code_parts.append(f"df = df[(df['{col}'] >= {lower_bound:.2f}) & (df['{col}'] <= {upper_bound:.2f})]")

        elif method == "flag":
            result[f"{col}_outlier"] = outlier_mask
            code_parts.append(f"df['{col}_outlier'] = (df['{col}'] < {lower_bound:.2f}) | (df['{col}'] > {upper_bound:.2f})")

        elif method == "replace_null":
            result.loc[outlier_mask, col] = np.nan
            code_parts.append(f"df.loc[(df['{col}'] < {lower_bound:.2f}) | (df['{col}'] > {upper_bound:.2f}), '{col}'] = np.nan")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.ABS_VALUE)
def abs_value(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Convert numeric values to absolute values.

    Parameters (from plan.parameters):
        target_columns: Columns to apply absolute value
        new_column: Optional new column name (otherwise modifies in place)

    Example:
        -42 -> 42
        -3.14 -> 3.14
    """
    columns = plan.get_target_column_names()
    new_column = plan.parameters.get("new_column")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        target_col = new_column if new_column else col
        result[target_col] = result[col].abs()
        code_parts.append(f"df['{target_col}'] = df['{col}'].abs()")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.PERCENT_OF_TOTAL)
def percent_of_total(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Calculate each value as percentage of column total.

    Parameters (from plan.parameters):
        target_columns: Columns to calculate percentage for
        partition_by: Optional column(s) to calculate percentage within groups
        suffix: Suffix for new columns (default: "_pct")
        decimal_places: Number of decimal places (default: 2)

    Example:
        values [10, 20, 30] -> [16.67, 33.33, 50.00] (percent of 60)
    """
    columns = plan.get_target_column_names()
    partition_by = plan.parameters.get("partition_by")
    suffix = plan.parameters.get("suffix", "_pct")
    decimal_places = plan.parameters.get("decimal_places", 2)

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        new_col = f"{col}{suffix}"

        if partition_by:
            # Percentage within groups
            group_totals = result.groupby(partition_by)[col].transform('sum')
            result[new_col] = (result[col] / group_totals * 100).round(decimal_places)
            code_parts.append(f"df['{new_col}'] = (df['{col}'] / df.groupby({partition_by})['{col}'].transform('sum') * 100).round({decimal_places})")
        else:
            # Percentage of overall total
            total = result[col].sum()
            result[new_col] = (result[col] / total * 100).round(decimal_places) if total != 0 else 0
            code_parts.append(f"df['{new_col}'] = (df['{col}'] / df['{col}'].sum() * 100).round({decimal_places})")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.BIN_NUMERIC)
def bin_numeric(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Create bins/buckets from numeric values.

    Parameters (from plan.parameters):
        method: "quantile" (equal frequency), "fixed" (equal width), or "custom"
        bins: Number of bins for quantile/fixed, or list of bin edges for custom
        labels: Optional labels for bins
        target_columns: Columns to bin
        suffix: Suffix for new columns (default: "_bin")

    Example (quantile):
        values [1,2,3,4,5,6,7,8,9,10], bins=4
        -> quartiles: "Q1", "Q2", "Q3", "Q4"

    Example (custom):
        values [15, 25, 35, 45], bins=[0, 18, 35, 65, 100]
        -> ["0-18", "18-35", "35-65", "35-65"]
    """
    columns = plan.get_target_column_names()
    method = plan.parameters.get("method", "quantile")
    bins = plan.parameters.get("bins", 4)
    labels = plan.parameters.get("labels")
    suffix = plan.parameters.get("suffix", "_bin")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        new_col = f"{col}{suffix}"

        if method == "quantile":
            result[new_col] = pd.qcut(result[col], q=bins, labels=labels, duplicates='drop')
            code_parts.append(f"df['{new_col}'] = pd.qcut(df['{col}'], q={bins}, labels={labels})")

        elif method == "fixed":
            result[new_col] = pd.cut(result[col], bins=bins, labels=labels)
            code_parts.append(f"df['{new_col}'] = pd.cut(df['{col}'], bins={bins}, labels={labels})")

        elif method == "custom" and isinstance(bins, list):
            result[new_col] = pd.cut(result[col], bins=bins, labels=labels)
            code_parts.append(f"df['{new_col}'] = pd.cut(df['{col}'], bins={bins}, labels={labels})")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.FLOOR_CEILING)
def floor_ceiling(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Apply floor or ceiling to numeric values.

    Parameters (from plan.parameters):
        operation: "floor" or "ceiling" (default: "floor")
        target_columns: Columns to transform

    Example:
        3.7 (floor) -> 3
        3.2 (ceiling) -> 4
    """
    columns = plan.get_target_column_names()
    operation = plan.parameters.get("operation", "floor")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        if operation == "floor":
            result[col] = np.floor(result[col])
            code_parts.append(f"df['{col}'] = np.floor(df['{col}'])")
        elif operation == "ceiling":
            result[col] = np.ceil(result[col])
            code_parts.append(f"df['{col}'] = np.ceil(df['{col}'])")

    code = "\n".join(code_parts)
    return result, code
