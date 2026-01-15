# =============================================================================
# agents/transformations/date_ops.py - Date Operations
# =============================================================================
# Date and time manipulation operations:
# - date_diff: Calculate difference between dates
# - date_add: Add/subtract time periods
# - extract_date_part: Extract year, month, day, etc.
# - date_to_epoch: Convert to Unix timestamp
# =============================================================================

import pandas as pd
import numpy as np
from typing import Any

from agents.models.technical_plan import TechnicalPlan, TransformationType
from agents.transformations.registry import register


@register(TransformationType.DATE_DIFF)
def date_diff(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Calculate difference between two date columns.

    Parameters (from plan.parameters):
        start_column: The start date column
        end_column: The end date column (or use target_columns[0] and secondary_column)
        unit: "days", "months", "years", "hours", "minutes", "seconds" (default: "days")
        new_column: Name for result column (default: "date_diff")
        absolute: Return absolute value (default: True)

    Example:
        start_date="2024-01-01", end_date="2024-01-15", unit="days" -> 14
    """
    # Get column names
    start_column = plan.parameters.get("start_column")
    end_column = plan.parameters.get("end_column")

    # Also support target_columns with secondary_column
    if not start_column and plan.target_columns:
        start_column = plan.target_columns[0].column_name
        if plan.target_columns[0].secondary_column:
            end_column = plan.target_columns[0].secondary_column

    unit = plan.parameters.get("unit", "days")
    new_column = plan.parameters.get("new_column", "date_diff")
    absolute = plan.parameters.get("absolute", True)

    result = df.copy()

    if not start_column or not end_column:
        return result, "# Need both start_column and end_column for date_diff"

    # Convert to datetime
    start = pd.to_datetime(result[start_column], errors='coerce')
    end = pd.to_datetime(result[end_column], errors='coerce')

    # Calculate difference
    diff = end - start

    if unit == "days":
        result[new_column] = diff.dt.days
    elif unit == "hours":
        result[new_column] = diff.dt.total_seconds() / 3600
    elif unit == "minutes":
        result[new_column] = diff.dt.total_seconds() / 60
    elif unit == "seconds":
        result[new_column] = diff.dt.total_seconds()
    elif unit == "months":
        # Approximate months (30.44 days per month)
        result[new_column] = diff.dt.days / 30.44
    elif unit == "years":
        # Approximate years (365.25 days per year)
        result[new_column] = diff.dt.days / 365.25

    if absolute:
        result[new_column] = result[new_column].abs()

    code = f"df['{new_column}'] = (pd.to_datetime(df['{end_column}']) - pd.to_datetime(df['{start_column}'])).dt.{unit if unit == 'days' else 'total_seconds() / ...'}"
    return result, code


@register(TransformationType.DATE_ADD)
def date_add(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Add or subtract time periods from a date column.

    Parameters (from plan.parameters):
        amount: Number of units to add (negative to subtract)
        unit: "days", "weeks", "months", "years", "hours", "minutes"
        target_columns: Date columns to modify
        new_column: Optional new column name (otherwise modifies in place)

    Example:
        date="2024-01-15", amount=30, unit="days" -> "2024-02-14"
        date="2024-01-15", amount=-1, unit="months" -> "2023-12-15"
    """
    columns = plan.get_target_column_names()
    amount = plan.parameters.get("amount", 0)
    unit = plan.parameters.get("unit", "days")
    new_column = plan.parameters.get("new_column")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        target_col = new_column if new_column else col

        # Convert to datetime
        dt_col = pd.to_datetime(result[col], errors='coerce')

        # Add the offset
        if unit == "days":
            result[target_col] = dt_col + pd.Timedelta(days=amount)
            code_parts.append(f"df['{target_col}'] = pd.to_datetime(df['{col}']) + pd.Timedelta(days={amount})")
        elif unit == "weeks":
            result[target_col] = dt_col + pd.Timedelta(weeks=amount)
            code_parts.append(f"df['{target_col}'] = pd.to_datetime(df['{col}']) + pd.Timedelta(weeks={amount})")
        elif unit == "hours":
            result[target_col] = dt_col + pd.Timedelta(hours=amount)
            code_parts.append(f"df['{target_col}'] = pd.to_datetime(df['{col}']) + pd.Timedelta(hours={amount})")
        elif unit == "minutes":
            result[target_col] = dt_col + pd.Timedelta(minutes=amount)
            code_parts.append(f"df['{target_col}'] = pd.to_datetime(df['{col}']) + pd.Timedelta(minutes={amount})")
        elif unit == "months":
            result[target_col] = dt_col + pd.DateOffset(months=amount)
            code_parts.append(f"df['{target_col}'] = pd.to_datetime(df['{col}']) + pd.DateOffset(months={amount})")
        elif unit == "years":
            result[target_col] = dt_col + pd.DateOffset(years=amount)
            code_parts.append(f"df['{target_col}'] = pd.to_datetime(df['{col}']) + pd.DateOffset(years={amount})")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.EXTRACT_DATE_PART)
def extract_date_part(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Extract parts from datetime columns.

    Parameters (from plan.parameters):
        parts: List of parts to extract. Options:
               "year", "month", "day", "hour", "minute", "second",
               "weekday", "week", "quarter", "dayofyear", "daysinmonth"
        target_columns: Date columns to extract from
        suffix: Suffix pattern for new columns (default: "_{part}")

    Example:
        date="2024-03-15", parts=["year", "month", "weekday"]
        -> Creates columns: date_year=2024, date_month=3, date_weekday=4 (Friday)
    """
    columns = plan.get_target_column_names()
    parts = plan.parameters.get("parts", ["year", "month", "day"])
    suffix_pattern = plan.parameters.get("suffix", "_{part}")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        # Convert to datetime
        dt_col = pd.to_datetime(result[col], errors='coerce')

        for part in parts:
            new_col = f"{col}{suffix_pattern.replace('{part}', part)}"

            if part == "year":
                result[new_col] = dt_col.dt.year
            elif part == "month":
                result[new_col] = dt_col.dt.month
            elif part == "day":
                result[new_col] = dt_col.dt.day
            elif part == "hour":
                result[new_col] = dt_col.dt.hour
            elif part == "minute":
                result[new_col] = dt_col.dt.minute
            elif part == "second":
                result[new_col] = dt_col.dt.second
            elif part == "weekday":
                result[new_col] = dt_col.dt.weekday  # 0=Monday, 6=Sunday
            elif part == "week":
                result[new_col] = dt_col.dt.isocalendar().week
            elif part == "quarter":
                result[new_col] = dt_col.dt.quarter
            elif part == "dayofyear":
                result[new_col] = dt_col.dt.dayofyear
            elif part == "daysinmonth":
                result[new_col] = dt_col.dt.days_in_month
            elif part == "is_weekend":
                result[new_col] = dt_col.dt.weekday >= 5
            elif part == "month_name":
                result[new_col] = dt_col.dt.month_name()
            elif part == "day_name":
                result[new_col] = dt_col.dt.day_name()

            code_parts.append(f"df['{new_col}'] = pd.to_datetime(df['{col}']).dt.{part}")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.DATE_TO_EPOCH)
def date_to_epoch(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Convert datetime to Unix timestamp (seconds since 1970-01-01).

    Parameters (from plan.parameters):
        target_columns: Date columns to convert
        unit: "seconds" (default), "milliseconds", "nanoseconds"
        new_column: Optional new column name (otherwise modifies in place)

    Example:
        date="2024-01-01 00:00:00" -> 1704067200 (seconds)
    """
    columns = plan.get_target_column_names()
    unit = plan.parameters.get("unit", "seconds")
    new_column = plan.parameters.get("new_column")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        target_col = new_column if new_column else col

        # Convert to datetime then to epoch
        dt_col = pd.to_datetime(result[col], errors='coerce')

        if unit == "seconds":
            result[target_col] = (dt_col - pd.Timestamp("1970-01-01")).dt.total_seconds()
        elif unit == "milliseconds":
            result[target_col] = (dt_col - pd.Timestamp("1970-01-01")).dt.total_seconds() * 1000
        elif unit == "nanoseconds":
            result[target_col] = dt_col.astype(np.int64)

        code_parts.append(f"df['{target_col}'] = (pd.to_datetime(df['{col}']) - pd.Timestamp('1970-01-01')).dt.total_seconds()")

    code = "\n".join(code_parts)
    return result, code
