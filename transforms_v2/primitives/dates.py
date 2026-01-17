# =============================================================================
# transforms_v2/primitives/dates.py - Date/Time Operations
# =============================================================================
# Primitives for date manipulation: extract parts, calculate differences.
# =============================================================================

from __future__ import annotations

from typing import Any

import pandas as pd

from transforms_v2.registry import register_primitive
from transforms_v2.types import (
    ParamDef,
    Primitive,
    PrimitiveInfo,
    PrimitiveResult,
    TestPrompt,
)


# =============================================================================
# extract_date_part
# =============================================================================


@register_primitive
class ExtractDatePart(Primitive):
    """Extract parts from a datetime column (year, month, day, etc.)."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="extract_date_part",
            category="dates",
            description="Extract year, month, day, week, quarter, or other parts from a date column",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column containing dates",
                ),
                ParamDef(
                    name="part",
                    type="str",
                    required=True,
                    description="Part to extract: year, month, day, week, quarter, dayofweek, dayofyear, hour, minute, second",
                    choices=[
                        "year", "month", "day", "week", "quarter",
                        "dayofweek", "dayofyear", "hour", "minute", "second",
                        "weekday_name", "month_name"
                    ],
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for the new column (default: column_part)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Extract the year from the created_date column",
                    expected_params={
                        "column": "created_date",
                        "part": "year",
                    },
                    description="Extract year",
                ),
                TestPrompt(
                    prompt="Get the month from order_date as a new column called order_month",
                    expected_params={
                        "column": "order_date",
                        "part": "month",
                        "new_column": "order_month",
                    },
                    description="Extract month with custom name",
                ),
                TestPrompt(
                    prompt="Extract the day of week from transaction_date",
                    expected_params={
                        "column": "transaction_date",
                        "part": "dayofweek",
                    },
                    description="Extract day of week",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        part = params["part"]
        new_column = params.get("new_column") or f"{column}_{part}"

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.copy()

            # Parse dates if not already datetime
            dates = pd.to_datetime(result_df[column], format="mixed", errors="coerce")

            # Extract the requested part
            if part == "year":
                result_df[new_column] = dates.dt.year
            elif part == "month":
                result_df[new_column] = dates.dt.month
            elif part == "day":
                result_df[new_column] = dates.dt.day
            elif part == "week":
                result_df[new_column] = dates.dt.isocalendar().week
            elif part == "quarter":
                result_df[new_column] = dates.dt.quarter
            elif part == "dayofweek":
                result_df[new_column] = dates.dt.dayofweek  # Monday=0, Sunday=6
            elif part == "dayofyear":
                result_df[new_column] = dates.dt.dayofyear
            elif part == "hour":
                result_df[new_column] = dates.dt.hour
            elif part == "minute":
                result_df[new_column] = dates.dt.minute
            elif part == "second":
                result_df[new_column] = dates.dt.second
            elif part == "weekday_name":
                result_df[new_column] = dates.dt.day_name()
            elif part == "month_name":
                result_df[new_column] = dates.dt.month_name()
            else:
                return PrimitiveResult(
                    success=False,
                    error=f"Unknown date part: {part}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )

            # Count how many dates were successfully parsed
            valid_dates = dates.notna().sum()

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "part_extracted": part,
                    "valid_dates": int(valid_dates),
                    "invalid_dates": int(len(df) - valid_dates),
                },
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# date_diff
# =============================================================================


@register_primitive
class DateDiff(Primitive):
    """Calculate the difference between two date columns."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="date_diff",
            category="dates",
            description="Calculate the difference between two dates in days, months, or years",
            params=[
                ParamDef(
                    name="start_column",
                    type="str",
                    required=True,
                    description="Column containing the start date",
                ),
                ParamDef(
                    name="end_column",
                    type="str",
                    required=True,
                    description="Column containing the end date",
                ),
                ParamDef(
                    name="unit",
                    type="str",
                    required=False,
                    default="days",
                    description="Unit for difference: days, weeks, months, years, hours, minutes, seconds",
                    choices=["days", "weeks", "months", "years", "hours", "minutes", "seconds"],
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for the new column (default: date_diff_{unit})",
                ),
                ParamDef(
                    name="absolute",
                    type="bool",
                    required=False,
                    default=False,
                    description="Return absolute value (always positive)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Calculate days between start_date and end_date",
                    expected_params={
                        "start_column": "start_date",
                        "end_column": "end_date",
                        "unit": "days",
                    },
                    description="Days difference",
                ),
                TestPrompt(
                    prompt="Find the number of months between order_date and ship_date",
                    expected_params={
                        "start_column": "order_date",
                        "end_column": "ship_date",
                        "unit": "months",
                    },
                    description="Months difference",
                ),
                TestPrompt(
                    prompt="Calculate age in years from birth_date to today",
                    expected_params={
                        "start_column": "birth_date",
                        "end_column": "today",
                        "unit": "years",
                    },
                    description="Years difference",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        start_column = params["start_column"]
        end_column = params["end_column"]
        unit = params.get("unit", "days")
        new_column = params.get("new_column") or f"date_diff_{unit}"
        absolute = params.get("absolute", False)

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate columns
        missing = []
        if start_column not in df.columns:
            missing.append(start_column)
        if end_column not in df.columns:
            missing.append(end_column)

        if missing:
            return PrimitiveResult(
                success=False,
                error=f"Columns not found: {missing}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.copy()

            # Parse dates
            start_dates = pd.to_datetime(result_df[start_column], format="mixed", errors="coerce")
            end_dates = pd.to_datetime(result_df[end_column], format="mixed", errors="coerce")

            # Calculate difference
            diff = end_dates - start_dates

            if unit == "days":
                result_df[new_column] = diff.dt.days
            elif unit == "weeks":
                result_df[new_column] = diff.dt.days / 7
            elif unit == "months":
                # Approximate months (30.44 days per month on average)
                result_df[new_column] = diff.dt.days / 30.44
            elif unit == "years":
                # Approximate years (365.25 days per year)
                result_df[new_column] = diff.dt.days / 365.25
            elif unit == "hours":
                result_df[new_column] = diff.dt.total_seconds() / 3600
            elif unit == "minutes":
                result_df[new_column] = diff.dt.total_seconds() / 60
            elif unit == "seconds":
                result_df[new_column] = diff.dt.total_seconds()
            else:
                return PrimitiveResult(
                    success=False,
                    error=f"Unknown unit: {unit}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )

            # Apply absolute if requested
            if absolute:
                result_df[new_column] = result_df[new_column].abs()

            # Round to reasonable precision
            if unit in ("months", "years", "weeks"):
                result_df[new_column] = result_df[new_column].round(2)

            # Count valid calculations
            valid_count = result_df[new_column].notna().sum()

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "unit": unit,
                    "valid_calculations": int(valid_count),
                    "invalid_calculations": int(len(df) - valid_count),
                },
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# date_add
# =============================================================================


@register_primitive
class DateAdd(Primitive):
    """Add a duration to a date column."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="date_add",
            category="dates",
            description="Add days, weeks, months, or years to a date column",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column containing the date",
                ),
                ParamDef(
                    name="amount",
                    type="int",
                    required=True,
                    description="Amount to add (negative to subtract)",
                ),
                ParamDef(
                    name="unit",
                    type="str",
                    required=False,
                    default="days",
                    description="Unit: 'days', 'weeks', 'months', 'years', 'hours', 'minutes'",
                    choices=["days", "weeks", "months", "years", "hours", "minutes"],
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for result column (default: overwrites original)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Add 30 days to the due_date column",
                    expected_params={
                        "column": "due_date",
                        "amount": 30,
                        "unit": "days",
                    },
                    description="Add days",
                ),
                TestPrompt(
                    prompt="Subtract 1 year from the birth_date to get previous_year",
                    expected_params={
                        "column": "birth_date",
                        "amount": -1,
                        "unit": "years",
                        "new_column": "previous_year",
                    },
                    description="Subtract years",
                ),
                TestPrompt(
                    prompt="Add 2 weeks to the start_date",
                    expected_params={
                        "column": "start_date",
                        "amount": 2,
                        "unit": "weeks",
                    },
                    description="Add weeks",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        amount = params["amount"]
        unit = params.get("unit", "days")
        new_column = params.get("new_column") or column

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.copy()

            # Parse dates
            dates = pd.to_datetime(result_df[column], format="mixed", errors="coerce")

            # Calculate offset based on unit
            if unit == "days":
                offset = pd.DateOffset(days=amount)
            elif unit == "weeks":
                offset = pd.DateOffset(weeks=amount)
            elif unit == "months":
                offset = pd.DateOffset(months=amount)
            elif unit == "years":
                offset = pd.DateOffset(years=amount)
            elif unit == "hours":
                offset = pd.DateOffset(hours=amount)
            elif unit == "minutes":
                offset = pd.DateOffset(minutes=amount)
            else:
                return PrimitiveResult(
                    success=False,
                    error=f"Unknown unit: {unit}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )

            # Apply offset
            result_df[new_column] = dates + offset

            # Count valid operations
            valid_count = result_df[new_column].notna().sum()

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "amount_added": amount,
                    "unit": unit,
                    "valid_operations": int(valid_count),
                    "invalid_operations": int(len(df) - valid_count),
                },
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )
