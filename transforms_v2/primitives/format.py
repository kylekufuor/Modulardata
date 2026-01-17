# =============================================================================
# transforms_v2/primitives/format.py - Format Operations
# =============================================================================
# Primitives that format values: dates, phones, text casing, whitespace, etc.
# =============================================================================

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from transforms_v2.registry import register_primitive
from transforms_v2.types import (
    CaseType,
    ParamDef,
    Primitive,
    PrimitiveInfo,
    PrimitiveResult,
    TestPrompt,
)


# =============================================================================
# format_date
# =============================================================================


@register_primitive
class FormatDate(Primitive):
    """Format date values to a consistent format."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="format_date",
            category="format",
            description="Standardize dates to a consistent format",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column containing dates to format",
                ),
                ParamDef(
                    name="output_format",
                    type="str",
                    required=False,
                    default="%Y-%m-%d",
                    description="Output date format (strftime format)",
                ),
                ParamDef(
                    name="input_format",
                    type="str",
                    required=False,
                    default=None,
                    description="Input date format if known (for faster parsing)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Standardize all dates in created_date to YYYY-MM-DD format",
                    expected_params={"column": "created_date", "output_format": "%Y-%m-%d"},
                    description="ISO date format",
                ),
                TestPrompt(
                    prompt="Format the date column as MM/DD/YYYY",
                    expected_params={"column": "date", "output_format": "%m/%d/%Y"},
                    description="US date format",
                ),
                TestPrompt(
                    prompt="Convert order_date to display as 'January 15, 2024'",
                    expected_params={"column": "order_date", "output_format": "%B %d, %Y"},
                    description="Long date format",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        output_format = params.get("output_format", "%Y-%m-%d")
        input_format = params.get("input_format")

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()
        warnings = []

        try:
            # Parse dates
            if input_format:
                dates = pd.to_datetime(result_df[column], format=input_format, errors="coerce")
            else:
                # Use format='mixed' to handle diverse date formats (Pandas 2.0+)
                dates = pd.to_datetime(result_df[column], format="mixed", errors="coerce")

            # Count failures
            null_before = df[column].isna().sum()
            null_after = dates.isna().sum()
            failed_conversions = null_after - null_before

            if failed_conversions > 0:
                warnings.append(f"{failed_conversions} values could not be parsed as dates")

            # Format to output
            result_df[column] = dates.dt.strftime(output_format)

            # Handle NaT values (they become 'NaT' string after strftime)
            result_df.loc[dates.isna(), column] = None

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                warnings=warnings,
                metadata={"dates_formatted": int(rows_before - null_after)},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# format_phone
# =============================================================================


@register_primitive
class FormatPhone(Primitive):
    """Format phone numbers to a consistent format."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="format_phone",
            category="format",
            description="Standardize phone numbers to a consistent format",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column containing phone numbers",
                ),
                ParamDef(
                    name="format",
                    type="str",
                    required=False,
                    default="(XXX) XXX-XXXX",
                    description="Output format: '(XXX) XXX-XXXX', 'XXX-XXX-XXXX', 'XXXXXXXXXX', '+1 XXX-XXX-XXXX'",
                    choices=["(XXX) XXX-XXXX", "XXX-XXX-XXXX", "XXXXXXXXXX", "+1 XXX-XXX-XXXX"],
                ),
                ParamDef(
                    name="country_code",
                    type="str",
                    required=False,
                    default="1",
                    description="Default country code for numbers without one",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Format phone numbers as (555) 123-4567",
                    expected_params={"column": "phone", "format": "(XXX) XXX-XXXX"},
                    description="Parentheses format",
                ),
                TestPrompt(
                    prompt="Standardize the phone_number column to XXX-XXX-XXXX format",
                    expected_params={"column": "phone_number", "format": "XXX-XXX-XXXX"},
                    description="Dashes format",
                ),
                TestPrompt(
                    prompt="Clean up phone numbers to just digits",
                    expected_params={"column": "phone", "format": "XXXXXXXXXX"},
                    description="Digits only",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        fmt = params.get("format", "(XXX) XXX-XXXX")
        country_code = params.get("country_code", "1")

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()
        warnings = []
        formatted_count = 0
        failed_count = 0

        def format_phone_number(value):
            nonlocal formatted_count, failed_count

            if pd.isna(value) or str(value).strip() == "":
                return value

            # Extract just digits
            digits = re.sub(r"\D", "", str(value))

            # Handle country code
            if len(digits) == 11 and digits.startswith("1"):
                digits = digits[1:]
            elif len(digits) > 10:
                # Might have longer country code, take last 10
                digits = digits[-10:]

            if len(digits) != 10:
                failed_count += 1
                return value  # Return original if can't parse

            formatted_count += 1

            # Apply format
            if fmt == "(XXX) XXX-XXXX":
                return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
            elif fmt == "XXX-XXX-XXXX":
                return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
            elif fmt == "XXXXXXXXXX":
                return digits
            elif fmt == "+1 XXX-XXX-XXXX":
                return f"+{country_code} {digits[:3]}-{digits[3:6]}-{digits[6:]}"
            else:
                return digits

        result_df[column] = result_df[column].apply(format_phone_number)

        if failed_count > 0:
            warnings.append(f"{failed_count} phone numbers could not be formatted (invalid length)")

        return PrimitiveResult(
            success=True,
            df=result_df,
            rows_before=rows_before,
            rows_after=len(result_df),
            cols_before=cols_before,
            cols_after=len(result_df.columns),
            warnings=warnings,
            metadata={
                "phones_formatted": formatted_count,
                "phones_failed": failed_count,
            },
        )


# =============================================================================
# change_text_casing
# =============================================================================


@register_primitive
class ChangeTextCasing(Primitive):
    """Change text casing in a column."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="change_text_casing",
            category="format",
            description="Change text to uppercase, lowercase, title case, or sentence case",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to change casing",
                ),
                ParamDef(
                    name="case",
                    type="str",
                    required=True,
                    description="Target case: 'lower', 'upper', 'title', 'sentence'",
                    choices=["lower", "upper", "title", "sentence"],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Convert all names to title case",
                    expected_params={"column": "name", "case": "title"},
                    description="Title case names",
                ),
                TestPrompt(
                    prompt="Make the email column all lowercase",
                    expected_params={"column": "email", "case": "lower"},
                    description="Lowercase emails",
                ),
                TestPrompt(
                    prompt="Change status values to uppercase",
                    expected_params={"column": "status", "case": "upper"},
                    description="Uppercase status",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        case = params["case"]

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()

        try:
            if case == "lower":
                result_df[column] = result_df[column].astype(str).str.lower()
            elif case == "upper":
                result_df[column] = result_df[column].astype(str).str.upper()
            elif case == "title":
                result_df[column] = result_df[column].astype(str).str.title()
            elif case == "sentence":
                # Sentence case: capitalize first letter of each sentence
                result_df[column] = result_df[column].astype(str).str.capitalize()

            # Handle 'nan' strings that appear from NaN values
            result_df.loc[df[column].isna(), column] = None

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# trim_whitespace
# =============================================================================


@register_primitive
class TrimWhitespace(Primitive):
    """Remove extra whitespace from text values."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="trim_whitespace",
            category="format",
            description="Remove leading, trailing, and extra whitespace from text",
            params=[
                ParamDef(
                    name="columns",
                    type="list[str] | None",
                    required=False,
                    default=None,
                    description="Columns to trim. None means all string columns.",
                ),
                ParamDef(
                    name="trim_type",
                    type="str",
                    required=False,
                    default="all",
                    description="What to trim: 'leading', 'trailing', 'both', 'all' (includes internal)",
                    choices=["leading", "trailing", "both", "all"],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Remove extra spaces from all text columns",
                    expected_params={"columns": None, "trim_type": "all"},
                    description="Trim all columns",
                ),
                TestPrompt(
                    prompt="Trim whitespace from the name and email columns",
                    expected_params={"columns": ["name", "email"], "trim_type": "both"},
                    description="Trim specific columns",
                ),
                TestPrompt(
                    prompt="Clean up leading and trailing spaces in the description",
                    expected_params={"columns": ["description"], "trim_type": "both"},
                    description="Trim description",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        columns = params.get("columns")
        trim_type = params.get("trim_type", "all")

        rows_before = len(df)
        cols_before = len(df.columns)

        result_df = df.copy()

        # Determine which columns to trim
        if columns is None:
            # All object (string) columns
            cols_to_trim = result_df.select_dtypes(include=["object"]).columns.tolist()
        else:
            # Validate specified columns
            missing = [c for c in columns if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )
            cols_to_trim = columns

        try:
            for col in cols_to_trim:
                if result_df[col].dtype == object:
                    if trim_type == "leading":
                        result_df[col] = result_df[col].str.lstrip()
                    elif trim_type == "trailing":
                        result_df[col] = result_df[col].str.rstrip()
                    elif trim_type == "both":
                        result_df[col] = result_df[col].str.strip()
                    elif trim_type == "all":
                        # Strip leading/trailing and collapse internal whitespace
                        result_df[col] = result_df[col].str.strip()
                        result_df[col] = result_df[col].str.replace(r"\s+", " ", regex=True)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"columns_trimmed": len(cols_to_trim)},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# standardize_values
# =============================================================================


@register_primitive
class StandardizeValues(Primitive):
    """Replace inconsistent values with standard values."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="standardize_values",
            category="format",
            description="Replace multiple variant values with a single standard value",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to standardize",
                ),
                ParamDef(
                    name="mapping",
                    type="dict[str, list[str]]",
                    required=True,
                    description="Mapping of standard value to list of variants: {'Active': ['active', 'ACTIVE', 'A']}",
                ),
                ParamDef(
                    name="case_sensitive",
                    type="bool",
                    required=False,
                    default=False,
                    description="Whether matching should be case-sensitive",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Standardize status values: replace 'Qualifed', 'qualfied', 'QUALIFIED' with 'Qualified'",
                    expected_params={
                        "column": "status",
                        "mapping": {"Qualified": ["Qualifed", "qualfied", "QUALIFIED"]},
                    },
                    description="Fix status typos",
                ),
                TestPrompt(
                    prompt="Normalize campaign_source: 'google ads' and 'Google Ads' should be 'Google Ads'",
                    expected_params={
                        "column": "campaign_source",
                        "mapping": {"Google Ads": ["google ads", "Google ads", "GOOGLE ADS"]},
                    },
                    description="Normalize source names",
                ),
                TestPrompt(
                    prompt="Replace 'Y', 'yes', 'YES' with 'Yes' and 'N', 'no', 'NO' with 'No' in the confirmed column",
                    expected_params={
                        "column": "confirmed",
                        "mapping": {
                            "Yes": ["Y", "yes", "YES"],
                            "No": ["N", "no", "NO"],
                        },
                    },
                    description="Standardize yes/no values",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        mapping = params["mapping"]
        case_sensitive = params.get("case_sensitive", False)

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()
        replacements_made = 0

        try:
            # Build reverse mapping: variant -> standard
            reverse_map = {}
            for standard, variants in mapping.items():
                for variant in variants:
                    if case_sensitive:
                        reverse_map[variant] = standard
                    else:
                        reverse_map[variant.lower()] = standard

            def standardize(value):
                nonlocal replacements_made
                if pd.isna(value):
                    return value
                str_val = str(value)
                lookup = str_val if case_sensitive else str_val.lower()
                if lookup in reverse_map:
                    replacements_made += 1
                    return reverse_map[lookup]
                return value

            result_df[column] = result_df[column].apply(standardize)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"values_standardized": replacements_made},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )
