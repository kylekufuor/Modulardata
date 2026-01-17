# =============================================================================
# transforms_v2/primitives/quality.py - Data Quality Operations
# =============================================================================
# Primitives for data quality analysis: detect nulls, profile columns.
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
# detect_nulls
# =============================================================================


@register_primitive
class DetectNulls(Primitive):
    """Analyze null/missing values in the dataset."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="detect_nulls",
            category="quality",
            description="Detect and analyze null/missing values across columns",
            params=[
                ParamDef(
                    name="columns",
                    type="list[str]",
                    required=False,
                    default=None,
                    description="Columns to check (default: all columns)",
                ),
                ParamDef(
                    name="add_null_flag",
                    type="bool",
                    required=False,
                    default=False,
                    description="Add a boolean column flagging rows with any nulls",
                ),
                ParamDef(
                    name="add_null_count",
                    type="bool",
                    required=False,
                    default=False,
                    description="Add a column counting nulls per row",
                ),
                ParamDef(
                    name="threshold",
                    type="float",
                    required=False,
                    default=None,
                    description="Flag columns with null rate above this threshold (0.0 to 1.0)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Check for null values in the email and phone columns",
                    expected_params={
                        "columns": ["email", "phone"],
                    },
                    description="Check specific columns",
                ),
                TestPrompt(
                    prompt="Find all rows that have any missing values",
                    expected_params={
                        "add_null_flag": True,
                    },
                    description="Flag rows with nulls",
                ),
                TestPrompt(
                    prompt="Identify columns with more than 10% missing values",
                    expected_params={
                        "threshold": 0.1,
                    },
                    description="Threshold-based detection",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        columns = params.get("columns")
        add_null_flag = params.get("add_null_flag", False)
        add_null_count = params.get("add_null_count", False)
        threshold = params.get("threshold")

        rows_before = len(df)
        cols_before = len(df.columns)

        # Determine columns to check
        if columns:
            missing = [c for c in columns if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )
            check_cols = columns
        else:
            check_cols = list(df.columns)

        try:
            result_df = df.copy()

            # Calculate null statistics
            null_counts = {}
            null_rates = {}
            for col in check_cols:
                null_count = result_df[col].isna().sum()
                null_counts[col] = int(null_count)
                null_rates[col] = round(null_count / len(result_df), 4) if len(result_df) > 0 else 0

            # Add null flag column if requested
            if add_null_flag:
                result_df["_has_null"] = result_df[check_cols].isna().any(axis=1)

            # Add null count column if requested
            if add_null_count:
                result_df["_null_count"] = result_df[check_cols].isna().sum(axis=1)

            # Identify columns above threshold
            columns_above_threshold = []
            if threshold is not None:
                columns_above_threshold = [
                    col for col, rate in null_rates.items()
                    if rate > threshold
                ]

            # Summary statistics
            total_nulls = sum(null_counts.values())
            total_cells = len(result_df) * len(check_cols)
            overall_null_rate = total_nulls / total_cells if total_cells > 0 else 0

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "null_counts": null_counts,
                    "null_rates": null_rates,
                    "total_nulls": total_nulls,
                    "overall_null_rate": round(overall_null_rate, 4),
                    "columns_above_threshold": columns_above_threshold,
                    "rows_with_any_null": int(result_df[check_cols].isna().any(axis=1).sum()),
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
# profile_column
# =============================================================================


@register_primitive
class ProfileColumn(Primitive):
    """Generate statistics and profile for a column."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="profile_column",
            category="quality",
            description="Generate detailed statistics for a column (min, max, mean, nulls, unique, etc.)",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to profile",
                ),
                ParamDef(
                    name="include_percentiles",
                    type="bool",
                    required=False,
                    default=True,
                    description="Include percentile values (25th, 50th, 75th)",
                ),
                ParamDef(
                    name="include_top_values",
                    type="bool",
                    required=False,
                    default=True,
                    description="Include top N most frequent values",
                ),
                ParamDef(
                    name="top_n",
                    type="int",
                    required=False,
                    default=5,
                    description="Number of top values to include",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Get statistics for the amount column",
                    expected_params={
                        "column": "amount",
                    },
                    description="Basic column profile",
                ),
                TestPrompt(
                    prompt="Profile the status column with top 10 values",
                    expected_params={
                        "column": "status",
                        "top_n": 10,
                    },
                    description="Profile with custom top N",
                ),
                TestPrompt(
                    prompt="Analyze the email column without percentiles",
                    expected_params={
                        "column": "email",
                        "include_percentiles": False,
                    },
                    description="Profile text column",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        include_percentiles = params.get("include_percentiles", True)
        include_top_values = params.get("include_top_values", True)
        top_n = params.get("top_n", 5)

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
            col_data = df[column]

            # Basic statistics
            profile = {
                "column": column,
                "dtype": str(col_data.dtype),
                "count": int(len(col_data)),
                "null_count": int(col_data.isna().sum()),
                "null_rate": round(col_data.isna().sum() / len(col_data), 4) if len(col_data) > 0 else 0,
                "unique_count": int(col_data.nunique()),
                "unique_rate": round(col_data.nunique() / len(col_data), 4) if len(col_data) > 0 else 0,
            }

            # Check if numeric
            is_numeric = pd.api.types.is_numeric_dtype(col_data)

            if is_numeric:
                # Numeric statistics
                numeric_col = pd.to_numeric(col_data, errors="coerce")
                profile["min"] = float(numeric_col.min()) if pd.notna(numeric_col.min()) else None
                profile["max"] = float(numeric_col.max()) if pd.notna(numeric_col.max()) else None
                profile["mean"] = round(float(numeric_col.mean()), 4) if pd.notna(numeric_col.mean()) else None
                profile["std"] = round(float(numeric_col.std()), 4) if pd.notna(numeric_col.std()) else None
                profile["sum"] = float(numeric_col.sum()) if pd.notna(numeric_col.sum()) else None

                if include_percentiles:
                    try:
                        profile["p25"] = float(numeric_col.quantile(0.25))
                        profile["p50"] = float(numeric_col.quantile(0.50))  # median
                        profile["p75"] = float(numeric_col.quantile(0.75))
                    except Exception:
                        profile["p25"] = None
                        profile["p50"] = None
                        profile["p75"] = None
            else:
                # String/text statistics
                non_null = col_data.dropna().astype(str)
                if len(non_null) > 0:
                    profile["min_length"] = int(non_null.str.len().min())
                    profile["max_length"] = int(non_null.str.len().max())
                    profile["avg_length"] = round(float(non_null.str.len().mean()), 2)

            # Top values
            if include_top_values:
                value_counts = col_data.value_counts().head(top_n)
                profile["top_values"] = {
                    str(k): int(v) for k, v in value_counts.items()
                }

            # Check for potential issues
            issues = []
            if profile["null_rate"] > 0.5:
                issues.append("High null rate (>50%)")
            if profile["unique_rate"] == 1.0 and profile["count"] > 1:
                issues.append("All values are unique (potential ID column)")
            if profile["unique_count"] == 1:
                issues.append("Only one unique value (constant column)")

            profile["potential_issues"] = issues

            return PrimitiveResult(
                success=True,
                df=df,  # DataFrame unchanged
                rows_before=rows_before,
                rows_after=len(df),
                cols_before=cols_before,
                cols_after=len(df.columns),
                metadata={"profile": profile},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# validate_pattern
# =============================================================================


@register_primitive
class ValidatePattern(Primitive):
    """Validate that column values match a regex pattern."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="validate_pattern",
            category="quality",
            description="Check if values match a regex pattern (email, phone, custom, etc.)",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to validate",
                ),
                ParamDef(
                    name="pattern",
                    type="str",
                    required=False,
                    default=None,
                    description="Regex pattern to match (use this OR pattern_type)",
                ),
                ParamDef(
                    name="pattern_type",
                    type="str",
                    required=False,
                    default=None,
                    description="Predefined pattern: 'email', 'phone', 'url', 'date', 'zipcode', 'ssn'",
                    choices=["email", "phone", "url", "date", "zipcode", "ssn"],
                ),
                ParamDef(
                    name="add_valid_flag",
                    type="bool",
                    required=False,
                    default=True,
                    description="Add a boolean column flagging valid values",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for the validation flag column (default: column_valid)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Validate email addresses in the email column",
                    expected_params={
                        "column": "email",
                        "pattern_type": "email",
                    },
                    description="Validate emails",
                ),
                TestPrompt(
                    prompt="Check if phone numbers match the format (XXX) XXX-XXXX",
                    expected_params={
                        "column": "phone",
                        "pattern": r"\(\d{3}\)\s?\d{3}-\d{4}",
                    },
                    description="Custom phone pattern",
                ),
                TestPrompt(
                    prompt="Validate that the url column contains valid URLs",
                    expected_params={
                        "column": "url",
                        "pattern_type": "url",
                    },
                    description="Validate URLs",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    # Predefined patterns
    PATTERNS = {
        "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        "phone": r"^[\d\s\(\)\-\+\.]{7,20}$",
        "url": r"^https?://[^\s/$.?#].[^\s]*$",
        "date": r"^\d{1,4}[-/]\d{1,2}[-/]\d{1,4}$",
        "zipcode": r"^\d{5}(-\d{4})?$",
        "ssn": r"^\d{3}-\d{2}-\d{4}$",
    }

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        pattern = params.get("pattern")
        pattern_type = params.get("pattern_type")
        add_valid_flag = params.get("add_valid_flag", True)
        new_column = params.get("new_column") or f"{column}_valid"

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Determine which pattern to use
        if pattern is None and pattern_type is None:
            return PrimitiveResult(
                success=False,
                error="Must specify either 'pattern' or 'pattern_type'",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if pattern is None:
            pattern = self.PATTERNS.get(pattern_type)
            if pattern is None:
                return PrimitiveResult(
                    success=False,
                    error=f"Unknown pattern_type: {pattern_type}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )

        try:
            import re
            result_df = df.copy()

            # Compile regex
            regex = re.compile(pattern)

            # Apply validation (handle nulls)
            col_str = result_df[column].fillna("").astype(str)
            is_valid = col_str.str.match(pattern, na=False)

            # Nulls should be marked as invalid
            is_valid = is_valid & result_df[column].notna()

            if add_valid_flag:
                result_df[new_column] = is_valid

            # Count stats
            valid_count = int(is_valid.sum())
            invalid_count = rows_before - valid_count

            # Get sample invalid values for debugging
            invalid_samples = result_df.loc[~is_valid, column].dropna().head(5).tolist()

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "valid_count": valid_count,
                    "invalid_count": invalid_count,
                    "valid_rate": round(valid_count / rows_before, 4) if rows_before > 0 else 0,
                    "pattern_used": pattern,
                    "invalid_samples": invalid_samples,
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
# is_duplicate
# =============================================================================


@register_primitive
class IsDuplicate(Primitive):
    """Flag duplicate rows with a boolean column."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="is_duplicate",
            category="quality",
            description="Add a boolean column indicating if a row is a duplicate",
            params=[
                ParamDef(
                    name="subset",
                    type="list[str]",
                    required=False,
                    default=None,
                    description="Columns to check for duplicates (default: all columns)",
                ),
                ParamDef(
                    name="keep",
                    type="str",
                    required=False,
                    default="first",
                    description="Which occurrence to mark as NOT duplicate: 'first', 'last', or 'none' (mark all as duplicates)",
                    choices=["first", "last", "none"],
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default="_is_duplicate",
                    description="Name for the duplicate flag column",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Flag duplicate rows based on email",
                    expected_params={
                        "subset": ["email"],
                    },
                    description="Flag email duplicates",
                ),
                TestPrompt(
                    prompt="Mark all duplicate entries (don't keep any as original)",
                    expected_params={
                        "keep": "none",
                    },
                    description="Mark all duplicates",
                ),
                TestPrompt(
                    prompt="Identify duplicate orders by customer_id and product_id",
                    expected_params={
                        "subset": ["customer_id", "product_id"],
                    },
                    description="Multi-column duplicate check",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        subset = params.get("subset")
        keep = params.get("keep", "first")
        new_column = params.get("new_column", "_is_duplicate")

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate subset columns
        if subset:
            missing = [c for c in subset if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )

        try:
            result_df = df.copy()

            # Handle 'none' -> False for pandas duplicated()
            keep_arg = False if keep == "none" else keep

            # Mark duplicates
            result_df[new_column] = result_df.duplicated(subset=subset, keep=keep_arg)

            # Count duplicates
            duplicate_count = int(result_df[new_column].sum())

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "duplicate_count": duplicate_count,
                    "unique_count": rows_before - duplicate_count,
                    "duplicate_rate": round(duplicate_count / rows_before, 4) if rows_before > 0 else 0,
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
# detect_header
# =============================================================================


@register_primitive
class DetectHeader(Primitive):
    """Detect and set the header row in data with junk/metadata rows."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="detect_header",
            category="quality",
            description="Find the header row in data that has junk/metadata rows before it",
            params=[
                ParamDef(
                    name="max_rows_to_check",
                    type="int",
                    required=False,
                    default=20,
                    description="Maximum rows to analyze for header detection",
                ),
                ParamDef(
                    name="expected_columns",
                    type="list[str]",
                    required=False,
                    default=None,
                    description="Expected column names to help identify header row",
                ),
                ParamDef(
                    name="min_string_columns",
                    type="int",
                    required=False,
                    default=2,
                    description="Minimum number of string-like values to consider as header",
                ),
                ParamDef(
                    name="apply_header",
                    type="bool",
                    required=False,
                    default=True,
                    description="If True, set the detected row as header and remove junk rows",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Find the header row in this messy data",
                    expected_params={},
                    description="Auto-detect header",
                ),
                TestPrompt(
                    prompt="Detect header row, expecting columns: name, email, phone",
                    expected_params={
                        "expected_columns": ["name", "email", "phone"],
                    },
                    description="Detect with expected columns",
                ),
                TestPrompt(
                    prompt="Find where the actual data headers start",
                    expected_params={
                        "apply_header": True,
                    },
                    description="Find and apply header",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        max_rows = params.get("max_rows_to_check", 20)
        expected_columns = params.get("expected_columns")
        min_string_cols = params.get("min_string_columns", 2)
        apply_header = params.get("apply_header", True)

        rows_before = len(df)
        cols_before = len(df.columns)

        try:
            detected_row = None
            confidence = 0.0
            detection_reason = ""

            # Check rows for header characteristics
            rows_to_check = min(max_rows, len(df))

            for i in range(rows_to_check):
                row = df.iloc[i]
                row_values = [str(v).strip() for v in row.values if pd.notna(v)]

                # Score this row as potential header
                score = 0
                reasons = []

                # Check 1: If expected columns provided, look for matches
                if expected_columns:
                    matches = sum(1 for exp in expected_columns
                                  if any(exp.lower() in str(v).lower() for v in row_values))
                    if matches > 0:
                        score += matches * 30
                        reasons.append(f"matched {matches} expected columns")

                # Check 2: Count string-like values (not pure numbers)
                string_count = sum(1 for v in row_values
                                   if v and not v.replace('.', '').replace('-', '').isdigit())
                if string_count >= min_string_cols:
                    score += string_count * 10
                    reasons.append(f"{string_count} string values")

                # Check 3: No null values in row
                non_null_count = row.notna().sum()
                if non_null_count == len(row):
                    score += 20
                    reasons.append("no nulls")

                # Check 4: Values look like column names (short, no special chars)
                name_like = sum(1 for v in row_values
                               if v and len(v) < 50 and not any(c in v for c in '\n\t'))
                if name_like >= len(row_values) * 0.7:
                    score += 15
                    reasons.append("values look like column names")

                # Check 5: Row after this has different data types (indicates data vs header)
                if i < len(df) - 1:
                    next_row = df.iloc[i + 1]
                    type_diff = sum(1 for v1, v2 in zip(row.values, next_row.values)
                                   if type(v1) != type(v2) or
                                   (isinstance(v1, str) and isinstance(v2, (int, float))))
                    if type_diff > 0:
                        score += type_diff * 5
                        reasons.append(f"type change in next row")

                if score > confidence:
                    confidence = score
                    detected_row = i
                    detection_reason = "; ".join(reasons)

            result_df = df.copy()

            if detected_row is not None and apply_header:
                # Set detected row as header
                new_columns = [str(v).strip() if pd.notna(v) else f"col_{j}"
                              for j, v in enumerate(df.iloc[detected_row].values)]

                # Remove rows up to and including the header row
                result_df = df.iloc[detected_row + 1:].reset_index(drop=True)
                result_df.columns = new_columns

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "detected_header_row": detected_row,
                    "confidence_score": confidence,
                    "detection_reason": detection_reason,
                    "rows_removed": detected_row + 1 if detected_row is not None and apply_header else 0,
                    "new_columns": list(result_df.columns) if apply_header else None,
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
# validate_schema
# =============================================================================


@register_primitive
class ValidateSchema(Primitive):
    """Validate that data matches expected schema."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="validate_schema",
            category="quality",
            description="Check if columns match expected names and types",
            params=[
                ParamDef(
                    name="expected_columns",
                    type="list[str]",
                    required=False,
                    default=None,
                    description="List of expected column names",
                ),
                ParamDef(
                    name="expected_types",
                    type="dict[str, str]",
                    required=False,
                    default=None,
                    description="Expected types per column: {col: 'string'|'number'|'date'|'boolean'}",
                ),
                ParamDef(
                    name="allow_extra_columns",
                    type="bool",
                    required=False,
                    default=True,
                    description="Allow columns not in expected list",
                ),
                ParamDef(
                    name="case_sensitive",
                    type="bool",
                    required=False,
                    default=False,
                    description="Case-sensitive column name matching",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Validate that columns include name, email, and phone",
                    expected_params={
                        "expected_columns": ["name", "email", "phone"],
                    },
                    description="Validate column names",
                ),
                TestPrompt(
                    prompt="Check schema: amount should be number, status should be string",
                    expected_params={
                        "expected_types": {"amount": "number", "status": "string"},
                    },
                    description="Validate column types",
                ),
                TestPrompt(
                    prompt="Ensure data has exactly these columns: id, name, value",
                    expected_params={
                        "expected_columns": ["id", "name", "value"],
                        "allow_extra_columns": False,
                    },
                    description="Strict schema validation",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        expected_columns = params.get("expected_columns")
        expected_types = params.get("expected_types", {})
        allow_extra = params.get("allow_extra_columns", True)
        case_sensitive = params.get("case_sensitive", False)

        rows_before = len(df)
        cols_before = len(df.columns)

        try:
            validation_results = {
                "is_valid": True,
                "errors": [],
                "warnings": [],
                "column_status": {},
            }

            actual_columns = list(df.columns)

            # Normalize for comparison if not case sensitive
            if not case_sensitive:
                actual_lower = {c.lower(): c for c in actual_columns}

            # Check expected columns
            if expected_columns:
                for exp_col in expected_columns:
                    check_col = exp_col if case_sensitive else exp_col.lower()

                    if case_sensitive:
                        found = exp_col in actual_columns
                    else:
                        found = check_col in actual_lower

                    if found:
                        validation_results["column_status"][exp_col] = "present"
                    else:
                        validation_results["column_status"][exp_col] = "missing"
                        validation_results["errors"].append(f"Missing column: {exp_col}")
                        validation_results["is_valid"] = False

                # Check for extra columns
                if not allow_extra:
                    expected_set = set(c.lower() for c in expected_columns) if not case_sensitive else set(expected_columns)
                    actual_set = set(c.lower() for c in actual_columns) if not case_sensitive else set(actual_columns)
                    extra = actual_set - expected_set
                    if extra:
                        validation_results["warnings"].append(f"Extra columns: {list(extra)}")
                        validation_results["is_valid"] = False

            # Check expected types
            if expected_types:
                for col, expected_type in expected_types.items():
                    # Find actual column (case insensitive if needed)
                    actual_col = col
                    if not case_sensitive:
                        actual_col = actual_lower.get(col.lower())

                    if actual_col is None or actual_col not in df.columns:
                        continue  # Already reported as missing

                    actual_dtype = df[actual_col].dtype
                    type_ok = False

                    if expected_type == "string":
                        type_ok = actual_dtype == object or str(actual_dtype) == "string"
                    elif expected_type == "number":
                        type_ok = pd.api.types.is_numeric_dtype(actual_dtype)
                    elif expected_type == "date":
                        type_ok = pd.api.types.is_datetime64_any_dtype(actual_dtype)
                    elif expected_type == "boolean":
                        type_ok = actual_dtype == bool or str(actual_dtype) == "boolean"

                    if not type_ok:
                        validation_results["errors"].append(
                            f"Column '{col}' expected {expected_type}, got {actual_dtype}"
                        )
                        validation_results["is_valid"] = False

            return PrimitiveResult(
                success=True,
                df=df,  # DataFrame unchanged
                rows_before=rows_before,
                rows_after=len(df),
                cols_before=cols_before,
                cols_after=len(df.columns),
                metadata=validation_results,
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# infer_types
# =============================================================================


@register_primitive
class InferTypes(Primitive):
    """Automatically detect and optionally convert column data types."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="infer_types",
            category="quality",
            description="Analyze and optionally convert columns to their detected types",
            params=[
                ParamDef(
                    name="columns",
                    type="list[str]",
                    required=False,
                    default=None,
                    description="Columns to analyze (default: all)",
                ),
                ParamDef(
                    name="apply_conversion",
                    type="bool",
                    required=False,
                    default=False,
                    description="If True, convert columns to detected types",
                ),
                ParamDef(
                    name="sample_size",
                    type="int",
                    required=False,
                    default=1000,
                    description="Number of rows to sample for type detection",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Detect the data types of all columns",
                    expected_params={},
                    description="Infer all types",
                ),
                TestPrompt(
                    prompt="Auto-detect and convert column types",
                    expected_params={
                        "apply_conversion": True,
                    },
                    description="Infer and convert",
                ),
                TestPrompt(
                    prompt="Analyze types for the amount and date columns",
                    expected_params={
                        "columns": ["amount", "date"],
                    },
                    description="Infer specific columns",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        columns = params.get("columns") or list(df.columns)
        apply_conversion = params.get("apply_conversion", False)
        sample_size = params.get("sample_size", 1000)

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate columns
        missing = [c for c in columns if c not in df.columns]
        if missing:
            return PrimitiveResult(
                success=False,
                error=f"Columns not found: {missing}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.copy()
            type_info = {}

            # Sample data for analysis
            sample_df = df.head(sample_size) if len(df) > sample_size else df

            for col in columns:
                col_data = sample_df[col].dropna()

                if len(col_data) == 0:
                    type_info[col] = {
                        "current_type": str(df[col].dtype),
                        "inferred_type": "unknown",
                        "confidence": 0,
                        "reason": "all null values",
                    }
                    continue

                inferred = self._infer_column_type(col_data)
                type_info[col] = inferred

                # Apply conversion if requested
                if apply_conversion and inferred["inferred_type"] != "unknown":
                    result_df = self._convert_column(result_df, col, inferred["inferred_type"])

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "type_info": type_info,
                    "conversions_applied": apply_conversion,
                },
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )

    def _infer_column_type(self, col_data: pd.Series) -> dict:
        """Infer the best type for a column."""
        current_type = str(col_data.dtype)

        # Already numeric
        if pd.api.types.is_numeric_dtype(col_data):
            if pd.api.types.is_integer_dtype(col_data):
                return {"current_type": current_type, "inferred_type": "integer",
                        "confidence": 1.0, "reason": "already integer"}
            return {"current_type": current_type, "inferred_type": "float",
                    "confidence": 1.0, "reason": "already numeric"}

        # Already datetime
        if pd.api.types.is_datetime64_any_dtype(col_data):
            return {"current_type": current_type, "inferred_type": "datetime",
                    "confidence": 1.0, "reason": "already datetime"}

        # Try to detect type from string values
        str_values = col_data.astype(str)
        sample = str_values.head(100)

        # Check for boolean
        bool_values = {"true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"}
        if all(v.lower().strip() in bool_values for v in sample if v.strip()):
            return {"current_type": current_type, "inferred_type": "boolean",
                    "confidence": 0.95, "reason": "values match boolean patterns"}

        # Check for integer
        try:
            pd.to_numeric(str_values, errors="raise")
            if all('.' not in str(v) for v in sample if pd.notna(v)):
                return {"current_type": current_type, "inferred_type": "integer",
                        "confidence": 0.9, "reason": "all values are integers"}
            return {"current_type": current_type, "inferred_type": "float",
                    "confidence": 0.9, "reason": "all values are numeric"}
        except (ValueError, TypeError):
            pass

        # Check for datetime
        try:
            parsed = pd.to_datetime(str_values, errors="coerce", format="mixed")
            valid_ratio = parsed.notna().sum() / len(parsed)
            if valid_ratio > 0.8:
                return {"current_type": current_type, "inferred_type": "datetime",
                        "confidence": valid_ratio, "reason": f"{valid_ratio:.0%} parsed as dates"}
        except Exception:
            pass

        # Default to string
        return {"current_type": current_type, "inferred_type": "string",
                "confidence": 0.8, "reason": "defaulting to string"}

    def _convert_column(self, df: pd.DataFrame, col: str, target_type: str) -> pd.DataFrame:
        """Convert a column to the target type."""
        if target_type == "integer":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif target_type == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif target_type == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed")
        elif target_type == "boolean":
            true_vals = {"true", "yes", "1", "t", "y"}
            df[col] = df[col].astype(str).str.lower().str.strip().isin(true_vals)
        # string type: no conversion needed
        return df


# =============================================================================
# compare_schemas
# =============================================================================


@register_primitive
class CompareSchemas(Primitive):
    """Compare expected schema vs observed schema and return detailed diff."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="compare_schemas",
            category="quality",
            description="Compare expected schema against observed data and produce a detailed diff",
            params=[
                ParamDef(
                    name="expected_columns",
                    type="list[str]",
                    required=True,
                    description="List of expected column names",
                ),
                ParamDef(
                    name="expected_types",
                    type="dict[str, str]",
                    required=False,
                    default=None,
                    description="Expected types: {col: 'string'|'integer'|'float'|'datetime'|'boolean'}",
                ),
                ParamDef(
                    name="expected_nullable",
                    type="dict[str, bool]",
                    required=False,
                    default=None,
                    description="Expected nullability: {col: True/False}",
                ),
                ParamDef(
                    name="case_sensitive",
                    type="bool",
                    required=False,
                    default=False,
                    description="Case-sensitive column name comparison",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Compare data against expected schema with columns: id, name, email, amount",
                    expected_params={
                        "expected_columns": ["id", "name", "email", "amount"],
                    },
                    description="Basic schema comparison",
                ),
                TestPrompt(
                    prompt="Check if schema matches: id (integer), name (string), amount (float)",
                    expected_params={
                        "expected_columns": ["id", "name", "amount"],
                        "expected_types": {"id": "integer", "name": "string", "amount": "float"},
                    },
                    description="Schema comparison with types",
                ),
                TestPrompt(
                    prompt="Compare schema and check that email cannot be null",
                    expected_params={
                        "expected_columns": ["email"],
                        "expected_nullable": {"email": False},
                    },
                    description="Schema comparison with nullability",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        expected_columns = params["expected_columns"]
        expected_types = params.get("expected_types", {})
        expected_nullable = params.get("expected_nullable", {})
        case_sensitive = params.get("case_sensitive", False)

        rows_before = len(df)
        cols_before = len(df.columns)

        try:
            actual_columns = list(df.columns)

            # Build column lookup
            if case_sensitive:
                actual_lookup = {c: c for c in actual_columns}
            else:
                actual_lookup = {c.lower(): c for c in actual_columns}
                expected_lower = {c.lower(): c for c in expected_columns}

            # Schema diff results
            diff = {
                "missing_columns": [],
                "extra_columns": [],
                "type_mismatches": [],
                "nullability_violations": [],
                "column_mappings": {},  # expected -> actual
            }

            # Check for missing columns
            for exp_col in expected_columns:
                lookup_key = exp_col if case_sensitive else exp_col.lower()
                if lookup_key in actual_lookup:
                    diff["column_mappings"][exp_col] = actual_lookup[lookup_key]
                else:
                    diff["missing_columns"].append(exp_col)

            # Check for extra columns
            expected_set = set(expected_columns) if case_sensitive else set(c.lower() for c in expected_columns)
            actual_set = set(actual_columns) if case_sensitive else set(c.lower() for c in actual_columns)
            diff["extra_columns"] = list(actual_set - expected_set)

            # Check type mismatches
            for exp_col, exp_type in expected_types.items():
                lookup_key = exp_col if case_sensitive else exp_col.lower()
                if lookup_key not in actual_lookup:
                    continue  # Already reported as missing

                actual_col = actual_lookup[lookup_key]
                actual_dtype = df[actual_col].dtype

                # Determine observed type
                observed_type = self._classify_dtype(actual_dtype, df[actual_col])

                if observed_type != exp_type:
                    diff["type_mismatches"].append({
                        "column": exp_col,
                        "expected_type": exp_type,
                        "observed_type": observed_type,
                        "actual_dtype": str(actual_dtype),
                    })

            # Check nullability
            for exp_col, should_be_nullable in expected_nullable.items():
                lookup_key = exp_col if case_sensitive else exp_col.lower()
                if lookup_key not in actual_lookup:
                    continue

                actual_col = actual_lookup[lookup_key]
                has_nulls = df[actual_col].isna().any()

                if not should_be_nullable and has_nulls:
                    null_count = int(df[actual_col].isna().sum())
                    diff["nullability_violations"].append({
                        "column": exp_col,
                        "expected_nullable": should_be_nullable,
                        "has_nulls": True,
                        "null_count": null_count,
                    })

            # Summary
            has_drift = bool(
                diff["missing_columns"] or
                diff["extra_columns"] or
                diff["type_mismatches"] or
                diff["nullability_violations"]
            )

            diff["has_drift"] = has_drift
            diff["drift_count"] = (
                len(diff["missing_columns"]) +
                len(diff["extra_columns"]) +
                len(diff["type_mismatches"]) +
                len(diff["nullability_violations"])
            )

            return PrimitiveResult(
                success=True,
                df=df,
                rows_before=rows_before,
                rows_after=len(df),
                cols_before=cols_before,
                cols_after=len(df.columns),
                metadata=diff,
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )

    def _classify_dtype(self, dtype, col_data: pd.Series) -> str:
        """Classify a pandas dtype into our type categories."""
        if pd.api.types.is_integer_dtype(dtype):
            return "integer"
        elif pd.api.types.is_float_dtype(dtype):
            return "float"
        elif pd.api.types.is_bool_dtype(dtype):
            return "boolean"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "datetime"
        else:
            return "string"


# =============================================================================
# detect_renamed_columns
# =============================================================================


@register_primitive
class DetectRenamedColumns(Primitive):
    """Detect potential column renames using fuzzy matching."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="detect_renamed_columns",
            category="quality",
            description="Suggest potential column renames by comparing expected vs observed using fuzzy matching",
            params=[
                ParamDef(
                    name="expected_columns",
                    type="list[str]",
                    required=True,
                    description="List of expected column names",
                ),
                ParamDef(
                    name="similarity_threshold",
                    type="float",
                    required=False,
                    default=0.6,
                    description="Minimum similarity score (0-1) to suggest a rename",
                ),
                ParamDef(
                    name="check_type_compatibility",
                    type="bool",
                    required=False,
                    default=True,
                    description="Also check if data types are compatible",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Find renamed columns, expected: customer_name, phone_number, email_address",
                    expected_params={
                        "expected_columns": ["customer_name", "phone_number", "email_address"],
                    },
                    description="Detect renames",
                ),
                TestPrompt(
                    prompt="Suggest column renames with 70% similarity threshold",
                    expected_params={
                        "expected_columns": ["name", "amount", "date"],
                        "similarity_threshold": 0.7,
                    },
                    description="Rename detection with threshold",
                ),
                TestPrompt(
                    prompt="Find possible renamed columns ignoring type compatibility",
                    expected_params={
                        "expected_columns": ["id", "value"],
                        "check_type_compatibility": False,
                    },
                    description="Name-only rename detection",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        expected_columns = params["expected_columns"]
        threshold = params.get("similarity_threshold", 0.6)
        check_types = params.get("check_type_compatibility", True)

        rows_before = len(df)
        cols_before = len(df.columns)

        try:
            actual_columns = list(df.columns)

            # Find missing expected columns
            missing_expected = [c for c in expected_columns if c.lower() not in [a.lower() for a in actual_columns]]

            # Find extra actual columns
            extra_actual = [c for c in actual_columns if c.lower() not in [e.lower() for e in expected_columns]]

            # Find potential renames
            rename_suggestions = []

            for exp_col in missing_expected:
                best_match = None
                best_score = 0

                for act_col in extra_actual:
                    # Calculate similarity
                    score = self._similarity(exp_col.lower(), act_col.lower())

                    if score > best_score and score >= threshold:
                        best_score = score
                        best_match = act_col

                if best_match:
                    suggestion = {
                        "expected": exp_col,
                        "observed": best_match,
                        "name_similarity": round(best_score, 3),
                        "confidence": "high" if best_score > 0.8 else "medium" if best_score > 0.6 else "low",
                    }

                    # Check type compatibility if requested
                    if check_types:
                        obs_dtype = str(df[best_match].dtype)
                        suggestion["observed_dtype"] = obs_dtype

                    rename_suggestions.append(suggestion)

            return PrimitiveResult(
                success=True,
                df=df,
                rows_before=rows_before,
                rows_after=len(df),
                cols_before=cols_before,
                cols_after=len(df.columns),
                metadata={
                    "missing_columns": missing_expected,
                    "extra_columns": extra_actual,
                    "rename_suggestions": rename_suggestions,
                    "suggestion_count": len(rename_suggestions),
                },
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )

    def _similarity(self, s1: str, s2: str) -> float:
        """Calculate similarity between two strings using Levenshtein-like ratio."""
        if s1 == s2:
            return 1.0
        if not s1 or not s2:
            return 0.0

        # Simple character-based similarity
        len1, len2 = len(s1), len(s2)
        max_len = max(len1, len2)

        # Count matching characters
        matches = sum(1 for c1, c2 in zip(s1, s2) if c1 == c2)

        # Check for common substrings
        common_prefix = 0
        for c1, c2 in zip(s1, s2):
            if c1 == c2:
                common_prefix += 1
            else:
                break

        # Check if one contains the other
        if s1 in s2 or s2 in s1:
            return min(len1, len2) / max_len + 0.2

        # Basic ratio
        base_score = matches / max_len

        # Bonus for common prefix
        prefix_bonus = common_prefix / max_len * 0.3

        # Check for word overlap (underscore/camelCase split)
        import re
        words1 = set(re.split(r'[_\s]|(?<=[a-z])(?=[A-Z])', s1.lower()))
        words2 = set(re.split(r'[_\s]|(?<=[a-z])(?=[A-Z])', s2.lower()))
        word_overlap = len(words1 & words2) / max(len(words1), len(words2)) if words1 and words2 else 0

        return min(1.0, base_score + prefix_bonus + word_overlap * 0.3)


# =============================================================================
# detect_enum_drift
# =============================================================================


@register_primitive
class DetectEnumDrift(Primitive):
    """Detect new or missing categorical values compared to expected."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="detect_enum_drift",
            category="quality",
            description="Find new or missing categorical values in enumerated columns",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to check for enum drift",
                ),
                ParamDef(
                    name="expected_values",
                    type="list",
                    required=True,
                    description="List of expected/allowed values",
                ),
                ParamDef(
                    name="case_sensitive",
                    type="bool",
                    required=False,
                    default=False,
                    description="Case-sensitive value comparison",
                ),
                ParamDef(
                    name="add_drift_flag",
                    type="bool",
                    required=False,
                    default=False,
                    description="Add a column flagging rows with unexpected values",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Check if status column has only: active, inactive, pending",
                    expected_params={
                        "column": "status",
                        "expected_values": ["active", "inactive", "pending"],
                    },
                    description="Check enum values",
                ),
                TestPrompt(
                    prompt="Detect new country codes not in expected list",
                    expected_params={
                        "column": "country_code",
                        "expected_values": ["US", "CA", "UK", "DE", "FR"],
                    },
                    description="Detect new enum values",
                ),
                TestPrompt(
                    prompt="Find and flag rows with unexpected category values",
                    expected_params={
                        "column": "category",
                        "expected_values": ["A", "B", "C"],
                        "add_drift_flag": True,
                    },
                    description="Flag enum drift",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        expected_values = params["expected_values"]
        case_sensitive = params.get("case_sensitive", False)
        add_flag = params.get("add_drift_flag", False)

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

            # Get observed values
            observed_values = df[column].dropna().unique().tolist()

            # Normalize for comparison if not case sensitive
            if case_sensitive:
                expected_set = set(str(v) for v in expected_values)
                observed_set = set(str(v) for v in observed_values)
            else:
                expected_set = set(str(v).lower() for v in expected_values)
                observed_set = set(str(v).lower() for v in observed_values)
                # Keep original values for reporting
                observed_original = {str(v).lower(): str(v) for v in observed_values}

            # Find drift
            new_values = observed_set - expected_set
            missing_values = expected_set - observed_set

            # Get original case for new values
            if not case_sensitive:
                new_values_original = [observed_original.get(v, v) for v in new_values]
            else:
                new_values_original = list(new_values)

            # Count occurrences of new values
            new_value_counts = {}
            for v in new_values_original:
                if case_sensitive:
                    count = int((df[column].astype(str) == v).sum())
                else:
                    count = int((df[column].astype(str).str.lower() == v.lower()).sum())
                new_value_counts[v] = count

            # Add drift flag if requested
            if add_flag:
                if case_sensitive:
                    result_df[f"{column}_enum_drift"] = ~df[column].astype(str).isin(expected_set)
                else:
                    result_df[f"{column}_enum_drift"] = ~df[column].astype(str).str.lower().isin(expected_set)

            has_drift = bool(new_values or missing_values)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "has_drift": has_drift,
                    "new_values": new_values_original,
                    "new_value_counts": new_value_counts,
                    "missing_values": list(missing_values),
                    "expected_count": len(expected_values),
                    "observed_count": len(observed_values),
                    "rows_with_new_values": sum(new_value_counts.values()),
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
# detect_format_drift
# =============================================================================


@register_primitive
class DetectFormatDrift(Primitive):
    """Detect format changes in string columns (dates, phones, IDs, etc.)."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="detect_format_drift",
            category="quality",
            description="Detect format changes in columns by comparing against expected patterns",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to check for format drift",
                ),
                ParamDef(
                    name="expected_format",
                    type="str",
                    required=False,
                    default=None,
                    description="Expected regex pattern for values",
                ),
                ParamDef(
                    name="format_type",
                    type="str",
                    required=False,
                    default=None,
                    description="Predefined format: 'date_iso', 'date_us', 'date_eu', 'phone_us', 'phone_intl', 'uuid', 'email'",
                    choices=["date_iso", "date_us", "date_eu", "phone_us", "phone_intl", "uuid", "email"],
                ),
                ParamDef(
                    name="sample_size",
                    type="int",
                    required=False,
                    default=1000,
                    description="Number of rows to sample for format analysis",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Check if dates are in ISO format (YYYY-MM-DD)",
                    expected_params={
                        "column": "date",
                        "format_type": "date_iso",
                    },
                    description="Check ISO date format",
                ),
                TestPrompt(
                    prompt="Detect format drift in phone numbers against US format",
                    expected_params={
                        "column": "phone",
                        "format_type": "phone_us",
                    },
                    description="Check phone format",
                ),
                TestPrompt(
                    prompt="Check if IDs match pattern ABC-12345",
                    expected_params={
                        "column": "product_id",
                        "expected_format": r"^[A-Z]{3}-\d{5}$",
                    },
                    description="Custom format check",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    FORMATS = {
        "date_iso": r"^\d{4}-\d{2}-\d{2}$",
        "date_us": r"^\d{1,2}/\d{1,2}/\d{2,4}$",
        "date_eu": r"^\d{1,2}\.\d{1,2}\.\d{2,4}$",
        "phone_us": r"^(\+1)?[\s\.-]?\(?\d{3}\)?[\s\.-]?\d{3}[\s\.-]?\d{4}$",
        "phone_intl": r"^\+\d{1,3}[\s\.-]?\d{4,14}$",
        "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    }

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        expected_format = params.get("expected_format")
        format_type = params.get("format_type")
        sample_size = params.get("sample_size", 1000)

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if not expected_format and not format_type:
            return PrimitiveResult(
                success=False,
                error="Must specify either 'expected_format' or 'format_type'",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Get pattern
        pattern = expected_format or self.FORMATS.get(format_type)
        if not pattern:
            return PrimitiveResult(
                success=False,
                error=f"Unknown format_type: {format_type}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            import re

            # Sample data
            sample_df = df.head(sample_size) if len(df) > sample_size else df
            col_data = sample_df[column].dropna().astype(str)

            # Check format compliance
            matches = col_data.str.match(pattern, case=False)
            match_count = int(matches.sum())
            total_count = len(col_data)
            match_rate = match_count / total_count if total_count > 0 else 0

            # Get samples of non-matching values
            non_matching = col_data[~matches].head(10).tolist()

            # Try to detect what format the non-matching values are in
            detected_formats = self._detect_formats(non_matching)

            has_drift = match_rate < 1.0

            return PrimitiveResult(
                success=True,
                df=df,
                rows_before=rows_before,
                rows_after=len(df),
                cols_before=cols_before,
                cols_after=len(df.columns),
                metadata={
                    "has_drift": has_drift,
                    "expected_format": format_type or "custom",
                    "match_rate": round(match_rate, 4),
                    "match_count": match_count,
                    "non_match_count": total_count - match_count,
                    "non_matching_samples": non_matching,
                    "detected_alternative_formats": detected_formats,
                },
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )

    def _detect_formats(self, values: list) -> list:
        """Try to detect what formats the values are in."""
        detected = []
        for name, pattern in self.FORMATS.items():
            import re
            match_count = sum(1 for v in values if re.match(pattern, str(v), re.IGNORECASE))
            if match_count > 0:
                detected.append({"format": name, "match_count": match_count})
        return detected


# =============================================================================
# detect_distribution_drift
# =============================================================================


@register_primitive
class DetectDistributionDrift(Primitive):
    """Compare value distributions against a baseline/expected distribution."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="detect_distribution_drift",
            category="quality",
            description="Detect statistical drift in value distributions compared to baseline",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to analyze for distribution drift",
                ),
                ParamDef(
                    name="baseline_stats",
                    type="dict",
                    required=False,
                    default=None,
                    description="Baseline statistics: {mean, std, min, max, p25, p50, p75}",
                ),
                ParamDef(
                    name="baseline_distribution",
                    type="dict",
                    required=False,
                    default=None,
                    description="For categorical: expected value frequencies {value: proportion}",
                ),
                ParamDef(
                    name="drift_threshold",
                    type="float",
                    required=False,
                    default=0.1,
                    description="Threshold for flagging drift (0-1)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Check if amount distribution drifted from baseline mean=100, std=20",
                    expected_params={
                        "column": "amount",
                        "baseline_stats": {"mean": 100, "std": 20},
                    },
                    description="Numeric distribution drift",
                ),
                TestPrompt(
                    prompt="Detect distribution drift in status column against expected proportions",
                    expected_params={
                        "column": "status",
                        "baseline_distribution": {"active": 0.7, "inactive": 0.2, "pending": 0.1},
                    },
                    description="Categorical distribution drift",
                ),
                TestPrompt(
                    prompt="Check for drift in price with 5% threshold",
                    expected_params={
                        "column": "price",
                        "drift_threshold": 0.05,
                    },
                    description="Drift with custom threshold",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        baseline_stats = params.get("baseline_stats")
        baseline_dist = params.get("baseline_distribution")
        threshold = params.get("drift_threshold", 0.1)

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
            col_data = df[column].dropna()
            is_numeric = pd.api.types.is_numeric_dtype(col_data)

            drift_details = {
                "column": column,
                "is_numeric": is_numeric,
                "has_drift": False,
                "drift_score": 0.0,
                "drifted_metrics": [],
            }

            if is_numeric and baseline_stats:
                # Numeric distribution comparison
                observed_stats = {
                    "mean": float(col_data.mean()),
                    "std": float(col_data.std()),
                    "min": float(col_data.min()),
                    "max": float(col_data.max()),
                    "p25": float(col_data.quantile(0.25)),
                    "p50": float(col_data.quantile(0.50)),
                    "p75": float(col_data.quantile(0.75)),
                }
                drift_details["observed_stats"] = observed_stats

                # Compare each stat
                comparisons = []
                for stat, baseline_val in baseline_stats.items():
                    if stat in observed_stats and baseline_val != 0:
                        observed_val = observed_stats[stat]
                        relative_diff = abs(observed_val - baseline_val) / abs(baseline_val)
                        drifted = relative_diff > threshold

                        comparisons.append({
                            "metric": stat,
                            "baseline": baseline_val,
                            "observed": round(observed_val, 4),
                            "relative_diff": round(relative_diff, 4),
                            "drifted": drifted,
                        })

                        if drifted:
                            drift_details["drifted_metrics"].append(stat)

                drift_details["comparisons"] = comparisons
                drift_details["drift_score"] = sum(c["relative_diff"] for c in comparisons) / len(comparisons) if comparisons else 0

            elif baseline_dist:
                # Categorical distribution comparison
                observed_counts = col_data.value_counts(normalize=True).to_dict()
                observed_dist = {str(k): round(v, 4) for k, v in observed_counts.items()}
                drift_details["observed_distribution"] = observed_dist

                # Compare distributions
                comparisons = []
                all_keys = set(baseline_dist.keys()) | set(observed_dist.keys())

                for key in all_keys:
                    baseline_val = baseline_dist.get(str(key), 0)
                    observed_val = observed_dist.get(str(key), 0)
                    diff = abs(observed_val - baseline_val)
                    drifted = diff > threshold

                    comparisons.append({
                        "value": key,
                        "baseline_proportion": baseline_val,
                        "observed_proportion": observed_val,
                        "absolute_diff": round(diff, 4),
                        "drifted": drifted,
                    })

                    if drifted:
                        drift_details["drifted_metrics"].append(key)

                drift_details["comparisons"] = comparisons
                drift_details["drift_score"] = sum(c["absolute_diff"] for c in comparisons) / len(comparisons) if comparisons else 0

            else:
                # No baseline provided - just return current stats
                if is_numeric:
                    drift_details["observed_stats"] = {
                        "mean": float(col_data.mean()),
                        "std": float(col_data.std()),
                        "min": float(col_data.min()),
                        "max": float(col_data.max()),
                    }
                else:
                    drift_details["observed_distribution"] = col_data.value_counts(normalize=True).head(10).to_dict()

            drift_details["has_drift"] = len(drift_details["drifted_metrics"]) > 0

            return PrimitiveResult(
                success=True,
                df=df,
                rows_before=rows_before,
                rows_after=len(df),
                cols_before=cols_before,
                cols_after=len(df.columns),
                metadata=drift_details,
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# normalize_boolean
# =============================================================================


@register_primitive
class NormalizeBoolean(Primitive):
    """Convert various boolean representations to a canonical format."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="normalize_boolean",
            category="quality",
            description="Convert yes/no, Y/N, 1/0, true/false to canonical boolean values",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to normalize",
                ),
                ParamDef(
                    name="true_values",
                    type="list",
                    required=False,
                    default=None,
                    description="Values to treat as True (default: yes, y, true, t, 1, on)",
                ),
                ParamDef(
                    name="false_values",
                    type="list",
                    required=False,
                    default=None,
                    description="Values to treat as False (default: no, n, false, f, 0, off)",
                ),
                ParamDef(
                    name="output_format",
                    type="str",
                    required=False,
                    default="boolean",
                    description="Output format: 'boolean' (True/False), 'integer' (1/0), 'string' (true/false)",
                    choices=["boolean", "integer", "string"],
                ),
                ParamDef(
                    name="null_handling",
                    type="str",
                    required=False,
                    default="keep",
                    description="How to handle nulls: 'keep', 'true', 'false'",
                    choices=["keep", "true", "false"],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Convert the active column from yes/no to true/false",
                    expected_params={
                        "column": "active",
                    },
                    description="Basic boolean normalization",
                ),
                TestPrompt(
                    prompt="Normalize is_enabled to 1/0 format",
                    expected_params={
                        "column": "is_enabled",
                        "output_format": "integer",
                    },
                    description="Normalize to integer",
                ),
                TestPrompt(
                    prompt="Convert flag column treating 'X' as true and blank as false",
                    expected_params={
                        "column": "flag",
                        "true_values": ["X", "x"],
                        "false_values": ["", " "],
                    },
                    description="Custom boolean mapping",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    DEFAULT_TRUE = {"yes", "y", "true", "t", "1", "on", "1.0"}
    DEFAULT_FALSE = {"no", "n", "false", "f", "0", "off", "0.0", ""}

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        true_values = params.get("true_values")
        false_values = params.get("false_values")
        output_format = params.get("output_format", "boolean")
        null_handling = params.get("null_handling", "keep")

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

            # Build lookup sets
            true_set = set(str(v).lower().strip() for v in true_values) if true_values else self.DEFAULT_TRUE
            false_set = set(str(v).lower().strip() for v in false_values) if false_values else self.DEFAULT_FALSE

            # Track conversion stats
            true_count = 0
            false_count = 0
            null_count = 0
            unknown_count = 0
            unknown_values = set()

            def convert_value(val):
                nonlocal true_count, false_count, null_count, unknown_count, unknown_values

                if pd.isna(val):
                    null_count += 1
                    if null_handling == "true":
                        true_count += 1
                        return True
                    elif null_handling == "false":
                        false_count += 1
                        return False
                    return None

                str_val = str(val).lower().strip()

                if str_val in true_set:
                    true_count += 1
                    return True
                elif str_val in false_set:
                    false_count += 1
                    return False
                else:
                    unknown_count += 1
                    unknown_values.add(str(val))
                    return None

            # Apply conversion
            result_df[column] = df[column].apply(convert_value)

            # Convert to output format
            if output_format == "integer":
                result_df[column] = result_df[column].map({True: 1, False: 0, None: None})
            elif output_format == "string":
                result_df[column] = result_df[column].map({True: "true", False: "false", None: None})

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "true_count": true_count,
                    "false_count": false_count,
                    "null_count": null_count,
                    "unknown_count": unknown_count,
                    "unknown_values": list(unknown_values)[:10],
                    "output_format": output_format,
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
# normalize_enum_values
# =============================================================================


@register_primitive
class NormalizeEnumValues(Primitive):
    """Map variant labels to canonical values."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="normalize_enum_values",
            category="quality",
            description="Map variant values to canonical labels (e.g., 'USA', 'U.S.' → 'US')",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to normalize",
                ),
                ParamDef(
                    name="mapping",
                    type="dict",
                    required=True,
                    description="Mapping of canonical value to list of variants: {'US': ['USA', 'U.S.', 'United States']}",
                ),
                ParamDef(
                    name="case_sensitive",
                    type="bool",
                    required=False,
                    default=False,
                    description="Case-sensitive matching of variants",
                ),
                ParamDef(
                    name="unmapped_handling",
                    type="str",
                    required=False,
                    default="keep",
                    description="How to handle unmapped values: 'keep', 'null', 'error'",
                    choices=["keep", "null", "error"],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Normalize country names: US for USA/United States, UK for United Kingdom",
                    expected_params={
                        "column": "country",
                        "mapping": {
                            "US": ["USA", "U.S.", "United States"],
                            "UK": ["United Kingdom", "Great Britain", "GB"],
                        },
                    },
                    description="Normalize country codes",
                ),
                TestPrompt(
                    prompt="Standardize status values to active/inactive",
                    expected_params={
                        "column": "status",
                        "mapping": {
                            "active": ["enabled", "on", "yes", "1"],
                            "inactive": ["disabled", "off", "no", "0"],
                        },
                    },
                    description="Normalize status values",
                ),
                TestPrompt(
                    prompt="Normalize gender column and set unknown values to null",
                    expected_params={
                        "column": "gender",
                        "mapping": {
                            "M": ["male", "m", "man"],
                            "F": ["female", "f", "woman"],
                        },
                        "unmapped_handling": "null",
                    },
                    description="Normalize with null for unknown",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        mapping = params["mapping"]
        case_sensitive = params.get("case_sensitive", False)
        unmapped_handling = params.get("unmapped_handling", "keep")

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

            # Build reverse mapping (variant -> canonical)
            reverse_map = {}
            for canonical, variants in mapping.items():
                # Add canonical value itself
                key = canonical if case_sensitive else canonical.lower()
                reverse_map[key] = canonical

                for variant in variants:
                    key = str(variant) if case_sensitive else str(variant).lower()
                    reverse_map[key] = canonical

            # Track stats
            mapped_count = 0
            unmapped_count = 0
            unmapped_values = set()

            def normalize_value(val):
                nonlocal mapped_count, unmapped_count, unmapped_values

                if pd.isna(val):
                    return None

                lookup_key = str(val) if case_sensitive else str(val).lower()

                if lookup_key in reverse_map:
                    mapped_count += 1
                    return reverse_map[lookup_key]
                else:
                    unmapped_count += 1
                    unmapped_values.add(str(val))

                    if unmapped_handling == "null":
                        return None
                    elif unmapped_handling == "error":
                        raise ValueError(f"Unmapped value: {val}")
                    else:  # keep
                        return val

            result_df[column] = df[column].apply(normalize_value)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "mapped_count": mapped_count,
                    "unmapped_count": unmapped_count,
                    "unmapped_values": list(unmapped_values)[:20],
                    "canonical_values": list(mapping.keys()),
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
# generate_drift_report
# =============================================================================


@register_primitive
class GenerateDriftReport(Primitive):
    """Generate a comprehensive schema drift report for a dataset."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="generate_drift_report",
            category="quality",
            description="Generate a comprehensive report summarizing all detected schema drifts",
            params=[
                ParamDef(
                    name="expected_columns",
                    type="list[str]",
                    required=False,
                    default=None,
                    description="Expected column names to check",
                ),
                ParamDef(
                    name="expected_types",
                    type="dict[str, str]",
                    required=False,
                    default=None,
                    description="Expected types per column",
                ),
                ParamDef(
                    name="enum_columns",
                    type="dict[str, list]",
                    required=False,
                    default=None,
                    description="Columns with expected enum values: {col: [values]}",
                ),
                ParamDef(
                    name="baseline_stats",
                    type="dict[str, dict]",
                    required=False,
                    default=None,
                    description="Baseline statistics per column: {col: {mean, std, ...}}",
                ),
                ParamDef(
                    name="include_recommendations",
                    type="bool",
                    required=False,
                    default=True,
                    description="Include recommended actions for each drift",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Generate drift report for expected columns: id, name, email, amount",
                    expected_params={
                        "expected_columns": ["id", "name", "email", "amount"],
                    },
                    description="Basic drift report",
                ),
                TestPrompt(
                    prompt="Full drift report with types and enum checks",
                    expected_params={
                        "expected_columns": ["id", "status", "amount"],
                        "expected_types": {"id": "integer", "amount": "float"},
                        "enum_columns": {"status": ["active", "inactive"]},
                    },
                    description="Comprehensive drift report",
                ),
                TestPrompt(
                    prompt="Generate drift report with baseline statistics comparison",
                    expected_params={
                        "expected_columns": ["revenue"],
                        "baseline_stats": {"revenue": {"mean": 1000, "std": 200}},
                    },
                    description="Drift report with stats",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        expected_columns = params.get("expected_columns", [])
        expected_types = params.get("expected_types", {})
        enum_columns = params.get("enum_columns", {})
        baseline_stats = params.get("baseline_stats", {})
        include_recs = params.get("include_recommendations", True)

        rows_before = len(df)
        cols_before = len(df.columns)

        try:
            report = {
                "summary": {
                    "dataset_rows": rows_before,
                    "dataset_columns": cols_before,
                    "expected_columns": len(expected_columns),
                    "total_drifts": 0,
                    "severity": "none",
                },
                "column_drift": {
                    "missing": [],
                    "extra": [],
                    "renamed_candidates": [],
                },
                "type_drift": [],
                "enum_drift": [],
                "distribution_drift": [],
                "recommendations": [],
            }

            actual_columns = list(df.columns)
            actual_lower = {c.lower(): c for c in actual_columns}

            # Check column presence
            if expected_columns:
                for exp_col in expected_columns:
                    if exp_col.lower() not in actual_lower:
                        report["column_drift"]["missing"].append(exp_col)
                        report["summary"]["total_drifts"] += 1

                        if include_recs:
                            report["recommendations"].append({
                                "type": "MISSING_COLUMN",
                                "column": exp_col,
                                "action": f"Add column '{exp_col}' with default value or map from existing column",
                            })

                expected_lower = {c.lower() for c in expected_columns}
                for act_col in actual_columns:
                    if act_col.lower() not in expected_lower:
                        report["column_drift"]["extra"].append(act_col)

                # Try to find renamed columns
                for missing in report["column_drift"]["missing"]:
                    for extra in report["column_drift"]["extra"]:
                        similarity = self._similarity(missing.lower(), extra.lower())
                        if similarity > 0.6:
                            report["column_drift"]["renamed_candidates"].append({
                                "expected": missing,
                                "observed": extra,
                                "similarity": round(similarity, 2),
                            })

            # Check types
            for col, exp_type in expected_types.items():
                if col.lower() in actual_lower:
                    actual_col = actual_lower[col.lower()]
                    actual_dtype = df[actual_col].dtype

                    obs_type = self._classify_dtype(actual_dtype)
                    if obs_type != exp_type:
                        report["type_drift"].append({
                            "column": col,
                            "expected": exp_type,
                            "observed": obs_type,
                        })
                        report["summary"]["total_drifts"] += 1

                        if include_recs:
                            report["recommendations"].append({
                                "type": "TYPE_MISMATCH",
                                "column": col,
                                "action": f"Cast column '{col}' from {obs_type} to {exp_type}",
                            })

            # Check enums
            for col, expected_vals in enum_columns.items():
                if col.lower() in actual_lower:
                    actual_col = actual_lower[col.lower()]
                    observed_vals = set(df[actual_col].dropna().astype(str).str.lower())
                    expected_set = set(str(v).lower() for v in expected_vals)

                    new_vals = observed_vals - expected_set
                    missing_vals = expected_set - observed_vals

                    if new_vals or missing_vals:
                        report["enum_drift"].append({
                            "column": col,
                            "new_values": list(new_vals),
                            "missing_values": list(missing_vals),
                        })
                        report["summary"]["total_drifts"] += 1

                        if include_recs and new_vals:
                            report["recommendations"].append({
                                "type": "ENUM_DRIFT",
                                "column": col,
                                "action": f"Map new values {list(new_vals)} to canonical values or update enum definition",
                            })

            # Check distributions
            for col, stats in baseline_stats.items():
                if col.lower() in actual_lower:
                    actual_col = actual_lower[col.lower()]
                    col_data = df[actual_col].dropna()

                    if pd.api.types.is_numeric_dtype(col_data):
                        obs_mean = float(col_data.mean())
                        obs_std = float(col_data.std())

                        drifted_metrics = []
                        if "mean" in stats:
                            diff = abs(obs_mean - stats["mean"]) / abs(stats["mean"]) if stats["mean"] != 0 else 0
                            if diff > 0.1:
                                drifted_metrics.append(f"mean ({stats['mean']} -> {obs_mean:.2f})")

                        if "std" in stats:
                            diff = abs(obs_std - stats["std"]) / abs(stats["std"]) if stats["std"] != 0 else 0
                            if diff > 0.1:
                                drifted_metrics.append(f"std ({stats['std']} -> {obs_std:.2f})")

                        if drifted_metrics:
                            report["distribution_drift"].append({
                                "column": col,
                                "drifted_metrics": drifted_metrics,
                            })
                            report["summary"]["total_drifts"] += 1

            # Determine severity
            total = report["summary"]["total_drifts"]
            if total == 0:
                report["summary"]["severity"] = "none"
            elif len(report["column_drift"]["missing"]) > 0:
                report["summary"]["severity"] = "critical"
            elif len(report["type_drift"]) > 0:
                report["summary"]["severity"] = "high"
            elif total > 3:
                report["summary"]["severity"] = "medium"
            else:
                report["summary"]["severity"] = "low"

            return PrimitiveResult(
                success=True,
                df=df,
                rows_before=rows_before,
                rows_after=len(df),
                cols_before=cols_before,
                cols_after=len(df.columns),
                metadata=report,
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )

    def _similarity(self, s1: str, s2: str) -> float:
        """Simple string similarity."""
        if s1 == s2:
            return 1.0
        if not s1 or not s2:
            return 0.0

        # Check containment
        if s1 in s2 or s2 in s1:
            return min(len(s1), len(s2)) / max(len(s1), len(s2))

        # Character overlap
        set1, set2 = set(s1), set(s2)
        return len(set1 & set2) / len(set1 | set2)

    def _classify_dtype(self, dtype) -> str:
        """Classify dtype to our type names."""
        if pd.api.types.is_integer_dtype(dtype):
            return "integer"
        elif pd.api.types.is_float_dtype(dtype):
            return "float"
        elif pd.api.types.is_bool_dtype(dtype):
            return "boolean"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "datetime"
        return "string"
