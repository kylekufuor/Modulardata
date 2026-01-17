# =============================================================================
# agents/quality_checks/values.py - Value Range and Outlier Checks
# =============================================================================
# Checks for numeric value bounds and outlier detection.
# =============================================================================

import pandas as pd
import numpy as np

from agents.models.technical_plan import TechnicalPlan
from agents.models.test_result import QualityIssue, Severity
from agents.quality_checks.registry import register_check


@register_check(
    "numeric_bounds",
    applies_to=["round_numbers", "normalize", "add_column"]
)
def check_numeric_bounds(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Check that numeric values are within reasonable bounds after transformation.
    """
    issues = []

    target_cols = plan.get_target_column_names()

    trans_type = plan.transformation_type
    if hasattr(trans_type, 'value'):
        trans_type = trans_type.value

    # For add_column, check the new column
    if trans_type == "add_column":
        new_col = plan.parameters.get("name")
        if new_col and new_col in after_df.columns:
            target_cols = [new_col]

    for col in target_cols:
        if col not in after_df.columns:
            continue

        # Only check numeric columns
        if not pd.api.types.is_numeric_dtype(after_df[col]):
            continue

        col_data = after_df[col].dropna()
        if len(col_data) == 0:
            continue

        # Check for infinity
        inf_count = np.isinf(col_data).sum()
        if inf_count > 0:
            issues.append(QualityIssue(
                check_name="numeric_bounds",
                severity=Severity.WARNING,
                column=col,
                message=f"Column '{col}' contains {inf_count} infinite values",
                details={"infinite_count": int(inf_count)},
                suggestion="Check for division by zero or overflow in calculations."
            ))

        # For normalize, check expected range
        if trans_type == "normalize":
            method = plan.parameters.get("method", "minmax")
            if method == "minmax":
                min_val = col_data.min()
                max_val = col_data.max()
                if min_val < -0.01 or max_val > 1.01:  # Allow small float errors
                    issues.append(QualityIssue(
                        check_name="numeric_bounds",
                        severity=Severity.WARNING,
                        column=col,
                        message=f"Normalized column '{col}' has values outside [0,1] range",
                        details={"min": float(min_val), "max": float(max_val)}
                    ))

    return issues


@register_check(
    "value_changes",
    applies_to=["replace_values"]
)
def check_value_changes(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Verify that replace_values actually made changes.
    """
    issues = []

    target_cols = plan.get_target_column_names()
    old_value = plan.parameters.get("old_value")
    new_value = plan.parameters.get("new_value")

    # If no specific columns, check all
    if not target_cols:
        target_cols = list(set(before_df.columns) & set(after_df.columns))

    total_changes = 0

    for col in target_cols:
        if col not in before_df.columns or col not in after_df.columns:
            continue

        # Count how many values changed
        try:
            before_matches = (before_df[col] == old_value).sum()
            after_matches = (after_df[col] == old_value).sum()
            changes = before_matches - after_matches
            total_changes += changes
        except Exception:
            # Comparison might fail for complex types
            pass

    if total_changes == 0:
        issues.append(QualityIssue(
            check_name="value_changes",
            severity=Severity.INFO,
            message=f"No occurrences of '{old_value}' found to replace",
            details={"old_value": str(old_value), "new_value": str(new_value)}
        ))

    return issues


@register_check(
    "sort_order_valid",
    applies_to=["sort_rows"]
)
def check_sort_order_valid(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Verify that sort actually ordered the data correctly.
    """
    issues = []

    sort_cols = plan.parameters.get("columns", [])
    ascending = plan.parameters.get("ascending", True)

    if not sort_cols:
        sort_cols = plan.get_target_column_names()

    if not sort_cols:
        return issues

    # Check if the first sort column is actually sorted
    col = sort_cols[0]
    if col not in after_df.columns:
        return issues

    col_data = after_df[col].dropna()
    if len(col_data) < 2:
        return issues

    # Check if sorted
    if ascending:
        is_sorted = col_data.is_monotonic_increasing
    else:
        is_sorted = col_data.is_monotonic_decreasing

    if not is_sorted:
        # Could be due to NaN handling or ties in other columns
        issues.append(QualityIssue(
            check_name="sort_order_valid",
            severity=Severity.INFO,
            column=col,
            message=f"Column '{col}' may not be strictly sorted (could be due to ties or NaN values)",
            details={"column": col, "ascending": ascending}
        ))

    return issues


@register_check(
    "phone_format_valid",
    applies_to=["format_phone"]
)
def check_phone_format_valid(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Verify that phone formatting actually changed the values to the expected format.
    """
    import re
    issues = []

    target_cols = plan.get_target_column_names()
    output_format = plan.parameters.get("output_format", "nnn-nnn-nnnn")

    # Define expected patterns for each format
    format_patterns = {
        "nnn-nnn-nnnn": r'^\d{3}-\d{3}-\d{4}$',
        "(nnn) nnn-nnnn": r'^\(\d{3}\) \d{3}-\d{4}$',
        "nnn.nnn.nnnn": r'^\d{3}\.\d{3}\.\d{4}$',
        "nnnnnnnnnn": r'^\d{10}$',
        "+1-nnn-nnn-nnnn": r'^\+1-\d{3}-\d{3}-\d{4}$',
    }

    expected_pattern = format_patterns.get(output_format, r'^\d{3}-\d{3}-\d{4}$')

    for col in target_cols:
        if col not in after_df.columns:
            continue

        # Count how many values match the expected format
        col_data = after_df[col].dropna().astype(str)
        if len(col_data) == 0:
            continue

        matches = col_data.str.match(expected_pattern, na=False)
        match_count = matches.sum()
        total_count = len(col_data)
        match_rate = match_count / total_count if total_count > 0 else 0

        # Check if before was different from after (something actually changed)
        if col in before_df.columns:
            before_data = before_df[col].dropna().astype(str)
            before_matches = before_data.str.match(expected_pattern, na=False).sum()
            changes_made = match_count - before_matches
        else:
            changes_made = match_count

        if match_rate < 0.5:
            # Less than 50% match the expected format - this is a problem
            sample_bad = col_data[~matches].head(3).tolist()
            issues.append(QualityIssue(
                check_name="phone_format_valid",
                severity=Severity.ERROR,
                column=col,
                message=f"Only {match_rate:.0%} of phone numbers match the expected format '{output_format}'",
                details={
                    "expected_format": output_format,
                    "match_rate": float(match_rate),
                    "sample_non_matching": sample_bad
                },
                suggestion="Some phone numbers may have invalid formats that couldn't be standardized."
            ))
        elif changes_made == 0:
            # No changes were made at all
            issues.append(QualityIssue(
                check_name="phone_format_valid",
                severity=Severity.WARNING,
                column=col,
                message=f"Phone formatting made no changes to '{col}' - values may already be in the target format or couldn't be parsed",
                details={"expected_format": output_format}
            ))
        elif match_rate < 0.9:
            # 50-90% match - warning
            issues.append(QualityIssue(
                check_name="phone_format_valid",
                severity=Severity.WARNING,
                column=col,
                message=f"{(1-match_rate):.0%} of phone numbers couldn't be formatted to '{output_format}'",
                details={
                    "expected_format": output_format,
                    "match_rate": float(match_rate)
                }
            ))

    return issues


@register_check(
    "aggregation_values",
    applies_to=["group_by"]
)
def check_aggregation_values(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Basic sanity check on aggregation results.
    """
    issues = []

    aggregations = plan.parameters.get("aggregations", {})

    for col, agg_func in aggregations.items():
        if col not in before_df.columns or col not in after_df.columns:
            continue

        # For sum, the total should roughly match
        if agg_func in ["sum", "mean"]:
            if not pd.api.types.is_numeric_dtype(before_df[col]):
                continue

            before_total = before_df[col].sum()
            after_total = after_df[col].sum()

            if agg_func == "sum":
                # Sum should be preserved
                if abs(before_total - after_total) > 0.01 * abs(before_total):
                    issues.append(QualityIssue(
                        check_name="aggregation_values",
                        severity=Severity.INFO,
                        column=col,
                        message=f"Sum of '{col}' changed from {before_total:.2f} to {after_total:.2f}",
                        details={
                            "before_sum": float(before_total),
                            "after_sum": float(after_total)
                        }
                    ))

    return issues
