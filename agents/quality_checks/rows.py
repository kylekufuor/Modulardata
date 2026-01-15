# =============================================================================
# agents/quality_checks/rows.py - Row Count Validation Checks
# =============================================================================
# Checks for row count changes and potential data loss.
# =============================================================================

import pandas as pd

from agents.models.technical_plan import TechnicalPlan
from agents.models.test_result import QualityIssue, Severity
from agents.quality_checks.registry import register_check


# Thresholds for warnings
HIGH_DATA_LOSS_THRESHOLD = 0.5  # 50% data loss triggers warning
EXTREME_DATA_LOSS_THRESHOLD = 0.8  # 80% data loss triggers error


@register_check(
    "row_count_change",
    applies_to=["drop_rows", "filter_rows", "deduplicate", "slice_rows"]
)
def check_row_count_change(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Check that row count changes are reasonable for row-reducing operations.

    Flags:
    - Warning if > 50% of rows removed
    - Error if > 80% of rows removed
    """
    issues = []

    rows_before = len(before_df)
    rows_after = len(after_df)

    if rows_before == 0:
        return issues  # Nothing to check

    rows_removed = rows_before - rows_after
    removal_percent = rows_removed / rows_before

    if removal_percent >= EXTREME_DATA_LOSS_THRESHOLD:
        issues.append(QualityIssue(
            check_name="row_count_change",
            severity=Severity.WARNING,
            message=f"Removed {rows_removed} rows ({removal_percent:.1%} of data)",
            details={
                "rows_before": rows_before,
                "rows_after": rows_after,
                "rows_removed": rows_removed,
                "percent_removed": removal_percent
            },
            suggestion="This removes most of your data. Double-check your criteria."
        ))
    elif removal_percent >= HIGH_DATA_LOSS_THRESHOLD:
        issues.append(QualityIssue(
            check_name="row_count_change",
            severity=Severity.WARNING,
            message=f"Removed {rows_removed} rows ({removal_percent:.1%} of data)",
            details={
                "rows_before": rows_before,
                "rows_after": rows_after,
                "rows_removed": rows_removed,
                "percent_removed": removal_percent
            },
            suggestion="Significant data reduction. Verify this is expected."
        ))

    return issues


@register_check(
    "row_count_unchanged",
    applies_to=["trim_whitespace", "change_case", "replace_values", "fill_nulls",
                "rename_column", "convert_type", "round_numbers", "normalize",
                "sort_rows", "reorder_columns", "format_date", "parse_date"]
)
def check_row_count_unchanged(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Check that operations which shouldn't change row count didn't.

    These operations modify values but shouldn't add/remove rows.
    """
    issues = []

    rows_before = len(before_df)
    rows_after = len(after_df)

    if rows_before != rows_after:
        diff = rows_after - rows_before
        direction = "added" if diff > 0 else "removed"

        issues.append(QualityIssue(
            check_name="row_count_unchanged",
            severity=Severity.WARNING,
            message=f"Unexpected row change: {abs(diff)} rows {direction}",
            details={
                "rows_before": rows_before,
                "rows_after": rows_after,
                "difference": diff
            },
            suggestion="This operation shouldn't change row count. Check for issues."
        ))

    return issues


@register_check(
    "aggregation_row_count",
    applies_to=["group_by"]
)
def check_aggregation_row_count(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Check that group_by aggregation produced reasonable results.

    Flags:
    - Warning if result has same rows as input (grouping might not have worked)
    - Info about compression ratio
    """
    issues = []

    rows_before = len(before_df)
    rows_after = len(after_df)

    if rows_before == 0:
        return issues

    # If same row count, grouping might not have done anything
    if rows_before == rows_after:
        issues.append(QualityIssue(
            check_name="aggregation_row_count",
            severity=Severity.INFO,
            message="Aggregation produced same number of rows as input",
            details={
                "rows_before": rows_before,
                "rows_after": rows_after
            },
            suggestion="Each group might have only one row. Verify grouping columns."
        ))

    # If only 1 row, all data was aggregated together
    elif rows_after == 1:
        issues.append(QualityIssue(
            check_name="aggregation_row_count",
            severity=Severity.INFO,
            message="All rows aggregated into single result",
            details={
                "rows_before": rows_before,
                "rows_after": rows_after
            }
        ))

    return issues


@register_check(
    "slice_bounds",
    applies_to=["slice_rows"]
)
def check_slice_bounds(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Check that slice operation returned expected number of rows.
    """
    issues = []

    n = plan.parameters.get("n", 10)
    rows_before = len(before_df)
    rows_after = len(after_df)

    expected_rows = min(n, rows_before)

    if rows_after != expected_rows:
        issues.append(QualityIssue(
            check_name="slice_bounds",
            severity=Severity.WARNING,
            message=f"Expected {expected_rows} rows but got {rows_after}",
            details={
                "requested_n": n,
                "rows_before": rows_before,
                "expected_rows": expected_rows,
                "actual_rows": rows_after
            }
        ))

    return issues
