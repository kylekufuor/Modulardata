# =============================================================================
# agents/quality_checks/duplicates.py - Duplicate Detection Checks
# =============================================================================
# Checks for duplicate rows and deduplicate operation success.
# =============================================================================

import pandas as pd

from agents.models.technical_plan import TechnicalPlan
from agents.models.test_result import QualityIssue, Severity
from agents.quality_checks.registry import register_check


@register_check(
    "deduplicate_success",
    applies_to=["deduplicate"]
)
def check_deduplicate_success(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Verify that deduplicate actually removed duplicates.
    """
    issues = []

    # Get subset columns if specified
    target_cols = plan.get_target_column_names() or None

    # Count duplicates before and after
    if target_cols:
        dupes_before = before_df.duplicated(subset=target_cols).sum()
        dupes_after = after_df.duplicated(subset=target_cols).sum()
    else:
        dupes_before = before_df.duplicated().sum()
        dupes_after = after_df.duplicated().sum()

    if dupes_before > 0 and dupes_after == dupes_before:
        issues.append(QualityIssue(
            check_name="deduplicate_success",
            severity=Severity.WARNING,
            message=f"Still have {dupes_after} duplicate rows after deduplication",
            details={
                "duplicates_before": int(dupes_before),
                "duplicates_after": int(dupes_after),
                "subset": target_cols
            },
            suggestion="Deduplication may not have worked. Check your criteria."
        ))
    elif dupes_after > 0:
        # Partial deduplication - this is unusual
        issues.append(QualityIssue(
            check_name="deduplicate_success",
            severity=Severity.INFO,
            message=f"Reduced duplicates from {dupes_before} to {dupes_after}",
            details={
                "duplicates_before": int(dupes_before),
                "duplicates_after": int(dupes_after)
            }
        ))
    elif dupes_before == 0:
        issues.append(QualityIssue(
            check_name="deduplicate_success",
            severity=Severity.INFO,
            message="No duplicates were found to remove",
            details={"duplicates_before": 0}
        ))

    return issues


@register_check(
    "new_duplicates_check",
    applies_to=["fill_nulls", "replace_values", "change_case", "trim_whitespace"]
)
def check_new_duplicates_introduced(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Check if transformation accidentally introduced duplicate rows.

    This can happen when:
    - fill_nulls makes previously unique rows identical
    - replace_values collapses distinct values
    - Case changes make different strings identical
    """
    issues = []

    # Only check if we have a reasonable number of rows
    if len(before_df) > 100000:
        return issues  # Skip for performance

    dupes_before = before_df.duplicated().sum()
    dupes_after = after_df.duplicated().sum()
    new_dupes = dupes_after - dupes_before

    if new_dupes > 0:
        pct = new_dupes / len(after_df) if len(after_df) > 0 else 0
        severity = Severity.WARNING if pct > 0.05 else Severity.INFO

        issues.append(QualityIssue(
            check_name="new_duplicates_check",
            severity=severity,
            message=f"Transformation created {new_dupes} new duplicate rows",
            details={
                "duplicates_before": int(dupes_before),
                "duplicates_after": int(dupes_after),
                "new_duplicates": int(new_dupes)
            },
            suggestion="Values that were different are now identical. This may or may not be intended."
        ))

    return issues


@register_check(
    "join_duplicates",
    applies_to=["join"]
)
def check_join_duplicates(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Check for row explosion from join operations.

    Many-to-many joins can significantly increase row count.
    """
    issues = []

    rows_before = len(before_df)
    rows_after = len(after_df)

    if rows_after > rows_before * 2:
        explosion_factor = rows_after / rows_before if rows_before > 0 else 0
        issues.append(QualityIssue(
            check_name="join_duplicates",
            severity=Severity.WARNING,
            message=f"Join increased rows by {explosion_factor:.1f}x (from {rows_before} to {rows_after})",
            details={
                "rows_before": rows_before,
                "rows_after": rows_after,
                "explosion_factor": explosion_factor
            },
            suggestion="This might indicate a many-to-many join. Verify join keys are correct."
        ))

    return issues
