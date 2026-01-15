# =============================================================================
# agents/quality_checks/nulls.py - Null Value Checks
# =============================================================================
# Checks for null value changes and fill operation success.
# =============================================================================

import pandas as pd

from agents.models.technical_plan import TechnicalPlan
from agents.models.test_result import QualityIssue, Severity
from agents.quality_checks.registry import register_check


@register_check(
    "fill_nulls_success",
    applies_to=["fill_nulls"]
)
def check_fill_nulls_success(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Verify that fill_nulls actually reduced null counts in target columns.
    """
    issues = []

    target_cols = plan.get_target_column_names()

    for col in target_cols:
        if col not in before_df.columns or col not in after_df.columns:
            continue

        nulls_before = before_df[col].isna().sum()
        nulls_after = after_df[col].isna().sum()

        if nulls_before > 0 and nulls_after == nulls_before:
            issues.append(QualityIssue(
                check_name="fill_nulls_success",
                severity=Severity.WARNING,
                column=col,
                message=f"Column '{col}' still has {nulls_after} null values",
                details={
                    "nulls_before": int(nulls_before),
                    "nulls_after": int(nulls_after)
                },
                suggestion="Fill operation may not have worked. Check the fill value/method."
            ))
        elif nulls_after > 0 and nulls_after < nulls_before:
            # Partial fill - might be intentional (e.g., forward fill doesn't fill leading nulls)
            issues.append(QualityIssue(
                check_name="fill_nulls_success",
                severity=Severity.INFO,
                column=col,
                message=f"Column '{col}' reduced from {nulls_before} to {nulls_after} nulls",
                details={
                    "nulls_before": int(nulls_before),
                    "nulls_after": int(nulls_after),
                    "nulls_filled": int(nulls_before - nulls_after)
                }
            ))

    return issues


@register_check(
    "new_nulls_introduced",
    applies_to=["convert_type", "parse_date", "add_column", "extract_pattern"]
)
def check_new_nulls_introduced(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Check if transformation introduced new null values.

    This is common with type conversions that fail for some values.
    """
    issues = []

    target_cols = plan.get_target_column_names()

    # For add_column, check the new column
    trans_type = plan.transformation_type
    if hasattr(trans_type, 'value'):
        trans_type = trans_type.value

    if trans_type == "add_column":
        new_col = plan.parameters.get("name")
        if new_col and new_col in after_df.columns:
            nulls = after_df[new_col].isna().sum()
            if nulls > 0:
                issues.append(QualityIssue(
                    check_name="new_nulls_introduced",
                    severity=Severity.INFO,
                    column=new_col,
                    message=f"New column '{new_col}' has {nulls} null values",
                    details={"null_count": int(nulls)}
                ))
        return issues

    # For extract_pattern, check the extracted column
    if trans_type == "extract_pattern":
        new_col = plan.parameters.get("new_column")
        if new_col and new_col in after_df.columns:
            nulls = after_df[new_col].isna().sum()
            total = len(after_df)
            if nulls > 0:
                pct = nulls / total if total > 0 else 0
                severity = Severity.WARNING if pct > 0.5 else Severity.INFO
                issues.append(QualityIssue(
                    check_name="new_nulls_introduced",
                    severity=severity,
                    column=new_col,
                    message=f"Pattern extraction failed for {nulls} rows ({pct:.1%})",
                    details={"null_count": int(nulls), "total_rows": total}
                ))
        return issues

    # For other operations, check target columns
    for col in target_cols:
        if col not in before_df.columns or col not in after_df.columns:
            continue

        nulls_before = before_df[col].isna().sum()
        nulls_after = after_df[col].isna().sum()
        new_nulls = nulls_after - nulls_before

        if new_nulls > 0:
            severity = Severity.WARNING if new_nulls > len(after_df) * 0.1 else Severity.INFO

            issues.append(QualityIssue(
                check_name="new_nulls_introduced",
                severity=severity,
                column=col,
                message=f"Column '{col}' has {new_nulls} new null values",
                details={
                    "nulls_before": int(nulls_before),
                    "nulls_after": int(nulls_after),
                    "new_nulls": int(new_nulls)
                },
                suggestion="Some values couldn't be converted. Check the source data."
            ))

    return issues


@register_check(
    "drop_rows_null_check",
    applies_to=["drop_rows"]
)
def check_drop_rows_null_removed(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    For drop_rows with isnull condition, verify nulls were actually removed.
    """
    issues = []

    # Check if any condition is isnull
    has_isnull_condition = False
    isnull_columns = []

    for condition in plan.conditions:
        op = condition.operator
        if hasattr(op, 'value'):
            op = op.value
        if op == "isnull":
            has_isnull_condition = True
            isnull_columns.append(condition.column)

    if not has_isnull_condition:
        return issues

    # Verify those columns no longer have nulls
    for col in isnull_columns:
        if col not in after_df.columns:
            continue

        remaining_nulls = after_df[col].isna().sum()
        if remaining_nulls > 0:
            issues.append(QualityIssue(
                check_name="drop_rows_null_check",
                severity=Severity.WARNING,
                column=col,
                message=f"Column '{col}' still has {remaining_nulls} null values after drop",
                details={"remaining_nulls": int(remaining_nulls)},
                suggestion="Some null rows may not have been removed. Check conditions."
            ))

    return issues
