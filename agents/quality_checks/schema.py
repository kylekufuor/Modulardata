# =============================================================================
# agents/quality_checks/schema.py - Schema Validation Checks
# =============================================================================
# Checks for column existence and data type consistency.
# =============================================================================

import pandas as pd

from agents.models.technical_plan import TechnicalPlan
from agents.models.test_result import QualityIssue, Severity
from agents.quality_checks.registry import register_check


@register_check("schema_valid", universal=True)
def check_schema_valid(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Universal check: Verify the schema is valid after transformation.

    Checks:
    - DataFrame is not empty (unless that's expected)
    - At least one column exists
    - Column names are valid strings
    """
    issues = []

    # Check for completely empty result (might be intentional)
    if len(after_df) == 0 and len(before_df) > 0:
        trans_type = plan.transformation_type
        if hasattr(trans_type, 'value'):
            trans_type = trans_type.value

        # Some operations can legitimately result in empty df
        row_reducing_ops = ['drop_rows', 'filter_rows', 'deduplicate', 'slice_rows']
        if trans_type not in row_reducing_ops:
            issues.append(QualityIssue(
                check_name="schema_valid",
                severity=Severity.ERROR,
                message="Transformation resulted in empty DataFrame",
                details={"rows_before": len(before_df), "rows_after": 0},
                suggestion="This transformation removed all rows. Consider adjusting your criteria."
            ))
        else:
            issues.append(QualityIssue(
                check_name="schema_valid",
                severity=Severity.WARNING,
                message="All rows were removed",
                details={"rows_before": len(before_df), "rows_after": 0},
                suggestion="Your filter criteria matched all rows. Is this intentional?"
            ))

    # Check for no columns
    if len(after_df.columns) == 0:
        issues.append(QualityIssue(
            check_name="schema_valid",
            severity=Severity.ERROR,
            message="All columns were removed",
            details={"cols_before": len(before_df.columns), "cols_after": 0},
            suggestion="The transformation removed all columns. This is likely an error."
        ))

    return issues


@register_check("target_columns_exist", universal=True)
def check_target_columns_exist(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Verify target columns still exist after transformation (where applicable).

    For drop_columns, we verify they were removed.
    For other operations, we verify they still exist.
    """
    issues = []
    target_cols = plan.get_target_column_names()

    trans_type = plan.transformation_type
    if hasattr(trans_type, 'value'):
        trans_type = trans_type.value

    # For drop_columns, verify columns were removed
    if trans_type == "drop_columns":
        for col in target_cols:
            if col in after_df.columns:
                issues.append(QualityIssue(
                    check_name="target_columns_exist",
                    severity=Severity.WARNING,
                    message=f"Column '{col}' was not removed as expected",
                    column=col,
                    details={"operation": "drop_columns"}
                ))

    # For rename_column, check new name exists
    elif trans_type == "rename_column":
        new_name = plan.parameters.get("new_name")
        if new_name and new_name not in after_df.columns:
            issues.append(QualityIssue(
                check_name="target_columns_exist",
                severity=Severity.ERROR,
                message=f"Renamed column '{new_name}' not found",
                column=new_name,
                details={"old_name": target_cols[0] if target_cols else None}
            ))

    # For add_column, check new column exists
    elif trans_type == "add_column":
        new_name = plan.parameters.get("name")
        if new_name and new_name not in after_df.columns:
            issues.append(QualityIssue(
                check_name="target_columns_exist",
                severity=Severity.ERROR,
                message=f"New column '{new_name}' was not created",
                column=new_name,
                details={"operation": "add_column"}
            ))

    # For other operations, target columns should still exist
    elif trans_type not in ["select_columns", "group_by"]:
        for col in target_cols:
            if col not in after_df.columns:
                issues.append(QualityIssue(
                    check_name="target_columns_exist",
                    severity=Severity.ERROR,
                    message=f"Target column '{col}' is missing after transformation",
                    column=col,
                    details={"operation": trans_type}
                ))

    return issues


@register_check(
    "column_types_preserved",
    applies_to=["trim_whitespace", "change_case", "replace_values", "fill_nulls",
                "deduplicate", "sort_rows", "filter_rows", "drop_rows"]
)
def check_column_types_preserved(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> list[QualityIssue]:
    """
    Check that column data types weren't changed unexpectedly.

    For operations that shouldn't change types, verify consistency.
    """
    issues = []

    # Get columns that exist in both
    common_cols = set(before_df.columns) & set(after_df.columns)

    for col in common_cols:
        before_type = str(before_df[col].dtype)
        after_type = str(after_df[col].dtype)

        # Allow some type coercion (e.g., int to float due to NaN handling)
        if before_type != after_type:
            # These are usually acceptable
            acceptable_changes = [
                ("int64", "float64"),  # NaN in int column
                ("int32", "float64"),
                ("Int64", "float64"),
                ("object", "string"),
            ]

            if (before_type, after_type) not in acceptable_changes:
                issues.append(QualityIssue(
                    check_name="column_types_preserved",
                    severity=Severity.INFO,
                    column=col,
                    message=f"Column '{col}' type changed from {before_type} to {after_type}",
                    details={"before_type": before_type, "after_type": after_type}
                ))

    return issues
