# =============================================================================
# agents/risk_assessment.py - Risk Assessment for Transformations
# =============================================================================
# Evaluates transformation plans to determine if they're "risky" and need
# user confirmation before applying.
#
# Risky operations include:
# - Removing >20% of rows
# - Keeping <50% of rows (aggressive filtering)
# - Dropping columns
# - Operations on deployed modules
# =============================================================================

from dataclasses import dataclass
from typing import Optional
import pandas as pd

from agents.models.technical_plan import TechnicalPlan, TransformationType


# Thresholds for what's considered "risky"
ROW_REMOVAL_THRESHOLD = 0.20  # >20% rows removed = risky
AGGRESSIVE_FILTER_THRESHOLD = 0.50  # keeping <50% of rows = risky


@dataclass
class RiskAssessment:
    """Result of risk assessment for a transformation."""
    is_risky: bool
    risk_level: str  # "none", "moderate", "high"
    reasons: list[str]
    preview: dict  # Preview of what will change
    confirmation_message: str  # Message to show user


def assess_transformation_risk(
    df: pd.DataFrame,
    plan: TechnicalPlan,
    is_deployed: bool = False,
) -> RiskAssessment:
    """
    Assess the risk level of a transformation before applying it.

    Args:
        df: Current DataFrame
        plan: The transformation plan to assess
        is_deployed: Whether this module is deployed (higher risk)

    Returns:
        RiskAssessment with risk level and preview info
    """
    reasons = []
    preview = {
        "rows_before": len(df),
        "cols_before": len(df.columns),
    }

    trans_type = plan.transformation_type
    if hasattr(trans_type, 'value'):
        trans_type = trans_type.value

    # Check if operating on deployed module
    if is_deployed:
        reasons.append("This module is deployed - changes will affect production")

    # Check for column dropping
    if trans_type == "drop_columns":
        cols_to_drop = plan.get_target_column_names()
        if cols_to_drop:
            reasons.append(f"Will permanently remove {len(cols_to_drop)} column(s): {', '.join(cols_to_drop)}")
            preview["columns_removed"] = cols_to_drop

    # Check for row-reducing operations
    if trans_type in ["drop_rows", "filter_rows", "deduplicate"]:
        # Estimate impact by running transformation in preview mode
        estimated_result = _estimate_row_impact(df, plan, trans_type)

        rows_before = len(df)
        rows_after = estimated_result["rows_after"]
        rows_removed = rows_before - rows_after
        removal_percent = rows_removed / rows_before if rows_before > 0 else 0

        preview["rows_after"] = rows_after
        preview["rows_removed"] = rows_removed
        preview["removal_percent"] = round(removal_percent * 100, 1)

        if trans_type == "filter_rows":
            keep_percent = rows_after / rows_before if rows_before > 0 else 0
            if keep_percent < AGGRESSIVE_FILTER_THRESHOLD:
                reasons.append(
                    f"Filter will keep only {preview['removal_percent']:.0f}% of rows "
                    f"({rows_after:,} of {rows_before:,})"
                )
        elif removal_percent > ROW_REMOVAL_THRESHOLD:
            reasons.append(
                f"Will remove {rows_removed:,} rows ({preview['removal_percent']:.0f}% of data)"
            )

        # Add sample of affected rows if available
        if "sample_removed" in estimated_result:
            preview["sample_removed"] = estimated_result["sample_removed"]

    # Determine risk level
    if not reasons:
        risk_level = "none"
        is_risky = False
        confirmation_message = ""
    elif is_deployed or len(reasons) > 1:
        risk_level = "high"
        is_risky = True
        confirmation_message = _build_confirmation_message(reasons, preview, "high")
    else:
        risk_level = "moderate"
        is_risky = True
        confirmation_message = _build_confirmation_message(reasons, preview, "moderate")

    return RiskAssessment(
        is_risky=is_risky,
        risk_level=risk_level,
        reasons=reasons,
        preview=preview,
        confirmation_message=confirmation_message,
    )


def _estimate_row_impact(
    df: pd.DataFrame,
    plan: TechnicalPlan,
    trans_type: str,
) -> dict:
    """
    Estimate how many rows will be affected without actually modifying data.
    """
    from agents.transformations.utils import build_condition_mask

    result = {"rows_after": len(df)}

    try:
        if trans_type == "filter_rows":
            if plan.conditions:
                mask = build_condition_mask(df, plan.conditions)
                result["rows_after"] = mask.sum()
                # Get sample of rows that will be removed
                removed_df = df[~mask]
                if len(removed_df) > 0:
                    result["sample_removed"] = removed_df.head(3).to_dict(orient="records")

        elif trans_type == "drop_rows":
            if plan.conditions:
                mask = build_condition_mask(df, plan.conditions)
                result["rows_after"] = len(df) - mask.sum()
                removed_df = df[mask]
                if len(removed_df) > 0:
                    result["sample_removed"] = removed_df.head(3).to_dict(orient="records")
            else:
                # Drop rows with nulls in target columns
                target_cols = plan.get_target_column_names()
                if target_cols:
                    mask = df[target_cols].isna().any(axis=1)
                    result["rows_after"] = len(df) - mask.sum()

        elif trans_type == "deduplicate":
            subset = plan.parameters.get("subset") or plan.get_target_column_names() or None
            keep = plan.parameters.get("keep", "first")
            # Count duplicates
            duplicated = df.duplicated(subset=subset, keep=keep)
            result["rows_after"] = len(df) - duplicated.sum()

    except Exception as e:
        # If estimation fails, assume no change (safer to not block)
        pass

    return result


def _build_confirmation_message(
    reasons: list[str],
    preview: dict,
    risk_level: str,
) -> str:
    """Build a user-friendly confirmation message."""
    if risk_level == "high":
        header = "⚠️ **This is a significant change.**"
    else:
        header = "This operation will modify your data."

    message_parts = [header, ""]

    for reason in reasons:
        message_parts.append(f"• {reason}")

    message_parts.append("")
    message_parts.append("Do you want to proceed?")

    return "\n".join(message_parts)
