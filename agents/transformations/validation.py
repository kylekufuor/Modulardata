# =============================================================================
# agents/transformations/validation.py - Data Quality & Validation Operations
# =============================================================================
# Data quality and validation operations:
# - validate_format: Check if values match patterns (email, phone, etc.)
# - mask_data: Mask sensitive data
# - flag_duplicates: Add boolean column for duplicates
# =============================================================================

import re
import pandas as pd
import numpy as np
from typing import Any

from agents.models.technical_plan import TechnicalPlan, TransformationType
from agents.transformations.registry import register


# Common validation patterns
VALIDATION_PATTERNS = {
    "email": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    "phone_us": r'^[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4}$',
    "phone_intl": r'^\+?[1-9]\d{1,14}$',
    "url": r'^https?://[^\s]+$',
    "zipcode_us": r'^\d{5}(-\d{4})?$',
    "ssn": r'^\d{3}-\d{2}-\d{4}$',
    "credit_card": r'^\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}$',
    "ip_address": r'^(\d{1,3}\.){3}\d{1,3}$',
    "date_iso": r'^\d{4}-\d{2}-\d{2}$',
    "date_us": r'^\d{2}/\d{2}/\d{4}$',
    "uuid": r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    "alphanumeric": r'^[a-zA-Z0-9]+$',
    "numeric": r'^-?\d+\.?\d*$',
}


@register(TransformationType.VALIDATE_FORMAT)
def validate_format(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Check if values match a pattern and flag invalid ones.

    Parameters (from plan.parameters):
        format_type: Predefined format type: "email", "phone_us", "phone_intl",
                     "url", "zipcode_us", "ssn", "credit_card", "ip_address",
                     "date_iso", "date_us", "uuid", "alphanumeric", "numeric"
        OR
        pattern: Custom regex pattern
        target_columns: Columns to validate
        suffix: Suffix for flag column (default: "_valid")
        invalid_action: "flag" (add bool column), "null" (set invalid to null),
                        "remove" (drop invalid rows) (default: "flag")

    Example:
        format_type="email", column="email"
        -> Creates email_valid column: True for valid emails, False otherwise
    """
    columns = plan.get_target_column_names()
    format_type = plan.parameters.get("format_type", "email")
    custom_pattern = plan.parameters.get("pattern")
    suffix = plan.parameters.get("suffix", "_valid")
    invalid_action = plan.parameters.get("invalid_action", "flag")

    # Get pattern
    if custom_pattern:
        pattern = custom_pattern
    else:
        pattern = VALIDATION_PATTERNS.get(format_type, r'.*')

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        # Check which values match the pattern
        valid_mask = result[col].astype(str).str.match(pattern, na=False)

        if invalid_action == "flag":
            new_col = f"{col}{suffix}"
            result[new_col] = valid_mask
            code_parts.append(f"df['{new_col}'] = df['{col}'].str.match(r'{pattern}')")

        elif invalid_action == "null":
            result.loc[~valid_mask, col] = np.nan
            code_parts.append(f"df.loc[~df['{col}'].str.match(r'{pattern}'), '{col}'] = np.nan")

        elif invalid_action == "remove":
            result = result[valid_mask]
            code_parts.append(f"df = df[df['{col}'].str.match(r'{pattern}')]")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.MASK_DATA)
def mask_data(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Mask sensitive data for privacy/security.

    Parameters (from plan.parameters):
        method: "last_n" (show last N chars), "first_n" (show first N),
                "middle" (show only middle), "all" (mask everything),
                "email" (mask email keeping domain)
        visible_chars: Number of characters to keep visible (default: 4)
        mask_char: Character to use for masking (default: "*")
        target_columns: Columns to mask

    Example:
        "4532-1234-5678-9012" (last_n=4) -> "************9012"
        "john@example.com" (email) -> "j***@example.com"
        "123-45-6789" (first_n=3) -> "123********"
    """
    columns = plan.get_target_column_names()
    method = plan.parameters.get("method", "last_n")
    visible_chars = plan.parameters.get("visible_chars", 4)
    mask_char = plan.parameters.get("mask_char", "*")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        def mask_value(val):
            if pd.isna(val):
                return val
            s = str(val)
            length = len(s)

            if method == "last_n":
                if length <= visible_chars:
                    return mask_char * length
                return mask_char * (length - visible_chars) + s[-visible_chars:]

            elif method == "first_n":
                if length <= visible_chars:
                    return mask_char * length
                return s[:visible_chars] + mask_char * (length - visible_chars)

            elif method == "middle":
                if length <= visible_chars * 2:
                    return mask_char * length
                return s[:visible_chars] + mask_char * (length - visible_chars * 2) + s[-visible_chars:]

            elif method == "all":
                return mask_char * length

            elif method == "email":
                if '@' in s:
                    local, domain = s.split('@', 1)
                    if len(local) > 1:
                        masked_local = local[0] + mask_char * (len(local) - 1)
                    else:
                        masked_local = mask_char
                    return f"{masked_local}@{domain}"
                return mask_char * length

            return s

        result[col] = result[col].apply(mask_value)
        code_parts.append(f"df['{col}'] = df['{col}'].apply(mask_{method})")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.FLAG_DUPLICATES)
def flag_duplicates(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Add boolean column indicating duplicate rows.

    Parameters (from plan.parameters):
        target_columns: Columns to check for duplicates. If empty, checks all columns.
        keep: Which occurrence to mark as NOT duplicate:
              "first" (mark all but first as duplicate),
              "last" (mark all but last as duplicate),
              False (mark all duplicates)
        column_name: Name for the flag column (default: "is_duplicate")

    Example:
        Rows [A, B, A, C] with keep="first"
        -> is_duplicate: [False, False, True, False]
    """
    columns = plan.get_target_column_names() or None
    keep = plan.parameters.get("keep", "first")
    column_name = plan.parameters.get("column_name", "is_duplicate")

    # Handle keep=False
    if keep == "false" or keep is False:
        keep = False

    result = df.copy()

    result[column_name] = result.duplicated(subset=columns, keep=keep)

    subset_str = f"subset={columns}, " if columns else ""
    code = f"df['{column_name}'] = df.duplicated({subset_str}keep={repr(keep)})"

    return result, code
