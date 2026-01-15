# =============================================================================
# agents/transformations/string_ops.py - String Operations
# =============================================================================
# Advanced string manipulation operations:
# - substring: Extract part of string
# - pad_string: Pad to fixed length
# - clean_text: Remove special characters
# - remove_html: Strip HTML tags
# =============================================================================

import re
import pandas as pd
import numpy as np
from typing import Any

from agents.models.technical_plan import TechnicalPlan, TransformationType
from agents.transformations.registry import register


@register(TransformationType.SUBSTRING)
def substring(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Extract part of a string by position or delimiter.

    Parameters (from plan.parameters):
        start: Start position (0-indexed, default: 0)
        end: End position (exclusive, default: None for end of string)
        OR
        before: Extract text before this delimiter
        after: Extract text after this delimiter
        target_columns: Columns to transform

    Example:
        "Hello World" with start=0, end=5 -> "Hello"
        "user@domain.com" with before="@" -> "user"
        "user@domain.com" with after="@" -> "domain.com"
    """
    columns = plan.get_target_column_names()
    start = plan.parameters.get("start")
    end = plan.parameters.get("end")
    before = plan.parameters.get("before")
    after = plan.parameters.get("after")
    suffix = plan.parameters.get("suffix", "_substr")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        new_col = f"{col}{suffix}" if plan.parameters.get("create_new", False) else col

        if before is not None:
            # Extract text before delimiter
            result[new_col] = result[col].astype(str).str.split(before).str[0]
            code_parts.append(f"df['{new_col}'] = df['{col}'].str.split('{before}').str[0]")

        elif after is not None:
            # Extract text after delimiter
            result[new_col] = result[col].astype(str).str.split(after).str[-1]
            code_parts.append(f"df['{new_col}'] = df['{col}'].str.split('{after}').str[-1]")

        else:
            # Extract by position
            start_pos = start if start is not None else 0
            if end is not None:
                result[new_col] = result[col].astype(str).str[start_pos:end]
                code_parts.append(f"df['{new_col}'] = df['{col}'].str[{start_pos}:{end}]")
            else:
                result[new_col] = result[col].astype(str).str[start_pos:]
                code_parts.append(f"df['{new_col}'] = df['{col}'].str[{start_pos}:]")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.PAD_STRING)
def pad_string(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Pad string to fixed length with specified character.

    Parameters (from plan.parameters):
        width: Target length
        side: "left", "right", or "both" (default: "left")
        fillchar: Character to pad with (default: "0")
        target_columns: Columns to pad

    Example:
        "42" with width=5, side="left", fillchar="0" -> "00042"
        "Hi" with width=6, side="both", fillchar="-" -> "--Hi--"
    """
    columns = plan.get_target_column_names()
    width = plan.parameters.get("width", 10)
    side = plan.parameters.get("side", "left")
    fillchar = plan.parameters.get("fillchar", "0")

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        if side == "left":
            result[col] = result[col].astype(str).str.rjust(width, fillchar)
            code_parts.append(f"df['{col}'] = df['{col}'].astype(str).str.rjust({width}, '{fillchar}')")
        elif side == "right":
            result[col] = result[col].astype(str).str.ljust(width, fillchar)
            code_parts.append(f"df['{col}'] = df['{col}'].astype(str).str.ljust({width}, '{fillchar}')")
        elif side == "both":
            result[col] = result[col].astype(str).str.center(width, fillchar)
            code_parts.append(f"df['{col}'] = df['{col}'].astype(str).str.center({width}, '{fillchar}')")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.CLEAN_TEXT)
def clean_text(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Remove special characters, keep only alphanumeric and spaces.

    Parameters (from plan.parameters):
        keep_spaces: Keep spaces (default: True)
        keep_punctuation: Keep basic punctuation .,!? (default: False)
        lowercase: Convert to lowercase after cleaning (default: False)
        target_columns: Columns to clean

    Example:
        "Hello, World! #123" -> "Hello World 123" (keep_spaces=True)
        "Hello, World! #123" -> "HelloWorld123" (keep_spaces=False)
    """
    columns = plan.get_target_column_names()
    keep_spaces = plan.parameters.get("keep_spaces", True)
    keep_punctuation = plan.parameters.get("keep_punctuation", False)
    lowercase = plan.parameters.get("lowercase", False)

    result = df.copy()
    code_parts = []

    for col in columns:
        if col not in result.columns:
            continue

        # Build regex pattern
        if keep_spaces and keep_punctuation:
            pattern = r'[^a-zA-Z0-9\s.,!?]'
        elif keep_spaces:
            pattern = r'[^a-zA-Z0-9\s]'
        elif keep_punctuation:
            pattern = r'[^a-zA-Z0-9.,!?]'
        else:
            pattern = r'[^a-zA-Z0-9]'

        result[col] = result[col].astype(str).str.replace(pattern, '', regex=True)

        if lowercase:
            result[col] = result[col].str.lower()
            code_parts.append(f"df['{col}'] = df['{col}'].str.replace(r'{pattern}', '', regex=True).str.lower()")
        else:
            code_parts.append(f"df['{col}'] = df['{col}'].str.replace(r'{pattern}', '', regex=True)")

    code = "\n".join(code_parts)
    return result, code


@register(TransformationType.REMOVE_HTML)
def remove_html(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    """
    Strip HTML tags from text content.

    Parameters (from plan.parameters):
        target_columns: Columns to clean
        decode_entities: Decode HTML entities like &amp; (default: True)

    Example:
        "<p>Hello <b>World</b></p>" -> "Hello World"
        "&lt;test&gt;" with decode_entities=True -> "<test>"
    """
    columns = plan.get_target_column_names()
    decode_entities = plan.parameters.get("decode_entities", True)

    result = df.copy()
    code_parts = []

    # HTML tag pattern
    html_pattern = r'<[^>]+>'

    for col in columns:
        if col not in result.columns:
            continue

        # Remove HTML tags
        result[col] = result[col].astype(str).str.replace(html_pattern, '', regex=True)

        if decode_entities:
            # Decode common HTML entities
            result[col] = (result[col]
                .str.replace('&amp;', '&', regex=False)
                .str.replace('&lt;', '<', regex=False)
                .str.replace('&gt;', '>', regex=False)
                .str.replace('&nbsp;', ' ', regex=False)
                .str.replace('&quot;', '"', regex=False)
                .str.replace('&#39;', "'", regex=False)
            )
            code_parts.append(f"df['{col}'] = df['{col}'].str.replace(r'{html_pattern}', '', regex=True)  # + decode entities")
        else:
            code_parts.append(f"df['{col}'] = df['{col}'].str.replace(r'{html_pattern}', '', regex=True)")

    code = "\n".join(code_parts)
    return result, code
