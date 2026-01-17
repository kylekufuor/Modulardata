# =============================================================================
# transforms_v2/primitives/text.py - Text Manipulation Operations
# =============================================================================
# Primitives for string manipulation: find/replace, extract, pad, etc.
# =============================================================================

from __future__ import annotations

import re
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
# find_replace
# =============================================================================


@register_primitive
class FindReplace(Primitive):
    """Find and replace text in a column."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="find_replace",
            category="text",
            description="Find and replace text values using literal strings or regex patterns",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to perform find/replace on",
                ),
                ParamDef(
                    name="find",
                    type="str",
                    required=True,
                    description="Text or pattern to find",
                ),
                ParamDef(
                    name="replace",
                    type="str",
                    required=True,
                    description="Replacement text",
                ),
                ParamDef(
                    name="use_regex",
                    type="bool",
                    required=False,
                    default=False,
                    description="Treat 'find' as a regex pattern",
                ),
                ParamDef(
                    name="case_sensitive",
                    type="bool",
                    required=False,
                    default=True,
                    description="Whether matching is case-sensitive",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Replace all occurrences of 'N/A' with 'Unknown' in the status column",
                    expected_params={"column": "status", "find": "N/A", "replace": "Unknown"},
                    description="Simple text replacement",
                ),
                TestPrompt(
                    prompt="Remove all dollar signs from the price column",
                    expected_params={"column": "price", "find": "$", "replace": ""},
                    description="Remove character",
                ),
                TestPrompt(
                    prompt="Replace any sequence of digits in notes with [REDACTED]",
                    expected_params={"column": "notes", "find": "\\d+", "replace": "[REDACTED]", "use_regex": True},
                    description="Regex replacement",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        find = params["find"]
        replace = params["replace"]
        use_regex = params.get("use_regex", False)
        case_sensitive = params.get("case_sensitive", True)

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
            flags = 0 if case_sensitive else re.IGNORECASE

            if use_regex:
                result_df[column] = result_df[column].astype(str).str.replace(
                    find, replace, regex=True, flags=flags
                )
            else:
                result_df[column] = result_df[column].astype(str).str.replace(
                    find, replace, regex=False, case=case_sensitive
                )

            # Restore NaN for originally null values
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
# extract_text
# =============================================================================


@register_primitive
class ExtractText(Primitive):
    """Extract substring from a column."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="extract_text",
            category="text",
            description="Extract a portion of text using position, pattern, or delimiter",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to extract from",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=True,
                    description="Name for the extracted text column",
                ),
                ParamDef(
                    name="method",
                    type="str",
                    required=True,
                    description="Extraction method: 'position', 'regex', 'before', 'after', 'between'",
                    choices=["position", "regex", "before", "after", "between"],
                ),
                ParamDef(
                    name="start",
                    type="int",
                    required=False,
                    default=0,
                    description="Start position (for 'position' method)",
                ),
                ParamDef(
                    name="end",
                    type="int",
                    required=False,
                    default=None,
                    description="End position (for 'position' method)",
                ),
                ParamDef(
                    name="pattern",
                    type="str",
                    required=False,
                    default=None,
                    description="Regex pattern with capture group (for 'regex' method)",
                ),
                ParamDef(
                    name="delimiter",
                    type="str",
                    required=False,
                    default=None,
                    description="Delimiter for 'before', 'after', 'between' methods",
                ),
                ParamDef(
                    name="delimiter2",
                    type="str",
                    required=False,
                    default=None,
                    description="Second delimiter for 'between' method",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Extract the first 3 characters from product_code into a new column called category_code",
                    expected_params={
                        "column": "product_code",
                        "new_column": "category_code",
                        "method": "position",
                        "start": 0,
                        "end": 3,
                    },
                    description="Extract by position",
                ),
                TestPrompt(
                    prompt="Extract the domain from email addresses into a column called email_domain",
                    expected_params={
                        "column": "email",
                        "new_column": "email_domain",
                        "method": "after",
                        "delimiter": "@",
                    },
                    description="Extract after delimiter",
                ),
                TestPrompt(
                    prompt="Extract the number from strings like 'Order #12345' into order_number",
                    expected_params={
                        "column": "order_text",
                        "new_column": "order_number",
                        "method": "regex",
                        "pattern": "#(\\d+)",
                    },
                    description="Extract with regex",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        new_column = params["new_column"]
        method = params["method"]

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if new_column in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{new_column}' already exists",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()
        col_str = result_df[column].astype(str)

        try:
            if method == "position":
                start = params.get("start", 0)
                end = params.get("end")
                result_df[new_column] = col_str.str[start:end]

            elif method == "regex":
                pattern = params.get("pattern")
                if not pattern:
                    return PrimitiveResult(
                        success=False,
                        error="Pattern required for regex extraction",
                        rows_before=rows_before,
                        cols_before=cols_before,
                    )
                extracted = col_str.str.extract(pattern, expand=False)
                result_df[new_column] = extracted

            elif method == "before":
                delimiter = params.get("delimiter")
                if not delimiter:
                    return PrimitiveResult(
                        success=False,
                        error="Delimiter required for 'before' extraction",
                        rows_before=rows_before,
                        cols_before=cols_before,
                    )
                result_df[new_column] = col_str.str.split(delimiter).str[0]

            elif method == "after":
                delimiter = params.get("delimiter")
                if not delimiter:
                    return PrimitiveResult(
                        success=False,
                        error="Delimiter required for 'after' extraction",
                        rows_before=rows_before,
                        cols_before=cols_before,
                    )
                result_df[new_column] = col_str.str.split(delimiter).str[-1]

            elif method == "between":
                delimiter = params.get("delimiter")
                delimiter2 = params.get("delimiter2")
                if not delimiter or not delimiter2:
                    return PrimitiveResult(
                        success=False,
                        error="Both delimiters required for 'between' extraction",
                        rows_before=rows_before,
                        cols_before=cols_before,
                    )
                # Extract text between two delimiters
                pattern = f"{re.escape(delimiter)}(.*?){re.escape(delimiter2)}"
                extracted = col_str.str.extract(pattern, expand=False)
                result_df[new_column] = extracted

            # Handle 'nan' strings from null values
            result_df.loc[df[column].isna(), new_column] = None

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
# text_length
# =============================================================================


@register_primitive
class TextLength(Primitive):
    """Calculate the length of text values."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="text_length",
            category="text",
            description="Get the character count of text values in a column",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to measure",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=True,
                    description="Name for the length column",
                ),
                ParamDef(
                    name="count_spaces",
                    type="bool",
                    required=False,
                    default=True,
                    description="Whether to include spaces in the count",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Add a column showing the length of each description",
                    expected_params={"column": "description", "new_column": "description_length"},
                    description="Basic length calculation",
                ),
                TestPrompt(
                    prompt="Create a character count column for the name field called name_chars",
                    expected_params={"column": "name", "new_column": "name_chars"},
                    description="Character count",
                ),
                TestPrompt(
                    prompt="Calculate the length of comments without counting spaces",
                    expected_params={"column": "comments", "new_column": "comments_length", "count_spaces": False},
                    description="Length without spaces",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        new_column = params["new_column"]
        count_spaces = params.get("count_spaces", True)

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if new_column in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{new_column}' already exists",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()

        try:
            if count_spaces:
                result_df[new_column] = result_df[column].astype(str).str.len()
            else:
                result_df[new_column] = result_df[column].astype(str).str.replace(" ", "").str.len()

            # Handle null values
            result_df.loc[df[column].isna(), new_column] = None

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
# pad_text
# =============================================================================


@register_primitive
class PadText(Primitive):
    """Pad text to a fixed length."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="pad_text",
            category="text",
            description="Add leading or trailing characters to reach a fixed length",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to pad",
                ),
                ParamDef(
                    name="length",
                    type="int",
                    required=True,
                    description="Target length after padding",
                ),
                ParamDef(
                    name="pad_char",
                    type="str",
                    required=False,
                    default="0",
                    description="Character to use for padding",
                ),
                ParamDef(
                    name="side",
                    type="str",
                    required=False,
                    default="left",
                    description="Where to add padding: 'left' or 'right'",
                    choices=["left", "right"],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Zero-pad the employee_id column to 6 digits",
                    expected_params={"column": "employee_id", "length": 6, "pad_char": "0", "side": "left"},
                    description="Zero-pad IDs",
                ),
                TestPrompt(
                    prompt="Pad product codes with leading zeros to 10 characters",
                    expected_params={"column": "product_code", "length": 10, "pad_char": "0", "side": "left"},
                    description="Pad product codes",
                ),
                TestPrompt(
                    prompt="Add trailing spaces to name column to make all values 20 characters",
                    expected_params={"column": "name", "length": 20, "pad_char": " ", "side": "right"},
                    description="Right-pad with spaces",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        length = params["length"]
        pad_char = params.get("pad_char", "0")
        side = params.get("side", "left")

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if len(pad_char) != 1:
            return PrimitiveResult(
                success=False,
                error="Pad character must be a single character",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()

        try:
            if side == "left":
                result_df[column] = result_df[column].astype(str).str.rjust(length, pad_char)
            else:
                result_df[column] = result_df[column].astype(str).str.ljust(length, pad_char)

            # Handle null values
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
# remove_characters
# =============================================================================


@register_primitive
class RemoveCharacters(Primitive):
    """Remove specific characters from text."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="remove_characters",
            category="text",
            description="Strip specific characters or character types from text",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to clean",
                ),
                ParamDef(
                    name="characters",
                    type="str",
                    required=False,
                    default=None,
                    description="Specific characters to remove (e.g., '$,.')",
                ),
                ParamDef(
                    name="remove_type",
                    type="str",
                    required=False,
                    default=None,
                    description="Type to remove: 'digits', 'letters', 'punctuation', 'whitespace'",
                    choices=["digits", "letters", "punctuation", "whitespace", None],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Remove all dollar signs and commas from the amount column",
                    expected_params={"column": "amount", "characters": "$,"},
                    description="Remove currency symbols",
                ),
                TestPrompt(
                    prompt="Strip all digits from the reference column",
                    expected_params={"column": "reference", "remove_type": "digits"},
                    description="Remove all numbers",
                ),
                TestPrompt(
                    prompt="Remove punctuation from the comments field",
                    expected_params={"column": "comments", "remove_type": "punctuation"},
                    description="Remove punctuation",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        characters = params.get("characters")
        remove_type = params.get("remove_type")

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if not characters and not remove_type:
            return PrimitiveResult(
                success=False,
                error="Either 'characters' or 'remove_type' must be specified",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()

        try:
            col_str = result_df[column].astype(str)

            if characters:
                # Remove specific characters
                pattern = "[" + re.escape(characters) + "]"
                result_df[column] = col_str.str.replace(pattern, "", regex=True)

            elif remove_type == "digits":
                result_df[column] = col_str.str.replace(r"\d", "", regex=True)

            elif remove_type == "letters":
                result_df[column] = col_str.str.replace(r"[a-zA-Z]", "", regex=True)

            elif remove_type == "punctuation":
                import string
                pattern = "[" + re.escape(string.punctuation) + "]"
                result_df[column] = col_str.str.replace(pattern, "", regex=True)

            elif remove_type == "whitespace":
                result_df[column] = col_str.str.replace(r"\s", "", regex=True)

            # Handle null values
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
# string_contains
# =============================================================================


@register_primitive
class StringContains(Primitive):
    """Check if text contains a substring."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="string_contains",
            category="text",
            description="Create a boolean column indicating if text contains a substring",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to search in",
                ),
                ParamDef(
                    name="substring",
                    type="str",
                    required=True,
                    description="Substring to search for",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for result column (default: column_contains)",
                ),
                ParamDef(
                    name="case_sensitive",
                    type="bool",
                    required=False,
                    default=False,
                    description="Case-sensitive search (default: False)",
                ),
                ParamDef(
                    name="regex",
                    type="bool",
                    required=False,
                    default=False,
                    description="Treat substring as regex pattern",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Check if the email column contains 'gmail'",
                    expected_params={
                        "column": "email",
                        "substring": "gmail",
                    },
                    description="Basic contains check",
                ),
                TestPrompt(
                    prompt="Flag names that contain 'Jr' or 'Sr' as has_suffix",
                    expected_params={
                        "column": "name",
                        "substring": "Jr|Sr",
                        "new_column": "has_suffix",
                        "regex": True,
                    },
                    description="Regex contains",
                ),
                TestPrompt(
                    prompt="Check if description contains 'ERROR' (case-sensitive)",
                    expected_params={
                        "column": "description",
                        "substring": "ERROR",
                        "case_sensitive": True,
                    },
                    description="Case-sensitive contains",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        substring = params["substring"]
        new_column = params.get("new_column") or f"{column}_contains"
        case_sensitive = params.get("case_sensitive", False)
        regex = params.get("regex", False)

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

            # Apply contains check
            result_df[new_column] = result_df[column].astype(str).str.contains(
                substring,
                case=case_sensitive,
                regex=regex,
                na=False
            )

            # Count matches
            match_count = int(result_df[new_column].sum())

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "match_count": match_count,
                    "no_match_count": rows_before - match_count,
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
# substring
# =============================================================================


@register_primitive
class Substring(Primitive):
    """Extract a portion of text by position."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="substring",
            category="text",
            description="Extract characters from a string by start position and length",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to extract from",
                ),
                ParamDef(
                    name="start",
                    type="int",
                    required=False,
                    default=0,
                    description="Starting position (0-based, default: 0)",
                ),
                ParamDef(
                    name="length",
                    type="int",
                    required=False,
                    default=None,
                    description="Number of characters to extract (default: rest of string)",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for result column (default: overwrites original)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Get the first 3 characters from the code column",
                    expected_params={
                        "column": "code",
                        "start": 0,
                        "length": 3,
                    },
                    description="First N characters",
                ),
                TestPrompt(
                    prompt="Extract characters 5-10 from the id column into short_id",
                    expected_params={
                        "column": "id",
                        "start": 4,
                        "length": 6,
                        "new_column": "short_id",
                    },
                    description="Middle portion",
                ),
                TestPrompt(
                    prompt="Get the last 4 characters of the phone column (skip first 6)",
                    expected_params={
                        "column": "phone",
                        "start": 6,
                    },
                    description="From position to end",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        start = params.get("start", 0)
        length = params.get("length")
        new_column = params.get("new_column") or column

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

            # Apply substring extraction
            col_str = result_df[column].astype(str)

            if length is not None:
                result_df[new_column] = col_str.str[start:start + length]
            else:
                result_df[new_column] = col_str.str[start:]

            # Handle original nulls
            result_df.loc[df[column].isna(), new_column] = None

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"start": start, "length": length},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# regex_replace
# =============================================================================


@register_primitive
class RegexReplace(Primitive):
    """Transform text using regex patterns with capture groups."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="regex_replace",
            category="text",
            description="Transform text using regex patterns with capture group backreferences",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to transform",
                ),
                ParamDef(
                    name="pattern",
                    type="str",
                    required=True,
                    description="Regex pattern with capture groups, e.g., '^(.{3})(.{4})(.*)$'",
                ),
                ParamDef(
                    name="replacement",
                    type="str",
                    required=True,
                    description="Replacement string with backreferences, e.g., '\\1-\\2-XX' or '$1-$2-XX'",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for result column (default: overwrites original)",
                ),
                ParamDef(
                    name="case_sensitive",
                    type="bool",
                    required=False,
                    default=True,
                    description="Case-sensitive matching",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Transform ABCDHED to ABC-DHED-XX format (insert dashes after 3rd and 7th chars, add XX)",
                    expected_params={
                        "column": "code",
                        "pattern": "^(.{3})(.{4})(.*)$",
                        "replacement": "\\1-\\2-XX",
                    },
                    description="Reformat string with inserted characters",
                ),
                TestPrompt(
                    prompt="Convert phone numbers from 1234567890 to (123) 456-7890 format",
                    expected_params={
                        "column": "phone",
                        "pattern": "^(\\d{3})(\\d{3})(\\d{4})$",
                        "replacement": "(\\1) \\2-\\3",
                    },
                    description="Format phone numbers",
                ),
                TestPrompt(
                    prompt="Swap first and last name in 'LastName, FirstName' format to 'FirstName LastName'",
                    expected_params={
                        "column": "name",
                        "pattern": "^([^,]+),\\s*(.+)$",
                        "replacement": "\\2 \\1",
                    },
                    description="Swap name parts",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        pattern = params["pattern"]
        replacement = params["replacement"]
        new_column = params.get("new_column") or column
        case_sensitive = params.get("case_sensitive", True)

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

            # Normalize replacement string: convert $1, $2 to \1, \2
            normalized_replacement = re.sub(r'\$(\d+)', r'\\\1', replacement)

            # Compile regex with flags
            flags = 0 if case_sensitive else re.IGNORECASE

            # Apply regex replacement
            result_df[new_column] = result_df[column].astype(str).str.replace(
                pattern,
                normalized_replacement,
                regex=True,
                flags=flags
            )

            # Handle original nulls
            result_df.loc[df[column].isna(), new_column] = None

            # Count successful transformations (where pattern matched)
            original_values = df[column].astype(str)
            new_values = result_df[new_column].astype(str)
            changed_count = int((original_values != new_values).sum())

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "transformed_count": changed_count,
                    "unchanged_count": rows_before - changed_count,
                },
            )
        except re.error as e:
            return PrimitiveResult(
                success=False,
                error=f"Invalid regex pattern: {e}",
                rows_before=rows_before,
                cols_before=cols_before,
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# regex_extract
# =============================================================================


@register_primitive
class RegexExtract(Primitive):
    """Extract text matching a regex pattern into a new column."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="regex_extract",
            category="text",
            description="Extract text matching a regex pattern (with optional capture group) into a new column",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to extract from",
                ),
                ParamDef(
                    name="pattern",
                    type="str",
                    required=True,
                    description="Regex pattern (use capture group to extract specific part)",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=True,
                    description="Name for the extracted value column",
                ),
                ParamDef(
                    name="group",
                    type="int",
                    required=False,
                    default=0,
                    description="Capture group to extract (0 = entire match, 1+ = specific group)",
                ),
                ParamDef(
                    name="case_sensitive",
                    type="bool",
                    required=False,
                    default=True,
                    description="Case-sensitive matching",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Extract all numbers from the text column into a numbers column",
                    expected_params={
                        "column": "text",
                        "pattern": "\\d+",
                        "new_column": "numbers",
                    },
                    description="Extract numbers",
                ),
                TestPrompt(
                    prompt="Extract email addresses from the notes column",
                    expected_params={
                        "column": "notes",
                        "pattern": "[\\w.-]+@[\\w.-]+\\.\\w+",
                        "new_column": "extracted_email",
                    },
                    description="Extract emails",
                ),
                TestPrompt(
                    prompt="Extract the domain from URLs in the link column",
                    expected_params={
                        "column": "link",
                        "pattern": "https?://([^/]+)",
                        "new_column": "domain",
                        "group": 1,
                    },
                    description="Extract URL domain",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        pattern = params["pattern"]
        new_column = params["new_column"]
        group = params.get("group", 0)
        case_sensitive = params.get("case_sensitive", True)

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if new_column in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{new_column}' already exists",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.copy()

            # Compile regex with flags
            flags = 0 if case_sensitive else re.IGNORECASE
            compiled_pattern = re.compile(pattern, flags)

            def extract_match(value):
                if pd.isna(value):
                    return None
                match = compiled_pattern.search(str(value))
                if match:
                    if group == 0:
                        return match.group(0)
                    elif group <= len(match.groups()):
                        return match.group(group)
                return None

            result_df[new_column] = result_df[column].apply(extract_match)

            # Count successful extractions
            extracted_count = int(result_df[new_column].notna().sum())

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "extracted_count": extracted_count,
                    "no_match_count": rows_before - extracted_count,
                },
            )
        except re.error as e:
            return PrimitiveResult(
                success=False,
                error=f"Invalid regex pattern: {e}",
                rows_before=rows_before,
                cols_before=cols_before,
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )
