# =============================================================================
# lib/profiler.py - Enhanced Data Profiling Engine
# =============================================================================
# This module provides comprehensive CSV analysis to give AI agents deep
# understanding of data. Enhanced features include:
#
#   - Semantic type detection (email, phone, URL, date, currency, etc.)
#   - Data quality issue detection with severity levels
#   - Statistical analysis (mean, median, std, outliers)
#   - Pattern recognition (date formats, phone formats)
#   - Value distribution analysis for categorical columns
#   - Whitespace and formatting issue detection
#
# The output is optimized for LLM context windows while being comprehensive
# enough for the AI to make intelligent data transformation decisions.
# =============================================================================

import io
import logging
import re
from pathlib import Path
from typing import Any, BinaryIO, Union

import numpy as np
import pandas as pd

from core.models import (
    ColumnProfile,
    ColumnStatistics,
    DataIssue,
    DataProfile,
    IssueSeverity,
    IssueType,
    SemanticType,
    ValueDistribution,
    # Schema Matching Models
    ColumnContract,
    ColumnMapping,
    ContractMatch,
    MatchConfidence,
    SchemaContract,
    SchemaDiscrepancy,
    SchemaMatch,
)

# Set up logging for this module
logger = logging.getLogger(__name__)

# Type alias for file inputs
FileInput = Union[str, Path, BinaryIO, io.BytesIO, io.StringIO]


# =============================================================================
# Constants
# =============================================================================

MAX_ROWS_FOR_PROFILE = 100_000
DEFAULT_SAMPLE_COUNT = 5
DEFAULT_SAMPLE_ROWS = 3
ENCODINGS_TO_TRY = ["utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]
DELIMITERS_TO_TRY = [",", ";", "\t", "|"]

# Regex patterns for semantic type detection
PATTERNS = {
    "email": re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
    "url": re.compile(r'^https?://[^\s]+$'),
    "phone_us": re.compile(r'^[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}$'),
    "phone_intl": re.compile(r'^\+?[1-9]\d{1,14}$'),
    "zipcode_us": re.compile(r'^\d{5}(-\d{4})?$'),
    "date_iso": re.compile(r'^\d{4}-\d{2}-\d{2}$'),
    "date_us": re.compile(r'^\d{1,2}/\d{1,2}/\d{2,4}$'),
    "date_eu": re.compile(r'^\d{1,2}\.\d{1,2}\.\d{2,4}$'),
    "datetime_iso": re.compile(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}'),
    "time": re.compile(r'^\d{1,2}:\d{2}(:\d{2})?(\s?[AP]M)?$', re.IGNORECASE),
    "currency": re.compile(r'^[$€£¥]\s?[\d,]+\.?\d*$|^[\d,]+\.?\d*\s?[$€£¥]$'),
    "percentage": re.compile(r'^-?\d+\.?\d*\s?%$'),
    "boolean": re.compile(r'^(true|false|yes|no|y|n|1|0)$', re.IGNORECASE),
}

# Common boolean values
BOOLEAN_VALUES = {'true', 'false', 'yes', 'no', 'y', 'n', '1', '0', 't', 'f'}


# =============================================================================
# CSV Reading Functions (unchanged from before)
# =============================================================================

def detect_encoding(file_path: Union[str, Path]) -> str:
    """Detect the encoding of a file by trying common encodings."""
    file_path = Path(file_path)
    for encoding in ENCODINGS_TO_TRY:
        try:
            with open(file_path, "r", encoding=encoding) as f:
                f.read(8192)
            logger.debug(f"Detected encoding: {encoding}")
            return encoding
        except (UnicodeDecodeError, UnicodeError):
            continue
    raise ValueError(f"Could not detect encoding for {file_path}")


def detect_delimiter(file_path: Union[str, Path], encoding: str = "utf-8") -> str:
    """Detect the delimiter used in a CSV file."""
    file_path = Path(file_path)
    with open(file_path, "r", encoding=encoding) as f:
        sample_lines = [f.readline() for _ in range(5)]
    sample_lines = [line for line in sample_lines if line.strip()]
    if not sample_lines:
        return ","

    best_delimiter = ","
    best_score = 0
    for delimiter in DELIMITERS_TO_TRY:
        counts = [len(line.split(delimiter)) for line in sample_lines]
        if not counts:
            continue
        avg_cols = sum(counts) / len(counts)
        consistency = 1 - (max(counts) - min(counts)) / max(max(counts), 1)
        score = avg_cols * consistency
        if score > best_score and avg_cols > 1:
            best_score = score
            best_delimiter = delimiter

    logger.debug(f"Detected delimiter: {repr(best_delimiter)}")
    return best_delimiter


def read_csv_safe(
    source: FileInput,
    max_rows: int | None = MAX_ROWS_FOR_PROFILE,
    encoding: str | None = None,
    delimiter: str | None = None,
    **kwargs
) -> tuple[pd.DataFrame, str | None, str | None]:
    """
    Safely read a CSV file with automatic encoding and delimiter detection.

    Returns:
        Tuple of (DataFrame, detected_encoding, detected_delimiter)
    """
    # Handle file-like objects
    if hasattr(source, "read"):
        logger.debug("Reading from file-like object")
        df = _read_csv_from_buffer(source, max_rows, **kwargs)
        return df, None, None

    file_path = Path(source)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    if not file_path.is_file():
        raise ValueError(f"Not a file: {file_path}")

    # Detect encoding
    if encoding is None:
        try:
            encoding = detect_encoding(file_path)
        except ValueError:
            encoding = "utf-8"

    # Detect delimiter
    if delimiter is None:
        try:
            delimiter = detect_delimiter(file_path, encoding)
        except Exception:
            delimiter = ","

    try:
        df = pd.read_csv(
            file_path,
            encoding=encoding,
            delimiter=delimiter,
            nrows=max_rows,
            on_bad_lines="warn",
            **kwargs
        )
        logger.info(f"Read CSV: {len(df)} rows × {len(df.columns)} columns")
        return df, encoding, delimiter

    except pd.errors.EmptyDataError:
        return pd.DataFrame(), encoding, delimiter
    except Exception as e:
        raise ValueError(f"Failed to read CSV: {e}") from e


def _read_csv_from_buffer(buffer: BinaryIO, max_rows: int | None, **kwargs) -> pd.DataFrame:
    """Read CSV from a file-like object."""
    for encoding in ENCODINGS_TO_TRY:
        try:
            if hasattr(buffer, "seek"):
                buffer.seek(0)
            return pd.read_csv(buffer, encoding=encoding, nrows=max_rows, on_bad_lines="warn", **kwargs)
        except (UnicodeDecodeError, UnicodeError):
            continue
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
    raise ValueError("Could not read CSV from buffer")


# =============================================================================
# Semantic Type Detection
# =============================================================================

def detect_semantic_type(series: pd.Series, col_name: str) -> tuple[SemanticType, str | None]:
    """
    Detect the semantic meaning of a column's data.

    Examines sample values to determine if the column contains:
    - Emails, URLs, phone numbers
    - Dates, times, datetimes
    - Currency, percentages
    - IDs, categories, etc.

    Returns:
        Tuple of (SemanticType, detected_pattern or None)
    """
    # Get non-null string values for pattern matching
    non_null = series.dropna()
    if len(non_null) == 0:
        return SemanticType.UNKNOWN, None

    # Convert to strings for pattern matching
    sample = non_null.head(100).astype(str)

    # Check column name hints first
    col_lower = col_name.lower()

    # ID detection by name
    if col_lower in ('id', 'uuid', 'guid') or col_lower.endswith('_id') or col_lower.endswith('id'):
        if series.nunique() == len(non_null):  # All unique
            return SemanticType.ID, None

    # Email detection by name or pattern
    if 'email' in col_lower or 'e-mail' in col_lower:
        return SemanticType.EMAIL, "email@domain.com"

    # Check patterns on sample values
    pattern_matches = _check_patterns(sample)

    if pattern_matches:
        # Return the most common pattern match
        best_match = max(pattern_matches.items(), key=lambda x: x[1])
        pattern_name, match_count = best_match

        # Only accept if majority match
        if match_count >= len(sample) * 0.7:
            return _pattern_to_semantic_type(pattern_name)

    # Check for boolean values
    if _is_boolean_column(sample):
        return SemanticType.BOOLEAN, "true/false"

    # Check if numeric column by dtype
    if pd.api.types.is_numeric_dtype(series):
        return SemanticType.NUMERIC, None

    # Check cardinality for category detection
    unique_ratio = series.nunique() / len(non_null) if len(non_null) > 0 else 0
    if unique_ratio < 0.05 and series.nunique() < 20:  # Low cardinality
        return SemanticType.CATEGORY, None

    # Check for name-like columns
    if any(hint in col_lower for hint in ['name', 'first', 'last', 'customer', 'user']):
        return SemanticType.NAME, None

    return SemanticType.UNKNOWN, None


def _check_patterns(sample: pd.Series) -> dict[str, int]:
    """Check how many values match each pattern."""
    matches = {}
    for pattern_name, pattern in PATTERNS.items():
        count = sum(1 for val in sample if pattern.match(str(val).strip()))
        if count > 0:
            matches[pattern_name] = count
    return matches


def _pattern_to_semantic_type(pattern_name: str) -> tuple[SemanticType, str | None]:
    """Convert pattern name to SemanticType."""
    mapping = {
        "email": (SemanticType.EMAIL, "email@domain.com"),
        "url": (SemanticType.URL, "https://..."),
        "phone_us": (SemanticType.PHONE, "(XXX) XXX-XXXX"),
        "phone_intl": (SemanticType.PHONE, "+X XXX XXX XXXX"),
        "zipcode_us": (SemanticType.ZIPCODE, "XXXXX or XXXXX-XXXX"),
        "date_iso": (SemanticType.DATE, "YYYY-MM-DD"),
        "date_us": (SemanticType.DATE, "MM/DD/YYYY"),
        "date_eu": (SemanticType.DATE, "DD.MM.YYYY"),
        "datetime_iso": (SemanticType.DATETIME, "YYYY-MM-DD HH:MM:SS"),
        "time": (SemanticType.TIME, "HH:MM:SS"),
        "currency": (SemanticType.CURRENCY, "$X,XXX.XX"),
        "percentage": (SemanticType.PERCENTAGE, "XX.X%"),
        "boolean": (SemanticType.BOOLEAN, "true/false"),
    }
    return mapping.get(pattern_name, (SemanticType.UNKNOWN, None))


def _is_boolean_column(sample: pd.Series) -> bool:
    """Check if column contains boolean-like values."""
    unique_lower = set(str(v).lower().strip() for v in sample.unique())
    return unique_lower.issubset(BOOLEAN_VALUES)


# =============================================================================
# Statistical Analysis
# =============================================================================

def compute_statistics(series: pd.Series) -> ColumnStatistics | None:
    """
    Compute statistical measures for a numeric column.

    Includes: mean, median, std, quartiles, skewness, and outlier detection.
    """
    if not pd.api.types.is_numeric_dtype(series):
        return None

    non_null = series.dropna()
    if len(non_null) < 2:
        return None

    try:
        mean = float(non_null.mean())
        median = float(non_null.median())
        std = float(non_null.std())
        q1 = float(non_null.quantile(0.25))
        q3 = float(non_null.quantile(0.75))

        # Skewness (measure of asymmetry)
        try:
            skewness = float(non_null.skew())
        except Exception:
            skewness = None

        # Outlier detection using IQR method
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers = non_null[(non_null < lower_bound) | (non_null > upper_bound)]
        outlier_count = len(outliers)

        return ColumnStatistics(
            mean=round(mean, 4),
            median=round(median, 4),
            std=round(std, 4),
            q1=round(q1, 4),
            q3=round(q3, 4),
            skewness=round(skewness, 4) if skewness is not None else None,
            outlier_count=outlier_count,
            outlier_bounds=(round(lower_bound, 4), round(upper_bound, 4)) if iqr > 0 else None,
        )
    except Exception as e:
        logger.warning(f"Error computing statistics: {e}")
        return None


# =============================================================================
# Value Distribution Analysis
# =============================================================================

def compute_distribution(series: pd.Series, total_count: int) -> ValueDistribution | None:
    """
    Compute value distribution for categorical columns.

    Shows top values with counts and percentages.
    """
    non_null = series.dropna()
    unique_count = series.nunique()

    # Determine cardinality
    if unique_count <= 10:
        cardinality = "low"
        is_categorical = True
    elif unique_count <= 100:
        cardinality = "medium"
        is_categorical = unique_count / len(non_null) < 0.5 if len(non_null) > 0 else False
    else:
        cardinality = "high"
        is_categorical = False

    # Get top values
    value_counts = series.value_counts().head(10)
    top_values = []
    for value, count in value_counts.items():
        percent = (count / total_count * 100) if total_count > 0 else 0
        top_values.append({
            "value": _safe_value(value),
            "count": int(count),
            "percent": round(percent, 1),
        })

    return ValueDistribution(
        top_values=top_values,
        is_categorical=is_categorical,
        cardinality=cardinality,
    )


# =============================================================================
# Data Quality Issue Detection
# =============================================================================

def detect_column_issues(
    series: pd.Series,
    col_name: str,
    total_rows: int,
    semantic_type: SemanticType,
) -> list[DataIssue]:
    """
    Detect data quality issues in a single column.
    """
    issues = []
    non_null = series.dropna()

    # ----- Missing Values -----
    null_count = series.isna().sum()
    null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0

    if null_pct >= 50:
        issues.append(DataIssue(
            issue_type=IssueType.HIGH_NULL_RATE,
            severity=IssueSeverity.CRITICAL,
            column=col_name,
            description=f"{null_pct:.0f}% of values are missing ({null_count:,} rows)",
            affected_count=null_count,
            affected_percent=null_pct,
            suggestion=f"Consider dropping column '{col_name}' or imputing values",
        ))
    elif null_pct >= 20:
        issues.append(DataIssue(
            issue_type=IssueType.MISSING_VALUES,
            severity=IssueSeverity.WARNING,
            column=col_name,
            description=f"{null_pct:.0f}% of values are missing ({null_count:,} rows)",
            affected_count=null_count,
            affected_percent=null_pct,
            suggestion=f"Review and fill missing values in '{col_name}'",
        ))

    # ----- String-specific issues -----
    if series.dtype == 'object':
        str_series = non_null.astype(str)

        # Empty strings
        empty_count = (str_series == '').sum()
        if empty_count > 0:
            issues.append(DataIssue(
                issue_type=IssueType.EMPTY_STRINGS,
                severity=IssueSeverity.WARNING,
                column=col_name,
                description=f"{empty_count:,} empty strings (different from null)",
                affected_count=empty_count,
                suggestion=f"Convert empty strings to null in '{col_name}'",
            ))

        # Whitespace issues
        whitespace_count = ((str_series != str_series.str.strip()) & (str_series != '')).sum()
        if whitespace_count > 0:
            examples = str_series[str_series != str_series.str.strip()].head(3).tolist()
            issues.append(DataIssue(
                issue_type=IssueType.WHITESPACE,
                severity=IssueSeverity.WARNING,
                column=col_name,
                description=f"{whitespace_count:,} values have leading/trailing whitespace",
                affected_count=whitespace_count,
                suggestion=f"Trim whitespace from '{col_name}'",
                examples=[repr(e) for e in examples],
            ))

        # Inconsistent casing
        if semantic_type in (SemanticType.CATEGORY, SemanticType.NAME, SemanticType.UNKNOWN):
            casing_issue = _detect_casing_issues(str_series, col_name)
            if casing_issue:
                issues.append(casing_issue)

    # ----- Numeric outliers -----
    if pd.api.types.is_numeric_dtype(series) and len(non_null) > 10:
        outlier_issue = _detect_outliers(non_null, col_name, total_rows)
        if outlier_issue:
            issues.append(outlier_issue)

    # ----- Invalid values for semantic type -----
    if semantic_type == SemanticType.EMAIL:
        invalid = _count_invalid_emails(non_null)
        if invalid > 0:
            issues.append(DataIssue(
                issue_type=IssueType.INVALID_VALUES,
                severity=IssueSeverity.WARNING,
                column=col_name,
                description=f"{invalid:,} values don't match email format",
                affected_count=invalid,
                suggestion=f"Validate email addresses in '{col_name}'",
            ))

    return issues


def _detect_casing_issues(str_series: pd.Series, col_name: str) -> DataIssue | None:
    """Detect inconsistent casing in string values."""
    # Group values that differ only by case
    lower_values = str_series.str.lower()
    unique_lower = lower_values.nunique()
    unique_original = str_series.nunique()

    if unique_original > unique_lower:
        # Find examples of case inconsistency
        value_counts = lower_values.value_counts()
        inconsistent_examples = []

        for lower_val, count in value_counts.head(5).items():
            original_variants = str_series[lower_values == lower_val].unique()
            if len(original_variants) > 1:
                inconsistent_examples.append(list(original_variants[:3]))

        if inconsistent_examples:
            return DataIssue(
                issue_type=IssueType.INCONSISTENT_CASING,
                severity=IssueSeverity.WARNING,
                column=col_name,
                description=f"Inconsistent casing: {unique_original} unique values but {unique_lower} when case-insensitive",
                affected_count=unique_original - unique_lower,
                suggestion=f"Standardize casing in '{col_name}' (e.g., all lowercase or title case)",
                examples=inconsistent_examples[:3],
            )
    return None


def _detect_outliers(series: pd.Series, col_name: str, total_rows: int) -> DataIssue | None:
    """Detect outliers using IQR method."""
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1

    if iqr == 0:
        return None

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    outliers = series[(series < lower_bound) | (series > upper_bound)]
    outlier_count = len(outliers)

    if outlier_count > 0:
        outlier_pct = (outlier_count / total_rows * 100) if total_rows > 0 else 0
        severity = IssueSeverity.CRITICAL if outlier_pct > 10 else IssueSeverity.INFO

        return DataIssue(
            issue_type=IssueType.OUTLIERS,
            severity=severity,
            column=col_name,
            description=f"{outlier_count:,} outliers detected ({outlier_pct:.1f}% of data)",
            affected_count=outlier_count,
            affected_percent=outlier_pct,
            suggestion=f"Review outliers in '{col_name}' - values outside [{lower_bound:.2f}, {upper_bound:.2f}]",
            examples=[_safe_value(v) for v in outliers.head(5).tolist()],
        )
    return None


def _count_invalid_emails(series: pd.Series) -> int:
    """Count values that don't match email format."""
    email_pattern = PATTERNS["email"]
    invalid_count = 0
    for val in series:
        if not email_pattern.match(str(val).strip()):
            invalid_count += 1
    return invalid_count


def detect_dataset_issues(df: pd.DataFrame) -> list[DataIssue]:
    """Detect dataset-level quality issues."""
    issues = []

    # Duplicate rows
    duplicate_count = len(df) - len(df.drop_duplicates())
    if duplicate_count > 0:
        dup_pct = (duplicate_count / len(df) * 100) if len(df) > 0 else 0
        severity = IssueSeverity.CRITICAL if dup_pct > 20 else IssueSeverity.WARNING

        issues.append(DataIssue(
            issue_type=IssueType.DUPLICATES,
            severity=severity,
            column=None,
            description=f"{duplicate_count:,} duplicate rows ({dup_pct:.1f}% of dataset)",
            affected_count=duplicate_count,
            affected_percent=dup_pct,
            suggestion="Remove duplicate rows with df.drop_duplicates()",
        ))

    return issues


# =============================================================================
# Column Analysis (Enhanced)
# =============================================================================

def analyze_column(
    series: pd.Series,
    total_rows: int,
    sample_count: int = DEFAULT_SAMPLE_COUNT,
) -> ColumnProfile:
    """
    Comprehensive analysis of a single column.

    Includes:
    - Basic stats (type, nulls, unique count)
    - Semantic type detection
    - Statistics for numeric columns
    - Value distribution for categorical columns
    - Data quality issues
    """
    col_name = str(series.name)

    # Basic stats
    null_count = int(series.isna().sum())
    null_percent = (null_count / total_rows * 100) if total_rows > 0 else 0.0
    unique_count = int(series.nunique())
    dtype = str(series.dtype)

    # Check for empty strings and whitespace
    empty_string_count = 0
    whitespace_only_count = 0

    if series.dtype == 'object':
        non_null = series.dropna().astype(str)
        empty_string_count = int((non_null == '').sum())
        whitespace_only_count = int(((non_null.str.strip() == '') & (non_null != '')).sum())

    # Semantic type detection
    semantic_type, detected_pattern = detect_semantic_type(series, col_name)

    # Is this a unique ID column?
    non_null_count = total_rows - null_count
    is_unique = (unique_count == non_null_count) and (non_null_count > 1)

    # Sample values
    sample_values = _extract_sample_values(series.dropna(), sample_count)

    # Min/max for numeric
    min_value = None
    max_value = None
    if pd.api.types.is_numeric_dtype(series):
        non_null = series.dropna()
        if len(non_null) > 0:
            min_value = _safe_value(non_null.min())
            max_value = _safe_value(non_null.max())

    # Statistics for numeric columns
    statistics = compute_statistics(series)

    # Distribution for categorical columns
    distribution = None
    if semantic_type == SemanticType.CATEGORY or (series.dtype == 'object' and unique_count < 50):
        distribution = compute_distribution(series, total_rows)

    # Detect issues
    issues = detect_column_issues(series, col_name, total_rows, semantic_type)

    return ColumnProfile(
        name=col_name,
        dtype=dtype,
        semantic_type=semantic_type,
        detected_pattern=detected_pattern,
        null_count=null_count,
        null_percent=round(null_percent, 2),
        empty_string_count=empty_string_count,
        whitespace_only_count=whitespace_only_count,
        unique_count=unique_count,
        is_unique=is_unique,
        sample_values=sample_values,
        min_value=min_value,
        max_value=max_value,
        statistics=statistics,
        distribution=distribution,
        issues=issues,
    )


def _extract_sample_values(series: pd.Series, count: int) -> list[Any]:
    """Extract sample values as JSON-safe types."""
    samples = []
    for value in series.head(count):
        safe = _safe_value(value)
        if safe is not None:
            samples.append(safe)
    return samples


def _safe_value(value: Any) -> Any:
    """Convert to JSON-safe Python type."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


# =============================================================================
# Header & Footer Detection
# =============================================================================

def detect_header_row(
    source: FileInput,
    max_rows_to_scan: int = 10,
    encoding: str | None = None,
) -> tuple[int, float]:
    """
    Detect which row contains the actual column headers.

    Scans the first N rows to find the most likely header row based on:
    - Row with unique string values (headers are typically unique text)
    - Row where data below has consistent types
    - Row that doesn't look like data (not numeric, not dates)

    Args:
        source: File path or file-like object
        max_rows_to_scan: How many rows to analyze
        encoding: File encoding (auto-detected if None)

    Returns:
        Tuple of (header_row_index, confidence_score)
        - header_row_index: 0-indexed row number
        - confidence_score: 0.0-1.0 confidence in detection
    """
    try:
        # Read raw rows without assuming header
        if hasattr(source, "read"):
            source.seek(0)
            raw_df = pd.read_csv(source, header=None, nrows=max_rows_to_scan, on_bad_lines="skip")
            source.seek(0)
        else:
            raw_df = pd.read_csv(source, header=None, nrows=max_rows_to_scan, on_bad_lines="skip")

        if raw_df.empty or len(raw_df) < 2:
            return 0, 1.0  # Default to first row

        best_row = 0
        best_score = 0.0

        for row_idx in range(min(max_rows_to_scan, len(raw_df))):
            score = _score_header_row(raw_df, row_idx)
            if score > best_score:
                best_score = score
                best_row = row_idx

        # Confidence is based on how clearly one row stands out
        confidence = min(1.0, best_score / 0.8) if best_score > 0 else 0.5

        logger.debug(f"Header detection: row {best_row} with confidence {confidence:.2f}")
        return best_row, confidence

    except Exception as e:
        logger.warning(f"Header detection failed: {e}")
        return 0, 0.5  # Default to first row with low confidence


def _score_header_row(df: pd.DataFrame, row_idx: int) -> float:
    """
    Score how likely a row is to be the header row.

    Higher score = more likely to be header.
    """
    if row_idx >= len(df):
        return 0.0

    row = df.iloc[row_idx]
    score = 0.0

    # Factor 1: All values are non-null strings
    non_null_count = row.notna().sum()
    if non_null_count == len(row):
        score += 0.2

    # Factor 2: Values look like column names (strings, not numbers)
    string_like = 0
    for val in row:
        if pd.isna(val):
            continue
        val_str = str(val).strip()
        # Headers are usually short strings, not pure numbers or dates
        if val_str and not _looks_like_data_value(val_str):
            string_like += 1

    if len(row) > 0:
        score += 0.3 * (string_like / len(row))

    # Factor 3: Values are unique (headers should be unique)
    unique_count = row.dropna().nunique()
    if non_null_count > 0 and unique_count == non_null_count:
        score += 0.2

    # Factor 4: Data below this row has consistent types
    if row_idx + 1 < len(df):
        data_below = df.iloc[row_idx + 1:]
        if len(data_below) > 0:
            type_consistency = _check_type_consistency(data_below)
            score += 0.3 * type_consistency

    return score


def _looks_like_data_value(value: str) -> bool:
    """Check if a string looks like a data value rather than a header."""
    # Pure numbers are data
    try:
        float(value.replace(",", "").replace("$", "").replace("%", ""))
        return True
    except ValueError:
        pass

    # Date patterns are data
    if PATTERNS["date_iso"].match(value) or PATTERNS["date_us"].match(value):
        return True

    # Email, phone, URL are data
    if PATTERNS["email"].match(value) or PATTERNS["url"].match(value):
        return True

    return False


def _check_type_consistency(df: pd.DataFrame) -> float:
    """Check how consistent data types are across columns (0.0-1.0)."""
    if df.empty:
        return 0.5

    consistent_cols = 0
    for col in df.columns:
        col_data = df[col].dropna()
        if len(col_data) == 0:
            continue

        # Check if column has consistent type
        types = set()
        for val in col_data.head(5):
            if isinstance(val, (int, float)) or (isinstance(val, str) and _looks_like_data_value(str(val))):
                types.add("numeric_or_data")
            else:
                types.add("text")

        if len(types) <= 1:
            consistent_cols += 1

    return consistent_cols / len(df.columns) if len(df.columns) > 0 else 0.5


def detect_footer_rows(
    df: pd.DataFrame,
    max_rows_to_check: int = 5,
) -> tuple[int | None, int, str | None]:
    """
    Detect footer/summary rows at the end of the data.

    Looks for patterns like:
    - Rows where most columns are empty
    - Rows with aggregate keywords (Total, Sum, Average, Count)
    - Empty rows after data

    Args:
        df: DataFrame to analyze
        max_rows_to_check: How many rows from the end to analyze

    Returns:
        Tuple of (data_end_row, footer_row_count, footer_type)
        - data_end_row: Last row of actual data (None if all rows are data)
        - footer_row_count: Number of footer rows detected
        - footer_type: Type of footer ("summary", "metadata", "empty", None)
    """
    if df.empty or len(df) < 2:
        return None, 0, None

    # Aggregate keywords that indicate summary rows
    AGGREGATE_KEYWORDS = {
        "total", "sum", "average", "avg", "count", "grand total",
        "subtotal", "sub-total", "sum total", "overall"
    }

    footer_start = None
    footer_type = None
    rows_to_check = min(max_rows_to_check, len(df))

    for i in range(1, rows_to_check + 1):
        row_idx = len(df) - i
        row = df.iloc[row_idx]

        # Check 1: Is this row mostly empty?
        null_ratio = row.isna().sum() / len(row) if len(row) > 0 else 0
        if null_ratio > 0.5:
            footer_start = row_idx
            footer_type = "empty"
            continue

        # Check 2: Does this row contain aggregate keywords?
        row_text = " ".join(str(v).lower() for v in row if pd.notna(v))
        has_aggregate = any(kw in row_text for kw in AGGREGATE_KEYWORDS)
        if has_aggregate:
            footer_start = row_idx
            footer_type = "summary"
            continue

        # Check 3: Does first column have aggregate keyword but others are numeric?
        first_val = str(row.iloc[0]).lower() if pd.notna(row.iloc[0]) else ""
        if any(kw in first_val for kw in AGGREGATE_KEYWORDS):
            footer_start = row_idx
            footer_type = "summary"
            continue

        # If we found data, stop looking
        break

    if footer_start is not None:
        data_end_row = footer_start - 1
        footer_count = len(df) - footer_start
        logger.debug(f"Footer detection: {footer_count} {footer_type} rows at end")
        return data_end_row, footer_count, footer_type

    return None, 0, None


# =============================================================================
# Profile Generation (Enhanced)
# =============================================================================

def generate_profile(
    df: pd.DataFrame,
    max_sample_rows: int = DEFAULT_SAMPLE_ROWS,
    file_size_bytes: int | None = None,
    encoding_detected: str | None = None,
    delimiter_detected: str | None = None,
) -> DataProfile:
    """
    Generate a comprehensive DataProfile from a DataFrame.

    Enhanced to include:
    - Semantic types for each column
    - Statistical analysis
    - Data quality issues with severity
    - Value distributions
    """
    # Handle empty DataFrame
    if df.empty:
        return DataProfile(
            row_count=0,
            column_count=0,
            columns=[],
            sample_rows=[],
            file_size_bytes=file_size_bytes,
            encoding_detected=encoding_detected,
            delimiter_detected=delimiter_detected,
            issues=[DataIssue(
                issue_type=IssueType.MISSING_VALUES,
                severity=IssueSeverity.CRITICAL,
                column=None,
                description="Dataset is empty (no rows)",
                suggestion="Check if the file was loaded correctly",
            )],
            warnings=["Dataset is empty"],
        )

    row_count = len(df)
    column_count = len(df.columns)

    # Analyze each column
    column_profiles = []
    for col_name in df.columns:
        try:
            col_profile = analyze_column(df[col_name], row_count)
            column_profiles.append(col_profile)
        except Exception as e:
            logger.warning(f"Error analyzing column '{col_name}': {e}")
            column_profiles.append(ColumnProfile(
                name=str(col_name),
                dtype="unknown",
            ))

    # Extract sample rows
    sample_rows = _extract_sample_rows(df, max_sample_rows)

    # Dataset-level issues
    issues = detect_dataset_issues(df)

    # Legacy warnings (for backward compatibility)
    warnings = _generate_legacy_warnings(df, column_profiles)

    # Compute dataset stats
    duplicate_row_count = len(df) - len(df.drop_duplicates())
    complete_row_count = int(df.dropna().shape[0])

    try:
        memory_usage = int(df.memory_usage(deep=True).sum())
    except Exception:
        memory_usage = None

    # Detect footer rows
    data_end_row, footer_row_count, footer_type = detect_footer_rows(df)

    # Add footer issue if detected
    if footer_row_count > 0:
        issues.append(DataIssue(
            issue_type=IssueType.INVALID_VALUES,
            severity=IssueSeverity.INFO,
            column=None,
            description=f"Detected {footer_row_count} {footer_type} row(s) at the end of the data",
            affected_count=footer_row_count,
            suggestion=f"Consider removing the last {footer_row_count} rows if they are not actual data",
        ))

    return DataProfile(
        row_count=row_count,
        column_count=column_count,
        columns=column_profiles,
        sample_rows=sample_rows,
        file_size_bytes=file_size_bytes,
        encoding_detected=encoding_detected,
        delimiter_detected=delimiter_detected,
        issues=issues,
        warnings=warnings,
        duplicate_row_count=duplicate_row_count,
        complete_row_count=complete_row_count,
        memory_usage_bytes=memory_usage,
        # Header/footer detection (header detection happens at read time)
        detected_data_end_row=data_end_row,
        detected_footer_rows=footer_row_count,
        footer_type=footer_type,
    )


def _extract_sample_rows(df: pd.DataFrame, count: int) -> list[dict[str, Any]]:
    """Extract sample rows as dictionaries."""
    sample_rows = []
    for _, row in df.head(count).iterrows():
        row_dict = {str(col): _safe_value(val) for col, val in row.items()}
        sample_rows.append(row_dict)
    return sample_rows


def _generate_legacy_warnings(df: pd.DataFrame, columns: list[ColumnProfile]) -> list[str]:
    """Generate simple warning strings for backward compatibility."""
    warnings = []

    for col in columns:
        if col.null_percent >= 50:
            warnings.append(f"Column '{col.name}' has {col.null_percent:.0f}% missing values")
        elif col.null_percent >= 20:
            warnings.append(f"Column '{col.name}' has {col.null_percent:.0f}% missing values")

        if col.is_unique and len(df) > 10:
            warnings.append(f"Column '{col.name}' has all unique values (possible ID column)")

    duplicate_count = len(df) - len(df.drop_duplicates())
    if duplicate_count > 0:
        pct = (duplicate_count / len(df)) * 100
        warnings.append(f"Dataset has {duplicate_count} duplicate rows ({pct:.1f}%)")

    return warnings


# =============================================================================
# Convenience Functions
# =============================================================================

def get_file_size(file_path: Union[str, Path]) -> int:
    """Get file size in bytes."""
    return Path(file_path).stat().st_size


def profile_from_file(
    file_path: Union[str, Path],
    max_rows: int | None = MAX_ROWS_FOR_PROFILE,
) -> DataProfile:
    """
    Profile a CSV file in one call.

    Combines read_csv_safe() and generate_profile().
    """
    file_path = Path(file_path)
    file_size = get_file_size(file_path)

    df, encoding, delimiter = read_csv_safe(file_path, max_rows=max_rows)
    profile = generate_profile(
        df,
        file_size_bytes=file_size,
        encoding_detected=encoding,
        delimiter_detected=delimiter,
    )

    return profile


# =============================================================================
# Schema Matching (Phase 2 Foundation)
# =============================================================================
# These functions support module contracts and schema matching for:
# - Validating incoming data against a saved module's expected schema
# - Chaining modules together in workflows
# - Confidence scoring for automatic vs. manual processing
# =============================================================================


# -----------------------------------------------------------------------------
# Column Name Matching Utilities
# -----------------------------------------------------------------------------

# Common synonyms for column names (lowercase)
COLUMN_SYNONYMS = {
    "email": ["email", "e_mail", "email_address", "emailaddress", "e-mail", "mail"],
    "phone": ["phone", "telephone", "tel", "phone_number", "phonenumber", "mobile", "cell"],
    "name": ["name", "full_name", "fullname", "customer_name", "customername", "user_name", "username"],
    "first_name": ["first_name", "firstname", "fname", "first", "given_name", "givenname"],
    "last_name": ["last_name", "lastname", "lname", "last", "surname", "family_name", "familyname"],
    "address": ["address", "street_address", "streetaddress", "addr", "street"],
    "city": ["city", "town", "municipality"],
    "state": ["state", "province", "region", "state_province"],
    "country": ["country", "nation", "country_code", "countrycode"],
    "zip": ["zip", "zipcode", "zip_code", "postal_code", "postalcode", "postal"],
    "date": ["date", "dt", "created_date", "createddate", "updated_date", "updateddate"],
    "id": ["id", "identifier", "key", "pk", "primary_key"],
    "customer_id": ["customer_id", "customerid", "cust_id", "custid", "client_id", "clientid"],
    "order_id": ["order_id", "orderid", "order_number", "ordernumber"],
    "product_id": ["product_id", "productid", "prod_id", "prodid", "item_id", "itemid"],
    "amount": ["amount", "total", "total_amount", "totalamount", "sum", "value"],
    "price": ["price", "unit_price", "unitprice", "cost", "rate"],
    "quantity": ["quantity", "qty", "count", "num", "number"],
    "status": ["status", "state", "condition", "status_code", "statuscode"],
    "description": ["description", "desc", "details", "notes", "comment", "comments"],
}


def normalize_column_name(name: str) -> str:
    """
    Normalize a column name for comparison.

    Converts to lowercase, removes special characters, standardizes separators.

    Example:
        "Customer Email Address" -> "customeremailaddress"
        "customer_email_address" -> "customeremailaddress"
    """
    # Convert to lowercase
    normalized = name.lower()

    # Replace common separators with nothing
    for sep in [" ", "_", "-", "."]:
        normalized = normalized.replace(sep, "")

    return normalized


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculate Levenshtein (edit) distance between two strings.

    This is the minimum number of single-character edits (insertions,
    deletions, substitutions) needed to transform s1 into s2.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # Cost is 0 if characters match, 1 otherwise
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def name_similarity(name1: str, name2: str) -> float:
    """
    Calculate similarity score between two column names (0.0 to 1.0).

    Uses normalized names and Levenshtein distance.
    Higher score = more similar.
    """
    # Normalize both names
    n1 = normalize_column_name(name1)
    n2 = normalize_column_name(name2)

    # Exact match after normalization
    if n1 == n2:
        return 1.0

    # Calculate Levenshtein distance
    max_len = max(len(n1), len(n2))
    if max_len == 0:
        return 1.0

    distance = levenshtein_distance(n1, n2)
    similarity = 1.0 - (distance / max_len)

    return max(0.0, similarity)


def find_synonym_match(name: str) -> str | None:
    """
    Find if a name matches any known synonym group.

    Returns the canonical (first) name from the synonym group.
    """
    normalized = normalize_column_name(name)

    for canonical, synonyms in COLUMN_SYNONYMS.items():
        for syn in synonyms:
            if normalize_column_name(syn) == normalized:
                return canonical

    return None


def match_column_name(
    incoming_name: str,
    contract_name: str,
    alternative_names: list[str] | None = None,
) -> tuple[float, str]:
    """
    Match an incoming column name against a contract column.

    Returns:
        Tuple of (confidence score, match type)
        - confidence: 0.0 to 1.0
        - match_type: "exact", "normalized", "synonym", "fuzzy"
    """
    # Exact match
    if incoming_name == contract_name:
        return 1.0, "exact"

    # Normalized match (case/separator insensitive)
    if normalize_column_name(incoming_name) == normalize_column_name(contract_name):
        return 0.95, "normalized"

    # Check alternative names from contract
    if alternative_names:
        for alt in alternative_names:
            if normalize_column_name(incoming_name) == normalize_column_name(alt):
                return 0.95, "alternative"

    # Synonym match
    incoming_canonical = find_synonym_match(incoming_name)
    contract_canonical = find_synonym_match(contract_name)

    if incoming_canonical and contract_canonical and incoming_canonical == contract_canonical:
        return 0.85, "synonym"

    # Fuzzy match
    similarity = name_similarity(incoming_name, contract_name)
    if similarity >= 0.7:
        return similarity * 0.9, "fuzzy"  # Scale down fuzzy matches slightly

    return similarity * 0.5, "weak"  # Very low confidence for poor matches


# -----------------------------------------------------------------------------
# Semantic Type Compatibility
# -----------------------------------------------------------------------------

# Semantic type compatibility groups
# Types within same group are considered compatible
COMPATIBLE_SEMANTIC_TYPES = {
    # Identifiers - all ID-like types are compatible
    "identifiers": {SemanticType.ID, SemanticType.NUMERIC},

    # Contact info
    "contact": {SemanticType.EMAIL, SemanticType.PHONE, SemanticType.URL},

    # Temporal - dates and times are somewhat interchangeable
    "temporal": {SemanticType.DATE, SemanticType.DATETIME, SemanticType.TIME},

    # Numeric values
    "numeric": {SemanticType.NUMERIC, SemanticType.CURRENCY, SemanticType.PERCENTAGE},

    # Text-like
    "text": {SemanticType.TEXT, SemanticType.NAME, SemanticType.CATEGORY, SemanticType.UNKNOWN},

    # Location
    "location": {SemanticType.ZIPCODE, SemanticType.COUNTRY, SemanticType.STATE},
}


def semantic_types_compatible(type1: SemanticType, type2: SemanticType) -> bool:
    """
    Check if two semantic types are compatible.

    Same type = always compatible.
    Different types in same group = compatible.
    UNKNOWN is compatible with everything.
    """
    # Same type
    if type1 == type2:
        return True

    # UNKNOWN is compatible with anything
    if type1 == SemanticType.UNKNOWN or type2 == SemanticType.UNKNOWN:
        return True

    # Check if in same compatibility group
    for group_types in COMPATIBLE_SEMANTIC_TYPES.values():
        if type1 in group_types and type2 in group_types:
            return True

    return False


def semantic_type_similarity(type1: SemanticType, type2: SemanticType) -> float:
    """
    Calculate similarity score between two semantic types (0.0 to 1.0).
    """
    # Same type
    if type1 == type2:
        return 1.0

    # UNKNOWN types
    if type1 == SemanticType.UNKNOWN or type2 == SemanticType.UNKNOWN:
        return 0.5  # Neutral - could be anything

    # Check compatibility groups
    for group_types in COMPATIBLE_SEMANTIC_TYPES.values():
        if type1 in group_types and type2 in group_types:
            return 0.7  # Good match within group

    return 0.2  # Incompatible types


# -----------------------------------------------------------------------------
# Data Type Compatibility
# -----------------------------------------------------------------------------

# Pandas dtype compatibility
DTYPE_COMPATIBLE = {
    "int64": {"int64", "int32", "float64", "object"},
    "int32": {"int64", "int32", "float64", "object"},
    "float64": {"int64", "int32", "float64", "object"},
    "object": {"object", "string", "category"},
    "bool": {"bool", "object"},
    "datetime64[ns]": {"datetime64[ns]", "object"},
    "category": {"category", "object"},
}


def dtypes_compatible(dtype1: str, dtype2: str) -> bool:
    """Check if two pandas dtypes are compatible."""
    # Normalize dtype strings
    d1 = dtype1.lower().strip()
    d2 = dtype2.lower().strip()

    # Same type
    if d1 == d2:
        return True

    # Check compatibility
    compatible_set = DTYPE_COMPATIBLE.get(d1, {d1})
    return d2 in compatible_set


# -----------------------------------------------------------------------------
# Value Overlap Detection
# -----------------------------------------------------------------------------

def calculate_value_overlap(
    incoming_samples: list[Any],
    contract_samples: list[Any],
) -> float:
    """
    Calculate overlap between sample values (0.0 to 1.0).

    Useful for detecting if columns contain similar data even if
    names don't match.
    """
    if not incoming_samples or not contract_samples:
        return 0.0

    # Normalize values for comparison
    incoming_normalized = set(str(v).lower().strip() for v in incoming_samples if v is not None)
    contract_normalized = set(str(v).lower().strip() for v in contract_samples if v is not None)

    if not incoming_normalized or not contract_normalized:
        return 0.0

    # Calculate Jaccard similarity
    intersection = len(incoming_normalized & contract_normalized)
    union = len(incoming_normalized | contract_normalized)

    if union == 0:
        return 0.0

    return intersection / union


# -----------------------------------------------------------------------------
# Contract Generation
# -----------------------------------------------------------------------------

def generate_contract(
    profile: DataProfile,
    module_id: str | None = None,
    module_name: str | None = None,
    mark_all_required: bool = True,
) -> SchemaContract:
    """
    Generate a SchemaContract from a DataProfile.

    This creates the "contract" for a module based on the last node's
    output profile. The contract defines what incoming data should
    look like to be processed by the module.

    Args:
        profile: DataProfile from the last node
        module_id: Optional module identifier
        module_name: Optional human-readable module name
        mark_all_required: If True, all columns are marked as required

    Returns:
        SchemaContract that can be saved and used for matching
    """
    from datetime import datetime
    import hashlib

    column_contracts = []

    for col in profile.columns:
        # Generate alternative names based on the column name
        alternative_names = _generate_alternative_names(col.name)

        # Determine if column should allow nulls
        nullable = col.null_percent > 0

        # Get allowed values for categorical columns
        allowed_values = None
        if col.distribution and col.distribution.is_categorical:
            allowed_values = [v["value"] for v in col.distribution.top_values]

        column_contracts.append(ColumnContract(
            name=col.name,
            alternative_names=alternative_names,
            semantic_type=col.semantic_type,
            dtype=col.dtype,
            expected_pattern=col.detected_pattern,
            required=mark_all_required,
            nullable=nullable,
            max_null_percent=max(col.null_percent + 10, 10),  # Allow some flexibility
            sample_values=col.sample_values[:5],
            allowed_values=allowed_values,
            min_value=col.min_value,
            max_value=col.max_value,
        ))

    # Generate fingerprint for quick comparison
    fingerprint_data = "|".join([
        f"{c.name}:{c.semantic_type.value}:{c.dtype}"
        for c in column_contracts
    ])
    fingerprint = hashlib.sha256(fingerprint_data.encode()).hexdigest()[:16]

    return SchemaContract(
        module_id=module_id,
        module_name=module_name,
        version="1.0",
        created_at=datetime.utcnow().isoformat(),
        columns=column_contracts,
        required_column_count=len([c for c in column_contracts if c.required]),
        min_row_count=1,
        expected_delimiter=profile.delimiter_detected,
        expected_encoding=profile.encoding_detected,
        fingerprint=fingerprint,
    )


def _generate_alternative_names(col_name: str) -> list[str]:
    """
    Generate alternative acceptable names for a column.

    Based on common variations and the synonym dictionary.
    """
    alternatives = set()
    normalized = normalize_column_name(col_name)

    # Add common variations
    alternatives.add(col_name.lower())
    alternatives.add(col_name.upper())
    alternatives.add(col_name.title())
    alternatives.add(col_name.replace("_", " "))
    alternatives.add(col_name.replace(" ", "_"))
    alternatives.add(col_name.replace("-", "_"))
    alternatives.add(col_name.replace("_", "-"))

    # Check if matches a synonym group
    for canonical, synonyms in COLUMN_SYNONYMS.items():
        for syn in synonyms:
            if normalize_column_name(syn) == normalized:
                # Add all synonyms as alternatives
                alternatives.update(synonyms)
                break

    # Remove the original name
    alternatives.discard(col_name)

    return list(alternatives)[:10]  # Limit to 10 alternatives


# -----------------------------------------------------------------------------
# Schema Matching
# -----------------------------------------------------------------------------

def match_schema(
    incoming_profile: DataProfile,
    contract: SchemaContract,
) -> SchemaMatch:
    """
    Match an incoming data profile against a module contract.

    This is the main function for determining if incoming data
    can be processed by a saved module.

    Args:
        incoming_profile: Profile of the incoming/new data
        contract: The module's expected schema contract

    Returns:
        SchemaMatch with confidence score, column mappings, and discrepancies
    """
    from datetime import datetime

    mappings: list[ColumnMapping] = []
    discrepancies: list[SchemaDiscrepancy] = []
    unmapped_incoming: list[str] = []
    unmapped_required: list[str] = []

    # Track which contract columns have been matched
    matched_contract_cols = set()

    # Track incoming column names
    incoming_names = [col.name for col in incoming_profile.columns]
    incoming_by_name = {col.name: col for col in incoming_profile.columns}

    # Try to match each incoming column to a contract column
    for incoming_col in incoming_profile.columns:
        best_match = _find_best_column_match(incoming_col, contract, matched_contract_cols)

        if best_match:
            contract_col, confidence, match_type, notes = best_match
            matched_contract_cols.add(contract_col.name)

            # Check type compatibility
            type_compatible = semantic_types_compatible(
                incoming_col.semantic_type,
                contract_col.semantic_type
            )
            dtype_compatible = dtypes_compatible(incoming_col.dtype, contract_col.dtype)

            mappings.append(ColumnMapping(
                incoming_name=incoming_col.name,
                contract_name=contract_col.name,
                match_type=match_type,
                confidence=confidence,
                notes=notes,
                type_compatible=type_compatible,
                dtype_compatible=dtype_compatible,
            ))

            # Add discrepancy if types don't match
            if not type_compatible:
                discrepancies.append(SchemaDiscrepancy(
                    discrepancy_type="type_mismatch",
                    severity=IssueSeverity.WARNING,
                    column=incoming_col.name,
                    description=f"Semantic type mismatch: incoming '{incoming_col.semantic_type.value}' vs expected '{contract_col.semantic_type.value}'",
                    suggestion=f"Verify column '{incoming_col.name}' contains {contract_col.semantic_type.value} data",
                    incoming_value=incoming_col.semantic_type.value,
                    expected_value=contract_col.semantic_type.value,
                ))

        else:
            unmapped_incoming.append(incoming_col.name)

    # Check for missing required columns
    for contract_col in contract.columns:
        if contract_col.name not in matched_contract_cols:
            if contract_col.required:
                unmapped_required.append(contract_col.name)
                discrepancies.append(SchemaDiscrepancy(
                    discrepancy_type="missing_required",
                    severity=IssueSeverity.CRITICAL,
                    column=contract_col.name,
                    description=f"Required column '{contract_col.name}' not found in incoming data",
                    suggestion=f"Check if column exists with different name: {contract_col.alternative_names[:3]}",
                    expected_value=contract_col.name,
                ))
            else:
                discrepancies.append(SchemaDiscrepancy(
                    discrepancy_type="missing_optional",
                    severity=IssueSeverity.INFO,
                    column=contract_col.name,
                    description=f"Optional column '{contract_col.name}' not found",
                ))

    # Add discrepancies for unmapped incoming columns
    for col_name in unmapped_incoming:
        discrepancies.append(SchemaDiscrepancy(
            discrepancy_type="extra_column",
            severity=IssueSeverity.INFO,
            column=col_name,
            description=f"Column '{col_name}' not in contract (will be ignored)",
            incoming_value=col_name,
        ))

    # Calculate overall confidence score
    confidence_score = _calculate_match_confidence(
        mappings=mappings,
        unmapped_required=unmapped_required,
        contract=contract,
    )

    # Determine confidence level
    confidence_level = _score_to_level(confidence_score)

    return SchemaMatch(
        confidence_score=confidence_score,
        confidence_level=confidence_level,
        is_compatible=confidence_score >= 40.0,
        auto_processable=confidence_score >= 85.0,
        column_mappings=mappings,
        unmapped_incoming=unmapped_incoming,
        unmapped_required=unmapped_required,
        discrepancies=discrepancies,
        contract_id=contract.contract_id,
        module_name=contract.module_name,
        match_timestamp=datetime.utcnow().isoformat(),
    )


def _find_best_column_match(
    incoming_col: ColumnProfile,
    contract: SchemaContract,
    already_matched: set[str],
) -> tuple[ColumnContract, float, str, str] | None:
    """
    Find the best matching contract column for an incoming column.

    Returns:
        Tuple of (contract_column, confidence, match_type, notes) or None
    """
    best_match = None
    best_score = 0.0

    for contract_col in contract.columns:
        # Skip already matched columns
        if contract_col.name in already_matched:
            continue

        # Calculate match score using multiple signals
        name_score, name_match_type = match_column_name(
            incoming_col.name,
            contract_col.name,
            contract_col.alternative_names,
        )

        semantic_score = semantic_type_similarity(
            incoming_col.semantic_type,
            contract_col.semantic_type,
        )

        value_score = calculate_value_overlap(
            incoming_col.sample_values,
            contract_col.sample_values,
        )

        # Weighted combination
        # Name is most important, then semantic type, then value overlap
        total_score = (
            name_score * 0.5 +       # 50% weight on name matching
            semantic_score * 0.3 +   # 30% weight on semantic type
            value_score * 0.2        # 20% weight on value overlap
        )

        # Minimum threshold for consideration
        if total_score > 0.4 and total_score > best_score:
            best_score = total_score
            best_match = (
                contract_col,
                total_score,
                name_match_type,
                f"Name: {name_score:.2f}, Type: {semantic_score:.2f}, Values: {value_score:.2f}",
            )

    return best_match


def _calculate_match_confidence(
    mappings: list[ColumnMapping],
    unmapped_required: list[str],
    contract: SchemaContract,
) -> float:
    """
    Calculate overall match confidence score (0-100).

    Factors:
    - Percentage of required columns matched
    - Average confidence of mappings
    - Penalty for missing required columns
    """
    if not contract.columns:
        return 0.0

    required_cols = [c for c in contract.columns if c.required]
    total_required = len(required_cols)

    if total_required == 0:
        # No required columns - base on mapping confidence
        if not mappings:
            return 50.0  # Neutral
        avg_confidence = sum(m.confidence for m in mappings) / len(mappings)
        return avg_confidence * 100

    # Calculate required column match rate
    matched_required = total_required - len(unmapped_required)
    required_match_rate = matched_required / total_required

    # Calculate average mapping confidence
    if mappings:
        avg_mapping_confidence = sum(m.confidence for m in mappings) / len(mappings)
    else:
        avg_mapping_confidence = 0.0

    # Combine factors
    # Missing required columns is heavily penalized
    base_score = (
        required_match_rate * 60 +      # 60% weight on matching required columns
        avg_mapping_confidence * 40     # 40% weight on mapping quality
    )

    # Penalty for each missing required column
    penalty = len(unmapped_required) * 15  # 15 points per missing required column

    final_score = max(0.0, base_score - penalty)
    return min(100.0, final_score)


def _score_to_level(score: float) -> MatchConfidence:
    """Convert numeric score to confidence level."""
    if score >= 85:
        return MatchConfidence.HIGH
    elif score >= 60:
        return MatchConfidence.MEDIUM
    elif score >= 40:
        return MatchConfidence.LOW
    else:
        return MatchConfidence.NO_MATCH


# -----------------------------------------------------------------------------
# Contract-to-Contract Matching (Workflow Validation)
# -----------------------------------------------------------------------------

def match_contracts(
    source_contract: SchemaContract,
    target_contract: SchemaContract,
) -> ContractMatch:
    """
    Match two contracts against each other for workflow validation.

    Used to check if Module A's output can connect to Module B's input.

    Args:
        source_contract: Output contract from upstream module
        target_contract: Input contract for downstream module

    Returns:
        ContractMatch indicating if modules can be chained
    """
    mappings: list[ColumnMapping] = []
    discrepancies: list[SchemaDiscrepancy] = []

    # Track matched columns
    matched_target_cols = set()

    # Try to match source columns to target requirements
    for source_col in source_contract.columns:
        best_match = None
        best_score = 0.0

        for target_col in target_contract.columns:
            if target_col.name in matched_target_cols:
                continue

            name_score, name_type = match_column_name(
                source_col.name,
                target_col.name,
                target_col.alternative_names,
            )

            semantic_score = semantic_type_similarity(
                source_col.semantic_type,
                target_col.semantic_type,
            )

            total_score = name_score * 0.6 + semantic_score * 0.4

            if total_score > 0.5 and total_score > best_score:
                best_score = total_score
                best_match = (target_col, total_score, name_type)

        if best_match:
            target_col, score, match_type = best_match
            matched_target_cols.add(target_col.name)

            mappings.append(ColumnMapping(
                incoming_name=source_col.name,
                contract_name=target_col.name,
                match_type=match_type,
                confidence=score,
                type_compatible=semantic_types_compatible(
                    source_col.semantic_type,
                    target_col.semantic_type,
                ),
            ))

    # Check for unmatched required target columns
    for target_col in target_contract.columns:
        if target_col.required and target_col.name not in matched_target_cols:
            discrepancies.append(SchemaDiscrepancy(
                discrepancy_type="missing_required",
                severity=IssueSeverity.CRITICAL,
                column=target_col.name,
                description=f"Target requires '{target_col.name}' but source doesn't provide it",
                expected_value=target_col.name,
            ))

    # Calculate confidence
    required_target = [c for c in target_contract.columns if c.required]
    if required_target:
        matched_required = len([c for c in required_target if c.name in matched_target_cols])
        confidence = (matched_required / len(required_target)) * 100
    else:
        confidence = 100.0 if mappings else 50.0

    return ContractMatch(
        confidence_score=confidence,
        is_chainable=confidence >= 80.0 and not any(
            d.severity == IssueSeverity.CRITICAL for d in discrepancies
        ),
        source_contract_id=source_contract.contract_id,
        target_contract_id=target_contract.contract_id,
        column_mappings=mappings,
        discrepancies=discrepancies,
    )
