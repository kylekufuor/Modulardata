# =============================================================================
# tests/test_profiler.py - Enhanced Data Profiler Tests
# =============================================================================
# Tests for the enhanced CSV profiling engine (lib/profiler.py).
# Covers:
#   - CSV reading with various formats
#   - Column analysis (types, nulls, samples, statistics)
#   - Semantic type detection
#   - Data quality issue detection
#   - Profile generation and text output
#
# Run with: poetry run pytest tests/test_profiler.py -v
# =============================================================================

from pathlib import Path

import pandas as pd
import pytest

from lib.profiler import (
    analyze_column,
    detect_delimiter,
    detect_semantic_type,
    generate_profile,
    profile_from_file,
    read_csv_safe,
)
from core.models import (
    ColumnProfile,
    DataProfile,
    SemanticType,
    IssueSeverity,
)


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_normal_csv() -> Path:
    return FIXTURES_DIR / "sample_normal.csv"


@pytest.fixture
def sample_with_nulls_csv() -> Path:
    return FIXTURES_DIR / "sample_with_nulls.csv"


@pytest.fixture
def sample_semicolon_csv() -> Path:
    return FIXTURES_DIR / "sample_semicolon.csv"


@pytest.fixture
def sample_empty_csv() -> Path:
    return FIXTURES_DIR / "sample_empty.csv"


@pytest.fixture
def sample_duplicates_csv() -> Path:
    return FIXTURES_DIR / "sample_duplicates.csv"


@pytest.fixture
def normal_dataframe(sample_normal_csv) -> pd.DataFrame:
    """Load normal CSV as DataFrame (unpacked from tuple)."""
    df, _, _ = read_csv_safe(sample_normal_csv)
    return df


# =============================================================================
# CSV Reading Tests
# =============================================================================

class TestReadCsvSafe:
    """Tests for read_csv_safe() function."""

    def test_read_normal_csv(self, sample_normal_csv):
        """Test reading a standard CSV file."""
        df, encoding, delimiter = read_csv_safe(sample_normal_csv)

        assert len(df) == 5
        assert len(df.columns) == 7
        assert "id" in df.columns
        assert "name" in df.columns
        assert encoding == "utf-8"
        assert delimiter == ","

    def test_read_csv_with_max_rows(self, sample_normal_csv):
        """Test reading with row limit."""
        df, _, _ = read_csv_safe(sample_normal_csv, max_rows=2)
        assert len(df) == 2

    def test_read_semicolon_csv(self, sample_semicolon_csv):
        """Test automatic delimiter detection for semicolon CSV."""
        df, encoding, delimiter = read_csv_safe(sample_semicolon_csv)

        assert len(df) == 5
        assert "product_id" in df.columns
        assert "product_name" in df.columns
        assert delimiter == ";"

    def test_read_empty_csv(self, sample_empty_csv):
        """Test reading an empty CSV (header only)."""
        df, _, _ = read_csv_safe(sample_empty_csv)
        assert len(df) == 0
        assert len(df.columns) == 3

    def test_read_nonexistent_file(self):
        """Test that reading a nonexistent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            read_csv_safe("/nonexistent/path/file.csv")


class TestDetectDelimiter:
    """Tests for delimiter detection."""

    def test_detect_comma(self, sample_normal_csv):
        delimiter = detect_delimiter(sample_normal_csv)
        assert delimiter == ","

    def test_detect_semicolon(self, sample_semicolon_csv):
        delimiter = detect_delimiter(sample_semicolon_csv)
        assert delimiter == ";"


# =============================================================================
# Semantic Type Detection Tests
# =============================================================================

class TestSemanticTypeDetection:
    """Tests for semantic type detection."""

    def test_detect_email_by_name(self):
        """Test email detection by column name."""
        series = pd.Series(["john@test.com", "jane@test.com"], name="email")
        sem_type, pattern = detect_semantic_type(series, "email")
        assert sem_type == SemanticType.EMAIL

    def test_detect_email_by_pattern(self):
        """Test email detection by value pattern."""
        series = pd.Series(["john@test.com", "jane@example.org", "bob@corp.net"], name="contact")
        sem_type, pattern = detect_semantic_type(series, "contact")
        assert sem_type == SemanticType.EMAIL

    def test_detect_id_column(self):
        """Test ID detection for unique values."""
        series = pd.Series([1, 2, 3, 4, 5], name="user_id")
        sem_type, _ = detect_semantic_type(series, "user_id")
        assert sem_type == SemanticType.ID

    def test_detect_boolean(self):
        """Test boolean detection."""
        series = pd.Series(["true", "false", "true", "false"], name="active")
        sem_type, pattern = detect_semantic_type(series, "active")
        assert sem_type == SemanticType.BOOLEAN

    def test_detect_date_iso(self):
        """Test ISO date detection."""
        series = pd.Series(["2024-01-15", "2024-02-20", "2024-03-10"], name="created")
        sem_type, pattern = detect_semantic_type(series, "created")
        assert sem_type == SemanticType.DATE
        assert "YYYY-MM-DD" in pattern

    def test_detect_category(self):
        """Test category detection for low cardinality."""
        series = pd.Series(["A", "B", "A", "C", "B", "A"] * 20, name="grade")
        sem_type, _ = detect_semantic_type(series, "grade")
        assert sem_type == SemanticType.CATEGORY


# =============================================================================
# Column Analysis Tests
# =============================================================================

class TestAnalyzeColumn:
    """Tests for analyze_column() function."""

    def test_analyze_integer_column(self, normal_dataframe):
        """Test analyzing an integer column."""
        profile = analyze_column(normal_dataframe["id"], len(normal_dataframe))

        assert profile.name == "id"
        assert "int" in profile.dtype.lower()
        assert profile.null_count == 0
        assert profile.null_percent == 0.0
        assert profile.unique_count == 5
        assert profile.is_unique == True

    def test_analyze_string_column(self, normal_dataframe):
        """Test analyzing a string column."""
        profile = analyze_column(normal_dataframe["name"], len(normal_dataframe))

        assert profile.name == "name"
        assert profile.dtype == "object"
        assert profile.null_count == 0

    def test_analyze_column_with_nulls(self, sample_with_nulls_csv):
        """Test analyzing a column with null values."""
        df, _, _ = read_csv_safe(sample_with_nulls_csv)
        profile = analyze_column(df["age"], len(df))

        assert profile.name == "age"
        assert profile.null_count > 0
        assert profile.null_percent > 0

    def test_analyze_numeric_with_statistics(self, normal_dataframe):
        """Test that numeric columns get statistics."""
        profile = analyze_column(normal_dataframe["salary"], len(normal_dataframe))

        assert profile.statistics is not None
        assert profile.statistics.mean is not None
        assert profile.statistics.median is not None
        assert profile.statistics.std is not None
        assert profile.min_value is not None
        assert profile.max_value is not None

    def test_analyze_categorical_with_distribution(self):
        """Test that categorical columns get distribution."""
        df = pd.DataFrame({"status": ["active", "inactive", "active", "pending", "active"]})
        profile = analyze_column(df["status"], len(df))

        assert profile.distribution is not None
        assert len(profile.distribution.top_values) > 0
        assert profile.distribution.cardinality == "low"


# =============================================================================
# Profile Generation Tests
# =============================================================================

class TestGenerateProfile:
    """Tests for generate_profile() function."""

    def test_generate_profile_basic(self, normal_dataframe):
        """Test basic profile generation."""
        profile = generate_profile(normal_dataframe)

        assert isinstance(profile, DataProfile)
        assert profile.row_count == 5
        assert profile.column_count == 7
        assert len(profile.columns) == 7
        assert len(profile.sample_rows) > 0

    def test_generate_profile_empty_dataframe(self):
        """Test profiling an empty DataFrame."""
        df = pd.DataFrame()
        profile = generate_profile(df)

        assert profile.row_count == 0
        assert profile.column_count == 0
        assert len(profile.issues) > 0  # Should have an issue about being empty

    def test_generate_profile_detects_nulls(self, sample_with_nulls_csv):
        """Test that profile detects columns with high null percentage."""
        df, _, _ = read_csv_safe(sample_with_nulls_csv)
        profile = generate_profile(df)

        # Check that columns have issues or warnings about nulls
        has_null_warning = any("missing" in w.lower() for w in profile.warnings)
        has_null_issue = any(
            issue.issue_type.value in ("missing_values", "high_null_rate")
            for col in profile.columns
            for issue in col.issues
        )
        assert has_null_warning or has_null_issue

    def test_generate_profile_detects_duplicates(self, sample_duplicates_csv):
        """Test that profile detects duplicate rows."""
        df, _, _ = read_csv_safe(sample_duplicates_csv)
        profile = generate_profile(df)

        assert profile.duplicate_row_count > 0
        duplicate_warnings = [w for w in profile.warnings if "duplicate" in w.lower()]
        assert len(duplicate_warnings) > 0

    def test_generate_profile_includes_stats(self, normal_dataframe):
        """Test that profile includes dataset-level stats."""
        profile = generate_profile(normal_dataframe)

        assert profile.complete_row_count >= 0
        assert profile.duplicate_row_count >= 0


# =============================================================================
# Text Summary Tests
# =============================================================================

class TestTextSummary:
    """Tests for the text summary output."""

    def test_to_text_summary(self, normal_dataframe):
        """Test text summary generation."""
        profile = generate_profile(normal_dataframe)
        summary = profile.to_text_summary()

        assert "Rows: 5" in summary
        assert "Columns: 7" in summary
        assert "id" in summary
        assert "name" in summary

    def test_text_summary_includes_semantic_types(self):
        """Test that semantic types appear in summary."""
        df = pd.DataFrame({
            "email": ["john@test.com", "jane@test.com"],
            "active": ["true", "false"]
        })
        profile = generate_profile(df)
        summary = profile.to_text_summary()

        # Should mention detected types
        assert "email" in summary.lower()

    def test_text_summary_shows_issues(self, sample_with_nulls_csv):
        """Test that issues appear in summary."""
        df, _, _ = read_csv_safe(sample_with_nulls_csv)
        profile = generate_profile(df)
        summary = profile.to_text_summary()

        # Should have warnings or issues section
        assert "ISSUES" in summary or "null" in summary.lower() or "missing" in summary.lower()


# =============================================================================
# Integration Tests
# =============================================================================

class TestProfileFromFile:
    """Tests for the convenience function profile_from_file()."""

    def test_profile_from_file(self, sample_normal_csv):
        """Test profiling directly from file path."""
        profile = profile_from_file(sample_normal_csv)

        assert isinstance(profile, DataProfile)
        assert profile.row_count == 5
        assert profile.column_count == 7
        assert profile.file_size_bytes is not None
        assert profile.file_size_bytes > 0
        assert profile.encoding_detected == "utf-8"
        assert profile.delimiter_detected == ","

    def test_profile_from_file_with_limit(self, sample_normal_csv):
        """Test profiling with row limit."""
        profile = profile_from_file(sample_normal_csv, max_rows=2)
        assert profile.row_count == 2


# =============================================================================
# Data Quality Issue Tests
# =============================================================================

class TestDataQualityIssues:
    """Tests for data quality issue detection."""

    def test_detects_whitespace_issues(self):
        """Test detection of leading/trailing whitespace."""
        df = pd.DataFrame({
            "name": ["  John", "Jane  ", " Bob ", "Alice"]
        })
        profile = generate_profile(df)

        # Check for whitespace issues
        name_col = next(c for c in profile.columns if c.name == "name")
        whitespace_issues = [i for i in name_col.issues if i.issue_type.value == "whitespace"]
        assert len(whitespace_issues) > 0

    def test_detects_empty_strings(self):
        """Test detection of empty strings."""
        df = pd.DataFrame({
            "value": ["a", "", "c", ""]
        })
        profile = generate_profile(df)

        value_col = next(c for c in profile.columns if c.name == "value")
        assert value_col.empty_string_count == 2

    def test_detects_outliers(self):
        """Test detection of numeric outliers."""
        df = pd.DataFrame({
            "amount": [10, 12, 11, 13, 10, 11, 12, 1000, 10, 11]  # 1000 is an outlier
        })
        profile = generate_profile(df)

        amount_col = next(c for c in profile.columns if c.name == "amount")
        if amount_col.statistics:
            assert amount_col.statistics.outlier_count >= 1


# =============================================================================
# Edge Case Tests
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_profile_single_column(self):
        df = pd.DataFrame({"value": [1, 2, 3, 4, 5]})
        profile = generate_profile(df)
        assert profile.row_count == 5
        assert profile.column_count == 1

    def test_profile_single_row(self):
        df = pd.DataFrame({"a": [1], "b": ["test"], "c": [3.14]})
        profile = generate_profile(df)
        assert profile.row_count == 1
        assert profile.column_count == 3

    def test_profile_all_null_column(self):
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "empty": [None, None, None]
        })
        profile = generate_profile(df)

        empty_col = next(c for c in profile.columns if c.name == "empty")
        assert empty_col.null_count == 3
        assert empty_col.null_percent == 100.0

    def test_profile_mixed_types(self):
        df = pd.DataFrame({
            "mixed": [1, "two", 3.0, None, "five"]
        })
        profile = generate_profile(df)
        assert profile.row_count == 5
        assert len(profile.columns) == 1
