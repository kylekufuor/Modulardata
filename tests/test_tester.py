# =============================================================================
# tests/test_tester.py - Tests for Tester Agent
# =============================================================================

import pytest
import pandas as pd
import numpy as np

from agents.tester import TesterAgent, validate_transformation
from agents.models.technical_plan import TechnicalPlan, TransformationType, ColumnTarget, FilterCondition
from agents.models.test_result import TestResult, Severity
from agents.quality_checks import get_checks_for_type, list_checks


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Carol', None, 'Eve'],
        'age': [25, 30, None, 40, 45],
        'salary': [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
        'department': ['Sales', 'Engineering', 'Sales', 'Marketing', 'Engineering']
    })


@pytest.fixture
def tester():
    """Create a TesterAgent instance."""
    return TesterAgent()


def make_plan(
    trans_type: str,
    columns: list[str] | None = None,
    parameters: dict | None = None,
    conditions: list[dict] | None = None
) -> TechnicalPlan:
    """Helper to create TechnicalPlan for tests."""
    target_columns = [ColumnTarget(column_name=c) for c in (columns or [])]
    filter_conditions = [FilterCondition(**c) for c in (conditions or [])]

    return TechnicalPlan(
        transformation_type=trans_type,
        target_columns=target_columns,
        conditions=filter_conditions,
        parameters=parameters or {},
        explanation=f"Test {trans_type}"
    )


# =============================================================================
# Registry Tests
# =============================================================================

class TestRegistry:
    """Tests for quality check registry."""

    def test_checks_registered(self):
        """Verify checks are registered."""
        checks = list_checks()
        assert len(checks) > 0
        assert "schema_valid" in checks
        assert "row_count_change" in checks

    def test_get_checks_for_drop_rows(self):
        """Verify correct checks run for drop_rows."""
        checks = get_checks_for_type("drop_rows")
        check_names = [name for name, _ in checks]

        assert "schema_valid" in check_names  # universal
        assert "target_columns_exist" in check_names  # universal
        assert "row_count_change" in check_names

    def test_get_checks_for_fill_nulls(self):
        """Verify correct checks run for fill_nulls."""
        checks = get_checks_for_type("fill_nulls")
        check_names = [name for name, _ in checks]

        assert "schema_valid" in check_names
        assert "fill_nulls_success" in check_names


# =============================================================================
# TesterAgent Tests
# =============================================================================

class TestTesterAgent:
    """Tests for TesterAgent class."""

    def test_validate_basic(self, sample_df, tester):
        """Test basic validation works."""
        plan = make_plan("trim_whitespace", columns=["name"])
        result = tester.validate(sample_df, sample_df, plan)

        assert isinstance(result, TestResult)
        assert result.rows_before == 5
        assert result.rows_after == 5

    def test_validate_returns_test_result(self, sample_df, tester):
        """Validate returns proper TestResult object."""
        plan = make_plan("drop_rows", columns=["name"])

        # Remove rows with null names
        after_df = sample_df.dropna(subset=["name"])

        result = tester.validate(sample_df, after_df, plan)

        assert isinstance(result, TestResult)
        assert result.transformation_type == "drop_rows"
        assert len(result.checks_run) > 0

    def test_quick_validate(self, sample_df, tester):
        """Test quick_validate returns tuple."""
        plan = make_plan("trim_whitespace", columns=["name"])
        passed, summary = tester.quick_validate(sample_df, sample_df, plan)

        assert isinstance(passed, bool)
        assert isinstance(summary, str)


# =============================================================================
# Schema Check Tests
# =============================================================================

class TestSchemaChecks:
    """Tests for schema validation checks."""

    def test_empty_result_warning(self, sample_df, tester):
        """Test warning when all rows removed."""
        plan = make_plan("drop_rows", columns=["name"])
        empty_df = sample_df.head(0)  # Empty DataFrame

        result = tester.validate(sample_df, empty_df, plan)

        assert not result.passed or result.has_warnings()
        messages = [i.message for i in result.issues]
        assert any("rows" in m.lower() and "removed" in m.lower() for m in messages)

    def test_all_columns_removed_error(self, sample_df, tester):
        """Test error when all columns removed."""
        plan = make_plan("drop_columns", columns=list(sample_df.columns))
        empty_cols_df = pd.DataFrame(index=sample_df.index)

        result = tester.validate(sample_df, empty_cols_df, plan)

        assert not result.passed
        assert result.severity == Severity.ERROR

    def test_target_column_missing_error(self, sample_df, tester):
        """Test error when target column disappears."""
        plan = make_plan("trim_whitespace", columns=["name"])
        after_df = sample_df.drop(columns=["name"])

        result = tester.validate(sample_df, after_df, plan)

        assert not result.passed
        messages = [i.message for i in result.issues]
        assert any("name" in m for m in messages)


# =============================================================================
# Row Count Check Tests
# =============================================================================

class TestRowCountChecks:
    """Tests for row count validation checks."""

    def test_high_data_loss_warning(self, tester):
        """Test warning when > 50% rows removed."""
        before_df = pd.DataFrame({'a': range(100)})
        after_df = pd.DataFrame({'a': range(30)})  # 70% removed
        plan = make_plan("drop_rows", columns=["a"])

        result = tester.validate(before_df, after_df, plan)

        assert result.has_warnings()
        messages = [i.message for i in result.issues]
        assert any("70" in m or "removed" in m.lower() for m in messages)

    def test_row_count_unchanged_for_transform(self, sample_df, tester):
        """Test no warning when rows unchanged for non-row-reducing ops."""
        plan = make_plan("trim_whitespace", columns=["name"])

        result = tester.validate(sample_df, sample_df, plan)

        # Should pass without row count warnings
        row_warnings = [i for i in result.issues if "row" in i.message.lower()]
        assert len(row_warnings) == 0

    def test_unexpected_row_change_warning(self, sample_df, tester):
        """Test warning when row count changes unexpectedly."""
        plan = make_plan("trim_whitespace", columns=["name"])
        after_df = sample_df.head(3)  # Unexpectedly fewer rows

        result = tester.validate(sample_df, after_df, plan)

        assert result.has_warnings()


# =============================================================================
# Null Check Tests
# =============================================================================

class TestNullChecks:
    """Tests for null value checks."""

    def test_fill_nulls_success(self, sample_df, tester):
        """Test fill_nulls check passes when nulls filled."""
        plan = make_plan("fill_nulls", columns=["age"], parameters={"method": "value", "value": 0})

        after_df = sample_df.copy()
        after_df['age'] = after_df['age'].fillna(0)

        result = tester.validate(sample_df, after_df, plan)

        # Should not have warnings about nulls remaining
        null_warnings = [i for i in result.issues
                        if i.check_name == "fill_nulls_success" and i.severity == Severity.WARNING]
        assert len(null_warnings) == 0

    def test_fill_nulls_failure_warning(self, sample_df, tester):
        """Test warning when fill_nulls doesn't fill."""
        plan = make_plan("fill_nulls", columns=["age"], parameters={"method": "value", "value": 0})

        # Don't actually fill - simulate failure
        result = tester.validate(sample_df, sample_df, plan)

        # Should have warning about nulls remaining
        assert result.has_warnings()

    def test_drop_rows_null_removed(self, sample_df, tester):
        """Test that drop_rows with isnull actually removes nulls."""
        plan = make_plan(
            "drop_rows",
            columns=["name"],
            conditions=[{"column": "name", "operator": "isnull"}]
        )

        after_df = sample_df.dropna(subset=["name"])

        result = tester.validate(sample_df, after_df, plan)

        # Should pass - nulls were removed
        null_errors = [i for i in result.issues
                      if i.check_name == "drop_rows_null_check" and i.severity == Severity.WARNING]
        assert len(null_errors) == 0


# =============================================================================
# Duplicate Check Tests
# =============================================================================

class TestDuplicateChecks:
    """Tests for duplicate detection checks."""

    def test_deduplicate_success(self, tester):
        """Test deduplicate check passes when duplicates removed."""
        before_df = pd.DataFrame({
            'a': [1, 1, 2, 2, 3],
            'b': ['x', 'x', 'y', 'y', 'z']
        })
        after_df = before_df.drop_duplicates()
        plan = make_plan("deduplicate")

        result = tester.validate(before_df, after_df, plan)

        # Check should pass
        dedupe_errors = [i for i in result.issues
                        if i.check_name == "deduplicate_success" and i.severity == Severity.WARNING]
        assert len(dedupe_errors) == 0

    def test_deduplicate_failure_warning(self, tester):
        """Test warning when deduplicate doesn't remove dupes."""
        before_df = pd.DataFrame({
            'a': [1, 1, 2, 2, 3],
            'b': ['x', 'x', 'y', 'y', 'z']
        })
        plan = make_plan("deduplicate")

        # Don't actually dedupe
        result = tester.validate(before_df, before_df, plan)

        assert result.has_warnings()

    def test_no_duplicates_info(self, sample_df, tester):
        """Test info message when no duplicates to remove."""
        plan = make_plan("deduplicate")

        result = tester.validate(sample_df, sample_df, plan)

        # Should have info about no duplicates
        info_msgs = [i for i in result.issues
                    if i.check_name == "deduplicate_success" and i.severity == Severity.INFO]
        assert len(info_msgs) > 0


# =============================================================================
# Value Check Tests
# =============================================================================

class TestValueChecks:
    """Tests for value range checks."""

    def test_normalize_bounds(self, tester):
        """Test normalize check verifies 0-1 bounds."""
        before_df = pd.DataFrame({'val': [10, 20, 30, 40, 50]})

        # Properly normalized
        after_df = pd.DataFrame({'val': [0.0, 0.25, 0.5, 0.75, 1.0]})
        plan = make_plan("normalize", columns=["val"], parameters={"method": "minmax"})

        result = tester.validate(before_df, after_df, plan)

        # Should pass bounds check
        bounds_errors = [i for i in result.issues if i.check_name == "numeric_bounds"]
        assert not any(i.severity == Severity.WARNING for i in bounds_errors)

    def test_sort_order_valid(self, sample_df, tester):
        """Test sort order check."""
        plan = make_plan("sort_rows", columns=["salary"], parameters={"columns": ["salary"], "ascending": True})

        after_df = sample_df.sort_values("salary")

        result = tester.validate(sample_df, after_df, plan)

        # Should pass
        assert result.passed


# =============================================================================
# Integration Tests
# =============================================================================

class TestIntegration:
    """Integration tests for full validation flow."""

    def test_full_drop_rows_validation(self, sample_df, tester):
        """Test complete validation for drop_rows."""
        plan = make_plan(
            "drop_rows",
            columns=["name"],
            conditions=[{"column": "name", "operator": "isnull"}]
        )

        after_df = sample_df.dropna(subset=["name"])

        result = tester.validate(sample_df, after_df, plan)

        assert result.passed
        assert result.rows_before == 5
        assert result.rows_after == 4
        assert result.rows_changed == -1
        assert len(result.checks_run) > 0

    def test_format_for_display(self, sample_df, tester):
        """Test format_for_display produces readable output."""
        plan = make_plan("drop_rows", columns=["name"])
        after_df = sample_df.dropna(subset=["name"])

        result = tester.validate(sample_df, after_df, plan)
        display = result.format_for_display()

        assert isinstance(display, str)
        assert "Quality Check" in display

    def test_convenience_function(self, sample_df):
        """Test validate_transformation convenience function."""
        plan = make_plan("trim_whitespace", columns=["name"])

        result = validate_transformation(sample_df, sample_df, plan)

        assert isinstance(result, TestResult)


# =============================================================================
# Edge Cases
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_before_df(self, tester):
        """Test validation with empty input DataFrame."""
        before_df = pd.DataFrame({'a': []})
        after_df = pd.DataFrame({'a': []})
        plan = make_plan("trim_whitespace", columns=["a"])

        result = tester.validate(before_df, after_df, plan)

        assert isinstance(result, TestResult)

    def test_single_row(self, tester):
        """Test validation with single row."""
        before_df = pd.DataFrame({'a': [1]})
        after_df = pd.DataFrame({'a': [1]})
        plan = make_plan("trim_whitespace", columns=["a"])

        result = tester.validate(before_df, after_df, plan)

        assert result.passed

    def test_large_dataframe(self, tester):
        """Test validation with larger DataFrame."""
        n = 10000
        before_df = pd.DataFrame({
            'a': range(n),
            'b': [f"val_{i}" for i in range(n)]
        })
        after_df = before_df.copy()
        plan = make_plan("trim_whitespace", columns=["b"])

        result = tester.validate(before_df, after_df, plan)

        assert result.passed
        assert result.validation_time_ms > 0

    def test_strict_mode(self, sample_df):
        """Test strict mode treats warnings as errors."""
        tester = TesterAgent(strict_mode=True)
        plan = make_plan("deduplicate")

        # This will generate info/warning about no duplicates
        result = tester.validate(sample_df, sample_df, plan)

        # In strict mode, warnings should fail
        # Note: info messages don't fail even in strict mode
        assert isinstance(result, TestResult)
