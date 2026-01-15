# =============================================================================
# tests/test_transformations.py - Transformation Function Tests
# =============================================================================
# Tests for all transformation functions in the Engineer agent.
# =============================================================================

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from agents.models.technical_plan import (
    TechnicalPlan,
    TransformationType,
    FilterCondition,
    FilterOperator,
    ColumnTarget,
)
from agents.transformations import get_transformer, REGISTRY


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def sample_df():
    """Sample DataFrame for testing."""
    return pd.DataFrame({
        'name': ['  Alice  ', 'Bob', '  Carol  ', 'David', 'Eve'],
        'email': ['alice@test.com', 'bob@test.com', None, 'david@test.com', 'eve@test.com'],
        'age': [25, 30, 35, None, 45],
        'salary': [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
        'department': ['Sales', 'Engineering', 'Sales', 'Engineering', 'Sales'],
        'hire_date': ['2020-01-15', '2019-06-20', '2021-03-10', '2018-11-05', '2022-07-01'],
    })


@pytest.fixture
def duplicate_df():
    """DataFrame with duplicates for testing."""
    return pd.DataFrame({
        'id': [1, 2, 2, 3, 3, 3],
        'name': ['A', 'B', 'B', 'C', 'C', 'C'],
        'value': [100, 200, 200, 300, 300, 300],
    })


def create_plan(
    trans_type: TransformationType,
    target_columns: list[str] = None,
    conditions: list[dict] = None,
    parameters: dict = None,
    explanation: str = "Test transformation",
) -> TechnicalPlan:
    """Helper to create TechnicalPlan instances."""
    targets = [ColumnTarget(column_name=c) for c in (target_columns or [])]
    conds = []
    if conditions:
        for c in conditions:
            conds.append(FilterCondition(
                column=c['column'],
                operator=FilterOperator(c['operator']),
                value=c.get('value'),
            ))

    return TechnicalPlan(
        transformation_type=trans_type,
        target_columns=targets,
        conditions=conds,
        parameters=parameters or {},
        explanation=explanation,
    )


# =============================================================================
# Registry Tests
# =============================================================================

class TestRegistry:
    """Tests for the transformation registry."""

    def test_registry_not_empty(self):
        """Registry should have registered transformations."""
        assert len(REGISTRY) > 0

    def test_get_transformer_returns_function(self):
        """get_transformer should return callable for valid type."""
        transformer = get_transformer(TransformationType.TRIM_WHITESPACE)
        assert transformer is not None
        assert callable(transformer)

    def test_get_transformer_returns_none_for_invalid(self):
        """get_transformer should return None for unknown type."""
        transformer = get_transformer("nonexistent_type")
        assert transformer is None


# =============================================================================
# Cleaning Operation Tests
# =============================================================================

class TestCleaningOperations:
    """Tests for cleaning transformations."""

    def test_trim_whitespace(self, sample_df):
        """trim_whitespace should remove leading/trailing spaces."""
        plan = create_plan(
            TransformationType.TRIM_WHITESPACE,
            target_columns=['name'],
        )
        transformer = get_transformer(TransformationType.TRIM_WHITESPACE)
        result, code = transformer(sample_df, plan)

        assert result['name'].iloc[0] == 'Alice'
        assert result['name'].iloc[2] == 'Carol'
        assert 'strip' in code.lower()

    def test_change_case_lower(self, sample_df):
        """change_case should convert to lowercase."""
        plan = create_plan(
            TransformationType.CHANGE_CASE,
            target_columns=['department'],
            parameters={'case_type': 'lower'},
        )
        transformer = get_transformer(TransformationType.CHANGE_CASE)
        result, code = transformer(sample_df, plan)

        assert result['department'].iloc[0] == 'sales'
        assert result['department'].iloc[1] == 'engineering'

    def test_change_case_upper(self, sample_df):
        """change_case should convert to uppercase."""
        plan = create_plan(
            TransformationType.CHANGE_CASE,
            target_columns=['department'],
            parameters={'case_type': 'upper'},
        )
        transformer = get_transformer(TransformationType.CHANGE_CASE)
        result, code = transformer(sample_df, plan)

        assert result['department'].iloc[0] == 'SALES'

    def test_change_case_title(self, sample_df):
        """change_case should convert to title case."""
        df = pd.DataFrame({'name': ['alice smith', 'BOB JONES']})
        plan = create_plan(
            TransformationType.CHANGE_CASE,
            target_columns=['name'],
            parameters={'case_type': 'title'},
        )
        transformer = get_transformer(TransformationType.CHANGE_CASE)
        result, code = transformer(df, plan)

        assert result['name'].iloc[0] == 'Alice Smith'
        assert result['name'].iloc[1] == 'Bob Jones'

    def test_deduplicate_keep_first(self, duplicate_df):
        """deduplicate should remove duplicates keeping first."""
        plan = create_plan(
            TransformationType.DEDUPLICATE,
            target_columns=['id'],
            parameters={'keep': 'first'},
        )
        transformer = get_transformer(TransformationType.DEDUPLICATE)
        result, code = transformer(duplicate_df, plan)

        assert len(result) == 3
        assert list(result['id']) == [1, 2, 3]

    def test_deduplicate_keep_last(self, duplicate_df):
        """deduplicate should keep last occurrence."""
        plan = create_plan(
            TransformationType.DEDUPLICATE,
            target_columns=['id'],
            parameters={'keep': 'last'},
        )
        transformer = get_transformer(TransformationType.DEDUPLICATE)
        result, code = transformer(duplicate_df, plan)

        assert len(result) == 3

    def test_replace_values_simple(self, sample_df):
        """replace_values should replace exact values."""
        plan = create_plan(
            TransformationType.REPLACE_VALUES,
            target_columns=['department'],
            parameters={'old_value': 'Sales', 'new_value': 'Revenue'},
        )
        transformer = get_transformer(TransformationType.REPLACE_VALUES)
        result, code = transformer(sample_df, plan)

        assert 'Sales' not in result['department'].values
        assert 'Revenue' in result['department'].values

    def test_replace_values_regex(self):
        """replace_values should support regex patterns."""
        df = pd.DataFrame({'phone': ['555-1234', '555-5678', '123-4567']})
        plan = create_plan(
            TransformationType.REPLACE_VALUES,
            target_columns=['phone'],
            parameters={
                'old_value': r'^555-',
                'new_value': '800-',
                'regex': True,
            },
        )
        transformer = get_transformer(TransformationType.REPLACE_VALUES)
        result, code = transformer(df, plan)

        assert result['phone'].iloc[0] == '800-1234'
        assert result['phone'].iloc[2] == '123-4567'  # Not changed

    def test_fill_nulls_with_value(self, sample_df):
        """fill_nulls should fill with static value."""
        plan = create_plan(
            TransformationType.FILL_NULLS,
            target_columns=['email'],
            parameters={'method': 'value', 'value': 'unknown@test.com'},
        )
        transformer = get_transformer(TransformationType.FILL_NULLS)
        result, code = transformer(sample_df, plan)

        assert result['email'].isna().sum() == 0
        assert result['email'].iloc[2] == 'unknown@test.com'

    def test_fill_nulls_with_mean(self, sample_df):
        """fill_nulls should fill numeric columns with mean."""
        plan = create_plan(
            TransformationType.FILL_NULLS,
            target_columns=['age'],
            parameters={'method': 'mean'},
        )
        transformer = get_transformer(TransformationType.FILL_NULLS)
        result, code = transformer(sample_df, plan)

        assert result['age'].isna().sum() == 0
        # Mean of [25, 30, 35, 45] = 33.75
        assert result['age'].iloc[3] == pytest.approx(33.75, rel=0.01)

    def test_sanitize_headers_snake_case(self):
        """sanitize_headers should convert to snake_case."""
        df = pd.DataFrame({
            'First Name': [1],
            'Last Name': [2],
            'Email Address': [3],
        })
        plan = create_plan(
            TransformationType.SANITIZE_HEADERS,
            parameters={'style': 'snake_case'},
        )
        transformer = get_transformer(TransformationType.SANITIZE_HEADERS)
        result, code = transformer(df, plan)

        assert 'first_name' in result.columns
        assert 'last_name' in result.columns
        assert 'email_address' in result.columns


# =============================================================================
# Filtering Operation Tests
# =============================================================================

class TestFilteringOperations:
    """Tests for filtering transformations."""

    def test_filter_rows_equals(self, sample_df):
        """filter_rows should keep rows matching condition."""
        plan = create_plan(
            TransformationType.FILTER_ROWS,
            conditions=[{'column': 'department', 'operator': 'eq', 'value': 'Sales'}],
        )
        transformer = get_transformer(TransformationType.FILTER_ROWS)
        result, code = transformer(sample_df, plan)

        assert len(result) == 3
        assert all(result['department'] == 'Sales')

    def test_drop_rows_isnull(self, sample_df):
        """drop_rows should remove rows with null values."""
        plan = create_plan(
            TransformationType.DROP_ROWS,
            conditions=[{'column': 'email', 'operator': 'isnull'}],
        )
        transformer = get_transformer(TransformationType.DROP_ROWS)
        result, code = transformer(sample_df, plan)

        assert len(result) == 4
        assert result['email'].isna().sum() == 0

    def test_filter_rows_greater_than(self, sample_df):
        """filter_rows should handle numeric comparisons."""
        plan = create_plan(
            TransformationType.FILTER_ROWS,
            conditions=[{'column': 'salary', 'operator': 'gt', 'value': 60000}],
        )
        transformer = get_transformer(TransformationType.FILTER_ROWS)
        result, code = transformer(sample_df, plan)

        assert len(result) == 3
        assert all(result['salary'] > 60000)

    def test_filter_rows_contains(self, sample_df):
        """filter_rows should handle string contains."""
        plan = create_plan(
            TransformationType.FILTER_ROWS,
            conditions=[{'column': 'email', 'operator': 'contains', 'value': 'test'}],
        )
        transformer = get_transformer(TransformationType.FILTER_ROWS)
        result, code = transformer(sample_df, plan)

        # All non-null emails contain 'test'
        assert len(result) == 4

    def test_sort_rows_ascending(self, sample_df):
        """sort_rows should sort in ascending order."""
        plan = create_plan(
            TransformationType.SORT_ROWS,
            target_columns=['salary'],
            parameters={'ascending': True},
        )
        transformer = get_transformer(TransformationType.SORT_ROWS)
        result, code = transformer(sample_df, plan)

        assert result['salary'].iloc[0] == 50000
        assert result['salary'].iloc[-1] == 90000

    def test_sort_rows_descending(self, sample_df):
        """sort_rows should sort in descending order."""
        plan = create_plan(
            TransformationType.SORT_ROWS,
            target_columns=['salary'],
            parameters={'ascending': False},
        )
        transformer = get_transformer(TransformationType.SORT_ROWS)
        result, code = transformer(sample_df, plan)

        assert result['salary'].iloc[0] == 90000

    def test_select_columns(self, sample_df):
        """select_columns should keep only specified columns."""
        plan = create_plan(
            TransformationType.SELECT_COLUMNS,
            target_columns=['name', 'email'],
        )
        transformer = get_transformer(TransformationType.SELECT_COLUMNS)
        result, code = transformer(sample_df, plan)

        assert list(result.columns) == ['name', 'email']

    def test_drop_columns(self, sample_df):
        """drop_columns should remove specified columns."""
        plan = create_plan(
            TransformationType.DROP_COLUMNS,
            target_columns=['salary', 'hire_date'],
        )
        transformer = get_transformer(TransformationType.DROP_COLUMNS)
        result, code = transformer(sample_df, plan)

        assert 'salary' not in result.columns
        assert 'hire_date' not in result.columns
        assert 'name' in result.columns

    def test_slice_rows_head(self, sample_df):
        """slice_rows should return first N rows."""
        plan = create_plan(
            TransformationType.SLICE_ROWS,
            parameters={'n': 3, 'position': 'head'},
        )
        transformer = get_transformer(TransformationType.SLICE_ROWS)
        result, code = transformer(sample_df, plan)

        assert len(result) == 3

    def test_slice_rows_tail(self, sample_df):
        """slice_rows should return last N rows."""
        plan = create_plan(
            TransformationType.SLICE_ROWS,
            parameters={'n': 2, 'position': 'tail'},
        )
        transformer = get_transformer(TransformationType.SLICE_ROWS)
        result, code = transformer(sample_df, plan)

        assert len(result) == 2


# =============================================================================
# Restructuring Operation Tests
# =============================================================================

class TestRestructuringOperations:
    """Tests for restructuring transformations."""

    def test_split_column(self):
        """split_column should split on delimiter."""
        df = pd.DataFrame({'full_name': ['John Doe', 'Jane Smith', 'Bob Wilson']})
        plan = create_plan(
            TransformationType.SPLIT_COLUMN,
            target_columns=['full_name'],
            parameters={
                'delimiter': ' ',
                'new_columns': ['first_name', 'last_name'],
            },
        )
        transformer = get_transformer(TransformationType.SPLIT_COLUMN)
        result, code = transformer(df, plan)

        assert 'first_name' in result.columns
        assert 'last_name' in result.columns
        assert result['first_name'].iloc[0] == 'John'
        assert result['last_name'].iloc[0] == 'Doe'

    def test_merge_columns(self, sample_df):
        """merge_columns should combine columns."""
        df = pd.DataFrame({
            'first': ['John', 'Jane'],
            'last': ['Doe', 'Smith'],
        })
        plan = create_plan(
            TransformationType.MERGE_COLUMNS,
            target_columns=['first', 'last'],
            parameters={'separator': ' ', 'new_column_name': 'full_name'},
        )
        transformer = get_transformer(TransformationType.MERGE_COLUMNS)
        result, code = transformer(df, plan)

        assert 'full_name' in result.columns
        assert result['full_name'].iloc[0] == 'John Doe'

    def test_reorder_columns(self, sample_df):
        """reorder_columns should change column order."""
        plan = create_plan(
            TransformationType.REORDER_COLUMNS,
            parameters={'order': ['email', 'name', 'age']},
        )
        transformer = get_transformer(TransformationType.REORDER_COLUMNS)
        result, code = transformer(sample_df, plan)

        assert list(result.columns) == ['email', 'name', 'age']

    def test_rename_column(self, sample_df):
        """rename_column should rename columns."""
        plan = create_plan(
            TransformationType.RENAME_COLUMN,
            target_columns=['name'],
            parameters={'new_name': 'full_name'},
        )
        transformer = get_transformer(TransformationType.RENAME_COLUMN)
        result, code = transformer(sample_df, plan)

        assert 'full_name' in result.columns
        assert 'name' not in result.columns


# =============================================================================
# Column Math Operation Tests
# =============================================================================

class TestColumnMathOperations:
    """Tests for column math transformations."""

    def test_add_column_arithmetic(self, sample_df):
        """add_column should create calculated column."""
        plan = create_plan(
            TransformationType.ADD_COLUMN,
            parameters={
                'name': 'annual_bonus',
                'expression': "df['salary'] * 0.1",
            },
        )
        transformer = get_transformer(TransformationType.ADD_COLUMN)
        result, code = transformer(sample_df, plan)

        assert 'annual_bonus' in result.columns
        assert result['annual_bonus'].iloc[0] == 5000.0

    def test_convert_type_to_int(self):
        """convert_type should convert to integer."""
        df = pd.DataFrame({'value': ['1', '2', '3', '4']})
        plan = create_plan(
            TransformationType.CONVERT_TYPE,
            target_columns=['value'],
            parameters={'target_type': 'int'},
        )
        transformer = get_transformer(TransformationType.CONVERT_TYPE)
        result, code = transformer(df, plan)

        assert pd.api.types.is_integer_dtype(result['value'])

    def test_round_numbers(self, sample_df):
        """round_numbers should round to specified decimals."""
        df = pd.DataFrame({'value': [1.234, 5.678, 9.101]})
        plan = create_plan(
            TransformationType.ROUND_NUMBERS,
            target_columns=['value'],
            parameters={'decimals': 1},
        )
        transformer = get_transformer(TransformationType.ROUND_NUMBERS)
        result, code = transformer(df, plan)

        assert result['value'].iloc[0] == 1.2
        assert result['value'].iloc[1] == 5.7

    def test_normalize_minmax(self):
        """normalize should scale to 0-1 range."""
        df = pd.DataFrame({'value': [0, 50, 100]})
        plan = create_plan(
            TransformationType.NORMALIZE,
            target_columns=['value'],
            parameters={'method': 'minmax'},
        )
        transformer = get_transformer(TransformationType.NORMALIZE)
        result, code = transformer(df, plan)

        assert result['value'].iloc[0] == 0.0
        assert result['value'].iloc[1] == 0.5
        assert result['value'].iloc[2] == 1.0

    def test_extract_pattern_email_domain(self):
        """extract_pattern should extract regex groups."""
        df = pd.DataFrame({'email': ['user@gmail.com', 'test@yahoo.com']})
        plan = create_plan(
            TransformationType.EXTRACT_PATTERN,
            target_columns=['email'],
            parameters={
                'pattern': r'@(\w+)\.',
                'new_column': 'domain',
            },
        )
        transformer = get_transformer(TransformationType.EXTRACT_PATTERN)
        result, code = transformer(df, plan)

        assert 'domain' in result.columns
        assert result['domain'].iloc[0] == 'gmail'
        assert result['domain'].iloc[1] == 'yahoo'

    def test_parse_date(self, sample_df):
        """parse_date should convert strings to datetime."""
        plan = create_plan(
            TransformationType.PARSE_DATE,
            target_columns=['hire_date'],
            parameters={'format': '%Y-%m-%d'},
        )
        transformer = get_transformer(TransformationType.PARSE_DATE)
        result, code = transformer(sample_df, plan)

        assert pd.api.types.is_datetime64_any_dtype(result['hire_date'])

    def test_format_date(self, sample_df):
        """format_date should convert datetime to string format."""
        # First parse the dates
        df = sample_df.copy()
        df['hire_date'] = pd.to_datetime(df['hire_date'])

        plan = create_plan(
            TransformationType.FORMAT_DATE,
            target_columns=['hire_date'],
            parameters={'output_format': '%m/%d/%Y'},
        )
        transformer = get_transformer(TransformationType.FORMAT_DATE)
        result, code = transformer(df, plan)

        assert result['hire_date'].iloc[0] == '01/15/2020'


# =============================================================================
# Aggregation Operation Tests
# =============================================================================

class TestAggregationOperations:
    """Tests for aggregation transformations."""

    def test_group_by_sum(self, sample_df):
        """group_by should aggregate by groups."""
        plan = create_plan(
            TransformationType.GROUP_BY,
            parameters={
                'by_columns': ['department'],
                'aggregations': {'salary': 'sum'},
            },
        )
        transformer = get_transformer(TransformationType.GROUP_BY)
        result, code = transformer(sample_df, plan)

        assert len(result) == 2  # Sales and Engineering
        sales_total = result[result['department'] == 'Sales']['salary'].iloc[0]
        assert sales_total == 210000  # 50000 + 70000 + 90000

    def test_cumulative_sum(self, sample_df):
        """cumulative should calculate running totals."""
        plan = create_plan(
            TransformationType.CUMULATIVE,
            target_columns=['salary'],
            parameters={'operation': 'sum'},
        )
        transformer = get_transformer(TransformationType.CUMULATIVE)
        result, code = transformer(sample_df, plan)

        assert 'salary_cumsum' in result.columns
        assert result['salary_cumsum'].iloc[0] == 50000
        assert result['salary_cumsum'].iloc[1] == 110000  # 50000 + 60000


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_dataframe(self):
        """Transformations should handle empty DataFrames."""
        df = pd.DataFrame()
        plan = create_plan(TransformationType.TRIM_WHITESPACE)
        transformer = get_transformer(TransformationType.TRIM_WHITESPACE)
        result, code = transformer(df, plan)

        assert len(result) == 0

    def test_single_row_dataframe(self):
        """Transformations should handle single-row DataFrames."""
        df = pd.DataFrame({'name': ['Alice'], 'age': [25]})
        plan = create_plan(
            TransformationType.TRIM_WHITESPACE,
            target_columns=['name'],
        )
        transformer = get_transformer(TransformationType.TRIM_WHITESPACE)
        result, code = transformer(df, plan)

        assert len(result) == 1

    def test_all_null_column(self):
        """fill_nulls should handle all-null columns."""
        df = pd.DataFrame({'value': [None, None, None]})
        plan = create_plan(
            TransformationType.FILL_NULLS,
            target_columns=['value'],
            parameters={'method': 'value', 'value': 0},
        )
        transformer = get_transformer(TransformationType.FILL_NULLS)
        result, code = transformer(df, plan)

        assert result['value'].isna().sum() == 0

    def test_transformation_returns_code(self, sample_df):
        """All transformations should return code strings."""
        plan = create_plan(
            TransformationType.TRIM_WHITESPACE,
            target_columns=['name'],
        )
        transformer = get_transformer(TransformationType.TRIM_WHITESPACE)
        result, code = transformer(sample_df, plan)

        assert isinstance(code, str)
        assert len(code) > 0
