# =============================================================================
# transforms_v2/primitives/rows.py - Row Operations
# =============================================================================
# Primitives that operate on rows: sort, filter, limit, dedupe, merge, fill.
# =============================================================================

from __future__ import annotations

from typing import Any

import pandas as pd

from transforms_v2.registry import register_primitive
from transforms_v2.types import (
    Condition,
    FillMethod,
    FilterOperator,
    ParamDef,
    Primitive,
    PrimitiveInfo,
    PrimitiveResult,
    TestPrompt,
)


# =============================================================================
# sort_rows
# =============================================================================


@register_primitive
class SortRows(Primitive):
    """Sort rows by one or more columns."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="sort_rows",
            category="rows",
            description="Sort rows by one or more columns in ascending or descending order",
            params=[
                ParamDef(
                    name="columns",
                    type="list[str]",
                    required=True,
                    description="Column(s) to sort by",
                ),
                ParamDef(
                    name="ascending",
                    type="bool | list[bool]",
                    required=False,
                    default=True,
                    description="Sort ascending (True) or descending (False). Can be a list for multiple columns.",
                ),
                ParamDef(
                    name="na_position",
                    type="str",
                    required=False,
                    default="last",
                    description="Where to put NaN values: 'first' or 'last'",
                    choices=["first", "last"],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Sort the data by lead_score from highest to lowest",
                    expected_params={"columns": ["lead_score"], "ascending": False},
                    description="Sort descending by numeric column",
                ),
                TestPrompt(
                    prompt="Arrange rows alphabetically by last name",
                    expected_params={"columns": ["last_name"], "ascending": True},
                    description="Sort ascending by text column",
                ),
                TestPrompt(
                    prompt="Order by created_date newest first, then by name A-Z",
                    expected_params={
                        "columns": ["created_date", "name"],
                        "ascending": [False, True],
                    },
                    description="Multi-column sort with mixed directions",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        columns = params["columns"]
        ascending = params.get("ascending", True)
        na_position = params.get("na_position", "last")

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate columns exist
        missing = [c for c in columns if c not in df.columns]
        if missing:
            return PrimitiveResult(
                success=False,
                error=f"Columns not found: {missing}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.sort_values(
                by=columns,
                ascending=ascending,
                na_position=na_position,
            ).reset_index(drop=True)

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
# filter_rows
# =============================================================================


@register_primitive
class FilterRows(Primitive):
    """Filter rows based on conditions."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="filter_rows",
            category="rows",
            description="Keep or remove rows based on filter conditions",
            params=[
                ParamDef(
                    name="conditions",
                    type="list[Condition]",
                    required=True,
                    description="List of filter conditions to apply",
                ),
                ParamDef(
                    name="logic",
                    type="str",
                    required=False,
                    default="and",
                    description="How to combine conditions: 'and' or 'or'",
                    choices=["and", "or"],
                ),
                ParamDef(
                    name="keep",
                    type="bool",
                    required=False,
                    default=True,
                    description="If True, keep matching rows. If False, remove them.",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Show only rows where lead_score is greater than 80",
                    expected_params={
                        "conditions": [{"column": "lead_score", "operator": "gt", "value": 80}],
                        "keep": True,
                    },
                    description="Simple numeric filter",
                ),
                TestPrompt(
                    prompt="Remove all rows where email is empty or null",
                    expected_params={
                        "conditions": [{"column": "email", "operator": "isnull"}],
                        "keep": False,
                    },
                    description="Remove rows with null values",
                ),
                TestPrompt(
                    prompt="Keep rows where status is either 'active' or 'pending'",
                    expected_params={
                        "conditions": [{"column": "status", "operator": "in", "value": ["active", "pending"]}],
                        "keep": True,
                    },
                    description="Filter by list of values",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        conditions_raw = params["conditions"]
        logic = params.get("logic", "and")
        keep = params.get("keep", True)

        rows_before = len(df)
        cols_before = len(df.columns)

        # Parse conditions
        conditions = []
        for c in conditions_raw:
            if isinstance(c, Condition):
                conditions.append(c)
            elif isinstance(c, dict):
                conditions.append(Condition.from_dict(c))
            else:
                return PrimitiveResult(
                    success=False,
                    error=f"Invalid condition format: {c}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )

        try:
            # Build masks for each condition
            masks = []
            for cond in conditions:
                mask = self._apply_condition(df, cond)
                if mask is None:
                    return PrimitiveResult(
                        success=False,
                        error=f"Column '{cond.column}' not found",
                        rows_before=rows_before,
                        cols_before=cols_before,
                    )
                masks.append(mask)

            # Combine masks
            if logic == "and":
                combined = masks[0]
                for m in masks[1:]:
                    combined = combined & m
            else:  # or
                combined = masks[0]
                for m in masks[1:]:
                    combined = combined | m

            # Apply keep/remove logic
            if not keep:
                combined = ~combined

            result_df = df[combined].reset_index(drop=True)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"rows_filtered": rows_before - len(result_df)},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )

    def _apply_condition(self, df: pd.DataFrame, cond: Condition) -> pd.Series | None:
        """Apply a single condition and return a boolean mask."""
        if cond.column not in df.columns:
            return None

        col = df[cond.column]
        op = cond.operator
        val = cond.value

        # Handle case sensitivity for string operations
        if not cond.case_sensitive and col.dtype == object:
            col = col.str.lower()
            if isinstance(val, str):
                val = val.lower()
            elif isinstance(val, list):
                val = [v.lower() if isinstance(v, str) else v for v in val]

        if op == FilterOperator.EQ:
            return col == val
        elif op == FilterOperator.NE:
            return col != val
        elif op == FilterOperator.GT:
            return col > val
        elif op == FilterOperator.LT:
            return col < val
        elif op == FilterOperator.GTE:
            return col >= val
        elif op == FilterOperator.LTE:
            return col <= val
        elif op == FilterOperator.ISNULL:
            return col.isna()
        elif op == FilterOperator.NOTNULL:
            return col.notna()
        elif op == FilterOperator.CONTAINS:
            return col.str.contains(val, case=cond.case_sensitive, na=False)
        elif op == FilterOperator.STARTSWITH:
            return col.str.startswith(val, na=False)
        elif op == FilterOperator.ENDSWITH:
            return col.str.endswith(val, na=False)
        elif op == FilterOperator.REGEX:
            return col.str.match(val, case=cond.case_sensitive, na=False)
        elif op == FilterOperator.IN:
            return col.isin(val)
        elif op == FilterOperator.NOT_IN:
            return ~col.isin(val)
        else:
            return None


# =============================================================================
# limit_rows
# =============================================================================


@register_primitive
class LimitRows(Primitive):
    """Limit to first or last N rows."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="limit_rows",
            category="rows",
            description="Keep only the first or last N rows",
            params=[
                ParamDef(
                    name="count",
                    type="int",
                    required=True,
                    description="Number of rows to keep",
                ),
                ParamDef(
                    name="from_end",
                    type="bool",
                    required=False,
                    default=False,
                    description="If True, take from end instead of beginning",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Keep only the first 100 rows",
                    expected_params={"count": 100, "from_end": False},
                    description="Limit to first N rows",
                ),
                TestPrompt(
                    prompt="Get the last 50 records",
                    expected_params={"count": 50, "from_end": True},
                    description="Get last N rows",
                ),
                TestPrompt(
                    prompt="Show me the top 10 entries",
                    expected_params={"count": 10, "from_end": False},
                    description="Top N (first N)",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        count = params["count"]
        from_end = params.get("from_end", False)

        rows_before = len(df)
        cols_before = len(df.columns)

        if count < 0:
            return PrimitiveResult(
                success=False,
                error="Count must be non-negative",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if from_end:
            result_df = df.tail(count).reset_index(drop=True)
        else:
            result_df = df.head(count).reset_index(drop=True)

        return PrimitiveResult(
            success=True,
            df=result_df,
            rows_before=rows_before,
            rows_after=len(result_df),
            cols_before=cols_before,
            cols_after=len(result_df.columns),
        )


# =============================================================================
# remove_duplicates
# =============================================================================


@register_primitive
class RemoveDuplicates(Primitive):
    """Remove duplicate rows."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="remove_duplicates",
            category="rows",
            description="Remove duplicate rows, keeping the first or last occurrence",
            params=[
                ParamDef(
                    name="subset",
                    type="list[str] | None",
                    required=False,
                    default=None,
                    description="Columns to check for duplicates. None means all columns.",
                ),
                ParamDef(
                    name="keep",
                    type="str",
                    required=False,
                    default="first",
                    description="Which duplicate to keep: 'first', 'last', or 'none' (remove all)",
                    choices=["first", "last", "none"],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Remove duplicate rows based on email address",
                    expected_params={"subset": ["email"], "keep": "first"},
                    description="Dedupe by single column",
                ),
                TestPrompt(
                    prompt="Delete all duplicate entries, keeping the most recent one",
                    expected_params={"subset": None, "keep": "last"},
                    description="Dedupe all columns, keep last",
                ),
                TestPrompt(
                    prompt="Remove rows where lead_id and email are duplicated",
                    expected_params={"subset": ["lead_id", "email"], "keep": "first"},
                    description="Dedupe by multiple columns",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        subset = params.get("subset")
        keep = params.get("keep", "first")

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate subset columns
        if subset:
            missing = [c for c in subset if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )

        # Handle 'none' -> False for pandas
        keep_arg = False if keep == "none" else keep

        try:
            result_df = df.drop_duplicates(
                subset=subset,
                keep=keep_arg,
            ).reset_index(drop=True)

            duplicates_removed = rows_before - len(result_df)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"duplicates_removed": duplicates_removed},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# merge_duplicates
# =============================================================================


@register_primitive
class MergeDuplicates(Primitive):
    """Merge duplicate rows by aggregating values."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="merge_duplicates",
            category="rows",
            description="Merge duplicate rows by combining values (sum, concat, max, etc.)",
            params=[
                ParamDef(
                    name="group_by",
                    type="list[str]",
                    required=True,
                    description="Columns that define duplicates (group by these)",
                ),
                ParamDef(
                    name="aggregations",
                    type="dict[str, str]",
                    required=False,
                    default=None,
                    description="How to aggregate each column: {col: 'sum'|'mean'|'max'|'min'|'first'|'last'|'concat'}",
                ),
                ParamDef(
                    name="default_agg",
                    type="str",
                    required=False,
                    default="first",
                    description="Default aggregation for columns not specified",
                    choices=["first", "last", "sum", "mean", "max", "min", "concat"],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Combine duplicate orders by customer_id, summing the amounts",
                    expected_params={
                        "group_by": ["customer_id"],
                        "aggregations": {"amount": "sum"},
                    },
                    description="Group by ID and sum values",
                ),
                TestPrompt(
                    prompt="Merge duplicate contacts by email, keeping the most recent activity_date",
                    expected_params={
                        "group_by": ["email"],
                        "aggregations": {"activity_date": "max"},
                    },
                    description="Group by email and keep max date",
                ),
                TestPrompt(
                    prompt="Consolidate rows with same product_id, concatenating all notes",
                    expected_params={
                        "group_by": ["product_id"],
                        "aggregations": {"notes": "concat"},
                    },
                    description="Group and concat text",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        group_by = params["group_by"]
        aggregations = params.get("aggregations", {})
        default_agg = params.get("default_agg", "first")

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate group_by columns
        missing = [c for c in group_by if c not in df.columns]
        if missing:
            return PrimitiveResult(
                success=False,
                error=f"Group by columns not found: {missing}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            # Build aggregation dict for all non-group columns
            agg_dict = {}
            for col in df.columns:
                if col in group_by:
                    continue
                agg_method = aggregations.get(col, default_agg)
                if agg_method == "concat":
                    agg_dict[col] = lambda x: ", ".join(str(v) for v in x if pd.notna(v))
                else:
                    agg_dict[col] = agg_method

            result_df = df.groupby(group_by, as_index=False).agg(agg_dict)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"groups_created": len(result_df)},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# fill_blanks
# =============================================================================


@register_primitive
class FillBlanks(Primitive):
    """Fill null/blank values in columns."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="fill_blanks",
            category="rows",
            description="Fill null or blank values using various methods",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to fill blanks in",
                ),
                ParamDef(
                    name="method",
                    type="str",
                    required=True,
                    description="Fill method: 'value', 'mean', 'median', 'mode', 'forward', 'backward'",
                    choices=["value", "mean", "median", "mode", "forward", "backward"],
                ),
                ParamDef(
                    name="value",
                    type="Any",
                    required=False,
                    default=None,
                    description="Value to fill with (only used when method='value')",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Fill empty lead_score values with 0",
                    expected_params={"column": "lead_score", "method": "value", "value": 0},
                    description="Fill with constant value",
                ),
                TestPrompt(
                    prompt="Replace missing prices with the average price",
                    expected_params={"column": "price", "method": "mean"},
                    description="Fill with mean",
                ),
                TestPrompt(
                    prompt="Fill blank status fields with 'Unknown'",
                    expected_params={"column": "status", "method": "value", "value": "Unknown"},
                    description="Fill text with constant",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        method = params["method"]
        fill_value = params.get("value")

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
        blanks_before = result_df[column].isna().sum()

        try:
            if method == "value":
                result_df[column] = result_df[column].fillna(fill_value)
            elif method == "mean":
                mean_val = result_df[column].mean()
                result_df[column] = result_df[column].fillna(mean_val)
            elif method == "median":
                median_val = result_df[column].median()
                result_df[column] = result_df[column].fillna(median_val)
            elif method == "mode":
                mode_val = result_df[column].mode()
                if len(mode_val) > 0:
                    result_df[column] = result_df[column].fillna(mode_val.iloc[0])
            elif method == "forward":
                result_df[column] = result_df[column].ffill()
            elif method == "backward":
                result_df[column] = result_df[column].bfill()

            blanks_after = result_df[column].isna().sum()

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "blanks_filled": int(blanks_before - blanks_after),
                    "blanks_remaining": int(blanks_after),
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
# add_rows
# =============================================================================


@register_primitive
class AddRows(Primitive):
    """Add new rows to the DataFrame."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="add_rows",
            category="rows",
            description="Add new rows to the top or bottom of the data",
            params=[
                ParamDef(
                    name="rows",
                    type="list[dict]",
                    required=True,
                    description="List of row dictionaries to add",
                ),
                ParamDef(
                    name="position",
                    type="str",
                    required=False,
                    default="bottom",
                    description="Where to add rows: 'top' or 'bottom'",
                    choices=["top", "bottom"],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Add a new row with name='Test User' and email='test@example.com'",
                    expected_params={
                        "rows": [{"name": "Test User", "email": "test@example.com"}],
                        "position": "bottom",
                    },
                    description="Add single row at bottom",
                ),
                TestPrompt(
                    prompt="Insert a header row at the top with column descriptions",
                    expected_params={
                        "rows": [{"description": "header row data"}],
                        "position": "top",
                    },
                    description="Add row at top",
                ),
                TestPrompt(
                    prompt="Append three new product entries to the list",
                    expected_params={
                        "rows": [{"product": "entry1"}, {"product": "entry2"}, {"product": "entry3"}],
                        "position": "bottom",
                    },
                    description="Add multiple rows",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        rows = params["rows"]
        position = params.get("position", "bottom")

        rows_before = len(df)
        cols_before = len(df.columns)

        try:
            new_df = pd.DataFrame(rows)

            if position == "top":
                result_df = pd.concat([new_df, df], ignore_index=True)
            else:
                result_df = pd.concat([df, new_df], ignore_index=True)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"rows_added": len(rows)},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )
