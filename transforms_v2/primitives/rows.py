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


# =============================================================================
# sample_rows
# =============================================================================


@register_primitive
class SampleRows(Primitive):
    """Randomly sample rows from the DataFrame."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="sample_rows",
            category="rows",
            description="Randomly sample a subset of rows for testing or preview",
            params=[
                ParamDef(
                    name="n",
                    type="int",
                    required=False,
                    default=None,
                    description="Number of rows to sample (use n OR fraction, not both)",
                ),
                ParamDef(
                    name="fraction",
                    type="float",
                    required=False,
                    default=None,
                    description="Fraction of rows to sample (0.0 to 1.0)",
                ),
                ParamDef(
                    name="seed",
                    type="int",
                    required=False,
                    default=None,
                    description="Random seed for reproducibility",
                ),
                ParamDef(
                    name="replace",
                    type="bool",
                    required=False,
                    default=False,
                    description="Sample with replacement (allows duplicates)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Get a random sample of 100 rows",
                    expected_params={"n": 100},
                    description="Sample fixed count",
                ),
                TestPrompt(
                    prompt="Sample 10% of the data randomly",
                    expected_params={"fraction": 0.1},
                    description="Sample by fraction",
                ),
                TestPrompt(
                    prompt="Get 50 random rows with seed 42 for reproducibility",
                    expected_params={"n": 50, "seed": 42},
                    description="Sample with seed",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        n = params.get("n")
        fraction = params.get("fraction")
        seed = params.get("seed")
        replace = params.get("replace", False)

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate parameters
        if n is None and fraction is None:
            n = min(10, rows_before)  # Default: 10 rows or all if less

        if n is not None and fraction is not None:
            return PrimitiveResult(
                success=False,
                error="Specify either 'n' or 'fraction', not both",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            if fraction is not None:
                if not 0.0 < fraction <= 1.0:
                    return PrimitiveResult(
                        success=False,
                        error=f"Fraction must be between 0 and 1, got {fraction}",
                        rows_before=rows_before,
                        cols_before=cols_before,
                    )
                result_df = df.sample(frac=fraction, random_state=seed, replace=replace)
            else:
                # Cap n at number of rows (unless replace=True)
                if not replace and n > rows_before:
                    n = rows_before
                result_df = df.sample(n=n, random_state=seed, replace=replace)

            result_df = result_df.reset_index(drop=True)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "sampled_rows": len(result_df),
                    "sample_rate": len(result_df) / rows_before if rows_before > 0 else 0,
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
# offset_rows
# =============================================================================


@register_primitive
class OffsetRows(Primitive):
    """Skip the first N rows (offset/pagination)."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="offset_rows",
            category="rows",
            description="Skip the first N rows, useful for removing headers or pagination",
            params=[
                ParamDef(
                    name="offset",
                    type="int",
                    required=True,
                    description="Number of rows to skip from the beginning",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Skip the first 5 rows",
                    expected_params={"offset": 5},
                    description="Basic offset",
                ),
                TestPrompt(
                    prompt="Remove the header row (first row)",
                    expected_params={"offset": 1},
                    description="Skip header",
                ),
                TestPrompt(
                    prompt="Start from row 100 (skip first 99 rows)",
                    expected_params={"offset": 99},
                    description="Pagination offset",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        offset = params["offset"]

        rows_before = len(df)
        cols_before = len(df.columns)

        if offset < 0:
            return PrimitiveResult(
                success=False,
                error=f"Offset must be non-negative, got {offset}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.iloc[offset:].reset_index(drop=True)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"rows_skipped": min(offset, rows_before)},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# head_rows
# =============================================================================


@register_primitive
class HeadRows(Primitive):
    """Get the first N rows."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="head_rows",
            category="rows",
            description="Get the first N rows from the dataset",
            params=[
                ParamDef(
                    name="n",
                    type="int",
                    required=False,
                    default=10,
                    description="Number of rows to return (default: 10)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Show the first 20 rows",
                    expected_params={"n": 20},
                    description="Get first 20",
                ),
                TestPrompt(
                    prompt="Preview the top 5 records",
                    expected_params={"n": 5},
                    description="Preview top records",
                ),
                TestPrompt(
                    prompt="Get the head of the data",
                    expected_params={"n": 10},
                    description="Default head",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        n = params.get("n", 10)

        rows_before = len(df)
        cols_before = len(df.columns)

        if n < 0:
            return PrimitiveResult(
                success=False,
                error=f"n must be non-negative, got {n}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.head(n).reset_index(drop=True)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"rows_returned": len(result_df)},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# tail_rows
# =============================================================================


@register_primitive
class TailRows(Primitive):
    """Get the last N rows."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="tail_rows",
            category="rows",
            description="Get the last N rows from the dataset",
            params=[
                ParamDef(
                    name="n",
                    type="int",
                    required=False,
                    default=10,
                    description="Number of rows to return (default: 10)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Show the last 20 rows",
                    expected_params={"n": 20},
                    description="Get last 20",
                ),
                TestPrompt(
                    prompt="Get the bottom 5 records",
                    expected_params={"n": 5},
                    description="Bottom records",
                ),
                TestPrompt(
                    prompt="Show the tail of the data",
                    expected_params={"n": 10},
                    description="Default tail",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        n = params.get("n", 10)

        rows_before = len(df)
        cols_before = len(df.columns)

        if n < 0:
            return PrimitiveResult(
                success=False,
                error=f"n must be non-negative, got {n}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.tail(n).reset_index(drop=True)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"rows_returned": len(result_df)},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# shuffle_rows
# =============================================================================


@register_primitive
class ShuffleRows(Primitive):
    """Randomly shuffle the order of rows."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="shuffle_rows",
            category="rows",
            description="Randomly shuffle (reorder) all rows in the dataset",
            params=[
                ParamDef(
                    name="seed",
                    type="int",
                    required=False,
                    default=None,
                    description="Random seed for reproducibility",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Randomize the order of rows",
                    expected_params={},
                    description="Basic shuffle",
                ),
                TestPrompt(
                    prompt="Shuffle the data with seed 42",
                    expected_params={"seed": 42},
                    description="Shuffle with seed",
                ),
                TestPrompt(
                    prompt="Mix up the row order randomly",
                    expected_params={},
                    description="Mix rows",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        seed = params.get("seed")

        rows_before = len(df)
        cols_before = len(df.columns)

        try:
            result_df = df.sample(frac=1, random_state=seed).reset_index(drop=True)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"shuffled": True},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# row_number
# =============================================================================


@register_primitive
class RowNumber(Primitive):
    """Add a sequential row number column."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="row_number",
            category="rows",
            description="Add a column with sequential row numbers",
            params=[
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default="row_num",
                    description="Name for the row number column",
                ),
                ParamDef(
                    name="start",
                    type="int",
                    required=False,
                    default=1,
                    description="Starting number (default: 1)",
                ),
                ParamDef(
                    name="partition_by",
                    type="list[str]",
                    required=False,
                    default=None,
                    description="Reset numbering for each group (like SQL PARTITION BY)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Add row numbers to the data",
                    expected_params={"new_column": "row_num"},
                    description="Basic row numbers",
                ),
                TestPrompt(
                    prompt="Number the rows starting from 0 in a column called idx",
                    expected_params={"new_column": "idx", "start": 0},
                    description="Zero-based index",
                ),
                TestPrompt(
                    prompt="Add row numbers that reset for each department",
                    expected_params={"partition_by": ["department"]},
                    description="Partitioned row numbers",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        new_column = params.get("new_column", "row_num")
        start = params.get("start", 1)
        partition_by = params.get("partition_by")

        rows_before = len(df)
        cols_before = len(df.columns)

        try:
            result_df = df.copy()

            if partition_by:
                # Validate partition columns
                missing = [c for c in partition_by if c not in df.columns]
                if missing:
                    return PrimitiveResult(
                        success=False,
                        error=f"Partition columns not found: {missing}",
                        rows_before=rows_before,
                        cols_before=cols_before,
                    )
                # Row number within each partition
                result_df[new_column] = result_df.groupby(partition_by).cumcount() + start
            else:
                # Simple sequential row numbers
                result_df[new_column] = range(start, start + len(result_df))

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"start": start, "partitioned": partition_by is not None},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# explode_column
# =============================================================================


@register_primitive
class ExplodeColumn(Primitive):
    """Split delimited values into multiple rows."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="explode_column",
            category="rows",
            description="Split a column with delimited values into multiple rows",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column containing delimited values to explode",
                ),
                ParamDef(
                    name="delimiter",
                    type="str",
                    required=False,
                    default=",",
                    description="Delimiter to split on (default: comma)",
                ),
                ParamDef(
                    name="trim",
                    type="bool",
                    required=False,
                    default=True,
                    description="Trim whitespace from resulting values",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Split the tags column into separate rows",
                    expected_params={"column": "tags"},
                    description="Basic explode",
                ),
                TestPrompt(
                    prompt="Explode the categories column using semicolon as separator",
                    expected_params={"column": "categories", "delimiter": ";"},
                    description="Custom delimiter",
                ),
                TestPrompt(
                    prompt="Split the items column by pipe character",
                    expected_params={"column": "items", "delimiter": "|"},
                    description="Pipe delimiter",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        delimiter = params.get("delimiter", ",")
        trim = params.get("trim", True)

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

            # Split the column values
            result_df[column] = result_df[column].astype(str).str.split(delimiter)

            # Explode into multiple rows
            result_df = result_df.explode(column).reset_index(drop=True)

            # Trim whitespace if requested
            if trim:
                result_df[column] = result_df[column].str.strip()

            # Handle 'nan' strings from null values
            result_df.loc[result_df[column] == "nan", column] = None

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "rows_created": len(result_df) - rows_before,
                    "delimiter": delimiter,
                },
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )
