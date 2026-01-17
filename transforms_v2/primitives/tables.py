# =============================================================================
# transforms_v2/primitives/tables.py - Multi-Table Operations
# =============================================================================
# Primitives for combining tables: join, union, lookup.
# =============================================================================

from __future__ import annotations

from typing import Any

import pandas as pd

from transforms_v2.registry import register_primitive
from transforms_v2.types import (
    JoinType,
    ParamDef,
    Primitive,
    PrimitiveInfo,
    PrimitiveResult,
    TestPrompt,
)


# =============================================================================
# join_tables
# =============================================================================


@register_primitive
class JoinTables(Primitive):
    """Join two tables on specified columns."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="join_tables",
            category="tables",
            description="Join two tables using inner, left, right, or outer join",
            params=[
                ParamDef(
                    name="right_table",
                    type="pd.DataFrame",
                    required=True,
                    description="The table to join with",
                ),
                ParamDef(
                    name="left_on",
                    type="str | list[str]",
                    required=True,
                    description="Column(s) from the left table to join on",
                ),
                ParamDef(
                    name="right_on",
                    type="str | list[str]",
                    required=True,
                    description="Column(s) from the right table to join on",
                ),
                ParamDef(
                    name="how",
                    type="str",
                    required=False,
                    default="left",
                    description="Join type: 'inner', 'left', 'right', 'outer'",
                    choices=["inner", "left", "right", "outer"],
                ),
                ParamDef(
                    name="suffixes",
                    type="tuple[str, str]",
                    required=False,
                    default=("_left", "_right"),
                    description="Suffixes for overlapping column names",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Join orders with customers on customer_id",
                    expected_params={
                        "left_on": "customer_id",
                        "right_on": "customer_id",
                        "how": "left",
                    },
                    description="Left join on ID",
                ),
                TestPrompt(
                    prompt="Match products with inventory using product_code, keeping only matches",
                    expected_params={
                        "left_on": "product_code",
                        "right_on": "product_code",
                        "how": "inner",
                    },
                    description="Inner join",
                ),
                TestPrompt(
                    prompt="Combine employees and departments on dept_id, include all from both",
                    expected_params={
                        "left_on": "dept_id",
                        "right_on": "dept_id",
                        "how": "outer",
                    },
                    description="Outer join",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        right_table = params["right_table"]
        left_on = params["left_on"]
        right_on = params["right_on"]
        how = params.get("how", "left")
        suffixes = params.get("suffixes", ("_left", "_right"))

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate join columns exist
        left_cols = [left_on] if isinstance(left_on, str) else left_on
        right_cols = [right_on] if isinstance(right_on, str) else right_on

        missing_left = [c for c in left_cols if c not in df.columns]
        if missing_left:
            return PrimitiveResult(
                success=False,
                error=f"Left join columns not found: {missing_left}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        missing_right = [c for c in right_cols if c not in right_table.columns]
        if missing_right:
            return PrimitiveResult(
                success=False,
                error=f"Right join columns not found: {missing_right}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = pd.merge(
                df,
                right_table,
                left_on=left_on,
                right_on=right_on,
                how=how,
                suffixes=suffixes,
            )

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "join_type": how,
                    "rows_matched": len(result_df),
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
# union_tables
# =============================================================================


@register_primitive
class UnionTables(Primitive):
    """Stack tables vertically (union/append)."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="union_tables",
            category="tables",
            description="Stack multiple tables vertically, combining all rows",
            params=[
                ParamDef(
                    name="other_tables",
                    type="list[pd.DataFrame]",
                    required=True,
                    description="Tables to append to the main table",
                ),
                ParamDef(
                    name="ignore_index",
                    type="bool",
                    required=False,
                    default=True,
                    description="Reset index after union",
                ),
                ParamDef(
                    name="match_columns",
                    type="bool",
                    required=False,
                    default=True,
                    description="If True, only include columns present in all tables",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Combine January, February, and March sales data into one table",
                    expected_params={"ignore_index": True},
                    description="Stack monthly data",
                ),
                TestPrompt(
                    prompt="Append new customer records to the existing customer list",
                    expected_params={"ignore_index": True},
                    description="Append new records",
                ),
                TestPrompt(
                    prompt="Union all regional reports into a single dataset",
                    expected_params={"ignore_index": True, "match_columns": True},
                    description="Union regional data",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        other_tables = params["other_tables"]
        ignore_index = params.get("ignore_index", True)
        match_columns = params.get("match_columns", True)

        rows_before = len(df)
        cols_before = len(df.columns)

        if not other_tables:
            return PrimitiveResult(
                success=False,
                error="No tables provided to union",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            all_tables = [df] + other_tables

            if match_columns:
                # Find common columns
                common_cols = set(df.columns)
                for table in other_tables:
                    common_cols &= set(table.columns)
                common_cols = list(common_cols)

                if not common_cols:
                    return PrimitiveResult(
                        success=False,
                        error="No common columns found between tables",
                        rows_before=rows_before,
                        cols_before=cols_before,
                    )

                all_tables = [t[common_cols] for t in all_tables]

            result_df = pd.concat(all_tables, ignore_index=ignore_index)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "tables_combined": len(all_tables),
                    "total_rows_added": len(result_df) - rows_before,
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
# lookup
# =============================================================================


@register_primitive
class Lookup(Primitive):
    """VLOOKUP-style operation to bring in values from another table."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="lookup",
            category="tables",
            description="Look up values from another table (like Excel VLOOKUP)",
            params=[
                ParamDef(
                    name="lookup_table",
                    type="pd.DataFrame",
                    required=True,
                    description="Table to look up values from",
                ),
                ParamDef(
                    name="lookup_column",
                    type="str",
                    required=True,
                    description="Column in main table to match on",
                ),
                ParamDef(
                    name="lookup_key",
                    type="str",
                    required=True,
                    description="Column in lookup table to match against",
                ),
                ParamDef(
                    name="return_columns",
                    type="list[str]",
                    required=True,
                    description="Columns from lookup table to bring back",
                ),
                ParamDef(
                    name="default_value",
                    type="Any",
                    required=False,
                    default=None,
                    description="Value to use when no match is found",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Look up customer name from customer table using customer_id",
                    expected_params={
                        "lookup_column": "customer_id",
                        "lookup_key": "customer_id",
                        "return_columns": ["customer_name"],
                    },
                    description="Lookup customer name",
                ),
                TestPrompt(
                    prompt="Get product price and category from product catalog by product_code",
                    expected_params={
                        "lookup_column": "product_code",
                        "lookup_key": "product_code",
                        "return_columns": ["price", "category"],
                    },
                    description="Lookup multiple columns",
                ),
                TestPrompt(
                    prompt="Find employee department using employee_id, use 'Unknown' if not found",
                    expected_params={
                        "lookup_column": "employee_id",
                        "lookup_key": "employee_id",
                        "return_columns": ["department"],
                        "default_value": "Unknown",
                    },
                    description="Lookup with default",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        lookup_table = params["lookup_table"]
        lookup_column = params["lookup_column"]
        lookup_key = params["lookup_key"]
        return_columns = params["return_columns"]
        default_value = params.get("default_value")

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate columns
        if lookup_column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Lookup column '{lookup_column}' not found in main table",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if lookup_key not in lookup_table.columns:
            return PrimitiveResult(
                success=False,
                error=f"Lookup key '{lookup_key}' not found in lookup table",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        missing_return = [c for c in return_columns if c not in lookup_table.columns]
        if missing_return:
            return PrimitiveResult(
                success=False,
                error=f"Return columns not found in lookup table: {missing_return}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            # Create lookup dictionary for each return column
            result_df = df.copy()

            # Deduplicate lookup table by key (keep first)
            lookup_dedup = lookup_table.drop_duplicates(subset=[lookup_key], keep="first")

            # Create mapping for each return column
            for col in return_columns:
                mapping = dict(zip(lookup_dedup[lookup_key], lookup_dedup[col]))
                result_df[col] = result_df[lookup_column].map(mapping)

                if default_value is not None:
                    result_df[col] = result_df[col].fillna(default_value)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"columns_added": return_columns},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )
