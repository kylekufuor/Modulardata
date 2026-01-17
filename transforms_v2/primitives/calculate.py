# =============================================================================
# transforms_v2/primitives/calculate.py - Mathematical Operations
# =============================================================================
# Primitives for calculations: math operations, rounding, percentages, etc.
# =============================================================================

from __future__ import annotations

from typing import Any

import pandas as pd
import numpy as np

from transforms_v2.registry import register_primitive
from transforms_v2.types import (
    ParamDef,
    Primitive,
    PrimitiveInfo,
    PrimitiveResult,
    TestPrompt,
)


# =============================================================================
# math_operation
# =============================================================================


@register_primitive
class MathOperation(Primitive):
    """Perform mathematical operations between columns or with constants."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="math_operation",
            category="calculate",
            description="Add, subtract, multiply, or divide columns or apply constants",
            params=[
                ParamDef(
                    name="new_column",
                    type="str",
                    required=True,
                    description="Name for the result column",
                ),
                ParamDef(
                    name="operation",
                    type="str",
                    required=True,
                    description="Operation: 'add', 'subtract', 'multiply', 'divide'",
                    choices=["add", "subtract", "multiply", "divide"],
                ),
                ParamDef(
                    name="column1",
                    type="str",
                    required=True,
                    description="First column (or left operand)",
                ),
                ParamDef(
                    name="column2",
                    type="str",
                    required=False,
                    default=None,
                    description="Second column (if operating between columns)",
                ),
                ParamDef(
                    name="value",
                    type="float",
                    required=False,
                    default=None,
                    description="Constant value (if applying to single column)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Calculate total by multiplying quantity and price into a new column called total",
                    expected_params={
                        "new_column": "total",
                        "operation": "multiply",
                        "column1": "quantity",
                        "column2": "price",
                    },
                    description="Multiply two columns",
                ),
                TestPrompt(
                    prompt="Add a 10% markup to the cost column and store in final_price",
                    expected_params={
                        "new_column": "final_price",
                        "operation": "multiply",
                        "column1": "cost",
                        "value": 1.1,
                    },
                    description="Multiply by constant",
                ),
                TestPrompt(
                    prompt="Calculate profit by subtracting cost from revenue",
                    expected_params={
                        "new_column": "profit",
                        "operation": "subtract",
                        "column1": "revenue",
                        "column2": "cost",
                    },
                    description="Subtract columns",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        new_column = params["new_column"]
        operation = params["operation"]
        column1 = params["column1"]
        column2 = params.get("column2")
        value = params.get("value")

        rows_before = len(df)
        cols_before = len(df.columns)

        if column1 not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column1}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if column2 and column2 not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column2}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if not column2 and value is None:
            return PrimitiveResult(
                success=False,
                error="Either column2 or value must be specified",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()

        try:
            col1 = pd.to_numeric(result_df[column1], errors="coerce")
            operand = pd.to_numeric(result_df[column2], errors="coerce") if column2 else value

            if operation == "add":
                result_df[new_column] = col1 + operand
            elif operation == "subtract":
                result_df[new_column] = col1 - operand
            elif operation == "multiply":
                result_df[new_column] = col1 * operand
            elif operation == "divide":
                # Handle division by zero
                if column2:
                    result_df[new_column] = col1 / operand.replace(0, np.nan)
                else:
                    if value == 0:
                        return PrimitiveResult(
                            success=False,
                            error="Cannot divide by zero",
                            rows_before=rows_before,
                            cols_before=cols_before,
                        )
                    result_df[new_column] = col1 / value

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
# round_numbers
# =============================================================================


@register_primitive
class RoundNumbers(Primitive):
    """Round numeric values to specified decimal places."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="round_numbers",
            category="calculate",
            description="Round numeric values to a specified number of decimal places",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to round",
                ),
                ParamDef(
                    name="decimals",
                    type="int",
                    required=False,
                    default=0,
                    description="Number of decimal places (0 for whole numbers)",
                ),
                ParamDef(
                    name="method",
                    type="str",
                    required=False,
                    default="round",
                    description="Rounding method: 'round', 'floor', 'ceil'",
                    choices=["round", "floor", "ceil"],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Round the price column to 2 decimal places",
                    expected_params={"column": "price", "decimals": 2},
                    description="Round to cents",
                ),
                TestPrompt(
                    prompt="Round down all values in quantity to whole numbers",
                    expected_params={"column": "quantity", "decimals": 0, "method": "floor"},
                    description="Floor to integers",
                ),
                TestPrompt(
                    prompt="Round up the shipping_cost to the nearest dollar",
                    expected_params={"column": "shipping_cost", "decimals": 0, "method": "ceil"},
                    description="Ceiling to whole number",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        decimals = params.get("decimals", 0)
        method = params.get("method", "round")

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
            numeric_col = pd.to_numeric(result_df[column], errors="coerce")

            if method == "round":
                result_df[column] = numeric_col.round(decimals)
            elif method == "floor":
                factor = 10 ** decimals
                result_df[column] = np.floor(numeric_col * factor) / factor
            elif method == "ceil":
                factor = 10 ** decimals
                result_df[column] = np.ceil(numeric_col * factor) / factor

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
# percentage
# =============================================================================


@register_primitive
class Percentage(Primitive):
    """Calculate percentage of total or between columns."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="percentage",
            category="calculate",
            description="Calculate percentage of column total or ratio between columns",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to calculate percentage for",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=True,
                    description="Name for the percentage column",
                ),
                ParamDef(
                    name="mode",
                    type="str",
                    required=False,
                    default="of_total",
                    description="Mode: 'of_total' (% of column sum) or 'ratio' (column / denominator)",
                    choices=["of_total", "ratio"],
                ),
                ParamDef(
                    name="denominator_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Denominator column for 'ratio' mode",
                ),
                ParamDef(
                    name="multiply_by_100",
                    type="bool",
                    required=False,
                    default=True,
                    description="Whether to multiply by 100 (True: 50%, False: 0.5)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Calculate what percentage each sale is of total sales",
                    expected_params={
                        "column": "sale_amount",
                        "new_column": "percent_of_total",
                        "mode": "of_total",
                    },
                    description="Percentage of total",
                ),
                TestPrompt(
                    prompt="Calculate completion rate as completed divided by total",
                    expected_params={
                        "column": "completed",
                        "new_column": "completion_rate",
                        "mode": "ratio",
                        "denominator_column": "total",
                    },
                    description="Ratio between columns",
                ),
                TestPrompt(
                    prompt="Show each region's contribution as a percentage of total revenue",
                    expected_params={
                        "column": "revenue",
                        "new_column": "revenue_pct",
                        "mode": "of_total",
                    },
                    description="Revenue contribution",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        new_column = params["new_column"]
        mode = params.get("mode", "of_total")
        denominator_column = params.get("denominator_column")
        multiply_by_100 = params.get("multiply_by_100", True)

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if mode == "ratio" and not denominator_column:
            return PrimitiveResult(
                success=False,
                error="denominator_column required for 'ratio' mode",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if denominator_column and denominator_column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Denominator column '{denominator_column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()

        try:
            numeric_col = pd.to_numeric(result_df[column], errors="coerce")

            if mode == "of_total":
                total = numeric_col.sum()
                if total == 0:
                    result_df[new_column] = 0
                else:
                    result_df[new_column] = numeric_col / total
            else:  # ratio
                denom = pd.to_numeric(result_df[denominator_column], errors="coerce")
                result_df[new_column] = numeric_col / denom.replace(0, np.nan)

            if multiply_by_100:
                result_df[new_column] = result_df[new_column] * 100

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
# running_total
# =============================================================================


@register_primitive
class RunningTotal(Primitive):
    """Calculate cumulative sum."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="running_total",
            category="calculate",
            description="Calculate a running/cumulative total for a numeric column",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to calculate running total for",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=True,
                    description="Name for the running total column",
                ),
                ParamDef(
                    name="group_by",
                    type="str",
                    required=False,
                    default=None,
                    description="Optional column to reset running total for each group",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Add a running total column for the amount field",
                    expected_params={"column": "amount", "new_column": "running_total"},
                    description="Simple running total",
                ),
                TestPrompt(
                    prompt="Calculate cumulative sales for each month",
                    expected_params={"column": "sales", "new_column": "cumulative_sales"},
                    description="Cumulative sum",
                ),
                TestPrompt(
                    prompt="Create a running balance grouped by account_id",
                    expected_params={
                        "column": "transaction_amount",
                        "new_column": "running_balance",
                        "group_by": "account_id",
                    },
                    description="Running total by group",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        new_column = params["new_column"]
        group_by = params.get("group_by")

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if group_by and group_by not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Group by column '{group_by}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()

        try:
            numeric_col = pd.to_numeric(result_df[column], errors="coerce")

            if group_by:
                result_df[new_column] = numeric_col.groupby(result_df[group_by]).cumsum()
            else:
                result_df[new_column] = numeric_col.cumsum()

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
# rank
# =============================================================================


@register_primitive
class Rank(Primitive):
    """Rank rows by column value."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="rank",
            category="calculate",
            description="Assign rank to rows based on column values",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to rank by",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=True,
                    description="Name for the rank column",
                ),
                ParamDef(
                    name="ascending",
                    type="bool",
                    required=False,
                    default=True,
                    description="If True, lowest value = rank 1; if False, highest = rank 1",
                ),
                ParamDef(
                    name="method",
                    type="str",
                    required=False,
                    default="dense",
                    description="Ranking method: 'dense', 'min', 'max', 'first', 'average'",
                    choices=["dense", "min", "max", "first", "average"],
                ),
                ParamDef(
                    name="group_by",
                    type="str",
                    required=False,
                    default=None,
                    description="Optional column to rank within groups",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Rank employees by salary from highest to lowest",
                    expected_params={
                        "column": "salary",
                        "new_column": "salary_rank",
                        "ascending": False,
                    },
                    description="Rank descending",
                ),
                TestPrompt(
                    prompt="Add a rank column for scores, lowest first",
                    expected_params={
                        "column": "score",
                        "new_column": "rank",
                        "ascending": True,
                    },
                    description="Rank ascending",
                ),
                TestPrompt(
                    prompt="Rank products by sales within each category",
                    expected_params={
                        "column": "sales",
                        "new_column": "category_rank",
                        "ascending": False,
                        "group_by": "category",
                    },
                    description="Rank within groups",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        new_column = params["new_column"]
        ascending = params.get("ascending", True)
        method = params.get("method", "dense")
        group_by = params.get("group_by")

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if group_by and group_by not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Group by column '{group_by}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()

        try:
            if group_by:
                result_df[new_column] = result_df.groupby(group_by)[column].rank(
                    ascending=ascending, method=method
                )
            else:
                result_df[new_column] = result_df[column].rank(
                    ascending=ascending, method=method
                )

            # Convert to integer if using dense ranking
            if method == "dense":
                result_df[new_column] = result_df[new_column].astype("Int64")

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
# conditional_value
# =============================================================================


@register_primitive
class ConditionalValue(Primitive):
    """Set values based on conditions (like Excel IF)."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="conditional_value",
            category="calculate",
            description="Set column values based on if/then conditions",
            params=[
                ParamDef(
                    name="new_column",
                    type="str",
                    required=True,
                    description="Name for the result column",
                ),
                ParamDef(
                    name="conditions",
                    type="list[dict]",
                    required=True,
                    description="List of {condition, value} pairs. Each condition has column, operator, compare_value.",
                ),
                ParamDef(
                    name="default",
                    type="Any",
                    required=False,
                    default=None,
                    description="Default value if no conditions match",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Create a grade column: A if score >= 90, B if >= 80, C if >= 70, else F",
                    expected_params={
                        "new_column": "grade",
                        "conditions": [
                            {"column": "score", "operator": "gte", "compare_value": 90, "value": "A"},
                            {"column": "score", "operator": "gte", "compare_value": 80, "value": "B"},
                            {"column": "score", "operator": "gte", "compare_value": 70, "value": "C"},
                        ],
                        "default": "F",
                    },
                    description="Multiple conditions with grades",
                ),
                TestPrompt(
                    prompt="Add a status column: 'High' if amount > 1000, otherwise 'Low'",
                    expected_params={
                        "new_column": "status",
                        "conditions": [
                            {"column": "amount", "operator": "gt", "compare_value": 1000, "value": "High"},
                        ],
                        "default": "Low",
                    },
                    description="Simple if/else",
                ),
                TestPrompt(
                    prompt="Create a tier column based on customer spend",
                    expected_params={
                        "new_column": "tier",
                        "conditions": [
                            {"column": "total_spend", "operator": "gte", "compare_value": 10000, "value": "Platinum"},
                            {"column": "total_spend", "operator": "gte", "compare_value": 5000, "value": "Gold"},
                            {"column": "total_spend", "operator": "gte", "compare_value": 1000, "value": "Silver"},
                        ],
                        "default": "Bronze",
                    },
                    description="Customer tier assignment",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        new_column = params["new_column"]
        conditions = params["conditions"]
        default = params.get("default")

        rows_before = len(df)
        cols_before = len(df.columns)

        result_df = df.copy()

        try:
            # Start with default value
            result_df[new_column] = default

            # Apply conditions in reverse order (so first condition has priority)
            for cond in reversed(conditions):
                col = cond.get("column")
                operator = cond.get("operator")
                compare_value = cond.get("compare_value")
                value = cond.get("value")

                if col not in df.columns:
                    return PrimitiveResult(
                        success=False,
                        error=f"Column '{col}' not found",
                        rows_before=rows_before,
                        cols_before=cols_before,
                    )

                # Build mask based on operator
                col_data = result_df[col]

                if operator == "eq":
                    mask = col_data == compare_value
                elif operator == "ne":
                    mask = col_data != compare_value
                elif operator == "gt":
                    mask = pd.to_numeric(col_data, errors="coerce") > compare_value
                elif operator == "lt":
                    mask = pd.to_numeric(col_data, errors="coerce") < compare_value
                elif operator == "gte":
                    mask = pd.to_numeric(col_data, errors="coerce") >= compare_value
                elif operator == "lte":
                    mask = pd.to_numeric(col_data, errors="coerce") <= compare_value
                elif operator == "contains":
                    mask = col_data.astype(str).str.contains(str(compare_value), na=False)
                elif operator == "isnull":
                    mask = col_data.isna()
                elif operator == "notnull":
                    mask = col_data.notna()
                else:
                    mask = pd.Series([False] * len(df))

                result_df.loc[mask, new_column] = value

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
