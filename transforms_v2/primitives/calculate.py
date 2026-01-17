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


# =============================================================================
# floor_ceil
# =============================================================================


@register_primitive
class FloorCeil(Primitive):
    """Apply floor or ceiling to numeric values."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="floor_ceil",
            category="calculate",
            description="Round numbers down (floor) or up (ceiling)",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to apply floor/ceil to",
                ),
                ParamDef(
                    name="method",
                    type="str",
                    required=True,
                    description="Method: 'floor' (round down) or 'ceil' (round up)",
                    choices=["floor", "ceil"],
                ),
                ParamDef(
                    name="precision",
                    type="int",
                    required=False,
                    default=0,
                    description="Decimal places (0 = integer, 1 = one decimal, etc.)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Round down all prices to the nearest integer",
                    expected_params={
                        "column": "price",
                        "method": "floor",
                        "precision": 0,
                    },
                    description="Floor to integer",
                ),
                TestPrompt(
                    prompt="Round up the amount column to the nearest whole number",
                    expected_params={
                        "column": "amount",
                        "method": "ceil",
                        "precision": 0,
                    },
                    description="Ceil to integer",
                ),
                TestPrompt(
                    prompt="Floor values to one decimal place",
                    expected_params={
                        "column": "value",
                        "method": "floor",
                        "precision": 1,
                    },
                    description="Floor to one decimal",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        method = params["method"]
        precision = params.get("precision", 0)

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
            import numpy as np

            # Convert to numeric
            numeric_col = pd.to_numeric(result_df[column], errors="coerce")

            # Apply multiplier for precision
            multiplier = 10 ** precision

            if method == "floor":
                result_df[column] = np.floor(numeric_col * multiplier) / multiplier
            elif method == "ceil":
                result_df[column] = np.ceil(numeric_col * multiplier) / multiplier
            else:
                return PrimitiveResult(
                    success=False,
                    error=f"Unknown method: {method}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )

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
# bin_values
# =============================================================================


@register_primitive
class BinValues(Primitive):
    """Create bins/buckets from numeric values."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="bin_values",
            category="calculate",
            description="Group numeric values into bins/buckets (like age ranges, price tiers)",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Numeric column to bin",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for the bin column (default: column_bin)",
                ),
                ParamDef(
                    name="bins",
                    type="int | list[float]",
                    required=True,
                    description="Number of equal-width bins OR list of bin edges [0, 10, 20, 50, 100]",
                ),
                ParamDef(
                    name="labels",
                    type="list[str]",
                    required=False,
                    default=None,
                    description="Labels for each bin (must be len(bins)-1 if bins is a list)",
                ),
                ParamDef(
                    name="include_lowest",
                    type="bool",
                    required=False,
                    default=True,
                    description="Include the lowest edge in the first bin",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Create 5 equal age groups from the age column",
                    expected_params={
                        "column": "age",
                        "bins": 5,
                    },
                    description="Equal-width bins",
                ),
                TestPrompt(
                    prompt="Bin ages into groups: 0-18, 18-35, 35-55, 55+",
                    expected_params={
                        "column": "age",
                        "bins": [0, 18, 35, 55, 100],
                        "labels": ["Youth", "Young Adult", "Middle Age", "Senior"],
                    },
                    description="Custom bins with labels",
                ),
                TestPrompt(
                    prompt="Create price tiers: Low (0-50), Medium (50-100), High (100+)",
                    expected_params={
                        "column": "price",
                        "bins": [0, 50, 100, float("inf")],
                        "labels": ["Low", "Medium", "High"],
                    },
                    description="Price tiers",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        bins = params["bins"]
        new_column = params.get("new_column") or f"{column}_bin"
        labels = params.get("labels")
        include_lowest = params.get("include_lowest", True)

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

            # Convert to numeric
            numeric_col = pd.to_numeric(result_df[column], errors="coerce")

            # Handle infinity in bins (replace with max/min values)
            if isinstance(bins, list):
                bins = [
                    float(numeric_col.min()) - 1 if b == float("-inf") else
                    float(numeric_col.max()) + 1 if b == float("inf") else b
                    for b in bins
                ]

            # Create bins
            result_df[new_column] = pd.cut(
                numeric_col,
                bins=bins,
                labels=labels,
                include_lowest=include_lowest,
            )

            # Convert to string for easier handling
            result_df[new_column] = result_df[new_column].astype(str)
            result_df.loc[result_df[new_column] == "nan", new_column] = None

            # Count bins
            bin_counts = result_df[new_column].value_counts().to_dict()

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"bin_counts": bin_counts},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# absolute_value
# =============================================================================


@register_primitive
class AbsoluteValue(Primitive):
    """Calculate the absolute value of a numeric column."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="absolute_value",
            category="calculate",
            description="Convert negative values to positive (absolute value)",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to apply absolute value to",
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
                    prompt="Get the absolute value of the balance column",
                    expected_params={"column": "balance"},
                    description="Basic absolute value",
                ),
                TestPrompt(
                    prompt="Convert all negative amounts to positive, store in abs_amount",
                    expected_params={"column": "amount", "new_column": "abs_amount"},
                    description="Absolute value to new column",
                ),
                TestPrompt(
                    prompt="Remove negative signs from the difference column",
                    expected_params={"column": "difference"},
                    description="Remove negatives",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
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

            # Convert to numeric and apply abs
            numeric_col = pd.to_numeric(result_df[column], errors="coerce")
            result_df[new_column] = numeric_col.abs()

            # Count negatives converted
            negatives_count = int((numeric_col < 0).sum())

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"negatives_converted": negatives_count},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# is_between
# =============================================================================


@register_primitive
class IsBetween(Primitive):
    """Check if values fall within a specified range."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="is_between",
            category="calculate",
            description="Create a boolean column indicating if values are within a range",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to check",
                ),
                ParamDef(
                    name="min_value",
                    type="float",
                    required=True,
                    description="Minimum value of range (inclusive by default)",
                ),
                ParamDef(
                    name="max_value",
                    type="float",
                    required=True,
                    description="Maximum value of range (inclusive by default)",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for result column (default: column_in_range)",
                ),
                ParamDef(
                    name="inclusive",
                    type="str",
                    required=False,
                    default="both",
                    description="Include boundaries: 'both', 'neither', 'left', 'right'",
                    choices=["both", "neither", "left", "right"],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Check if age is between 18 and 65",
                    expected_params={
                        "column": "age",
                        "min_value": 18,
                        "max_value": 65,
                    },
                    description="Basic range check",
                ),
                TestPrompt(
                    prompt="Flag prices between 10 and 100 as in_budget",
                    expected_params={
                        "column": "price",
                        "min_value": 10,
                        "max_value": 100,
                        "new_column": "in_budget",
                    },
                    description="Range with custom column name",
                ),
                TestPrompt(
                    prompt="Check if score is strictly between 0 and 100 (not including endpoints)",
                    expected_params={
                        "column": "score",
                        "min_value": 0,
                        "max_value": 100,
                        "inclusive": "neither",
                    },
                    description="Exclusive range",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        min_value = params["min_value"]
        max_value = params["max_value"]
        new_column = params.get("new_column") or f"{column}_in_range"
        inclusive = params.get("inclusive", "both")

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

            # Convert to numeric
            numeric_col = pd.to_numeric(result_df[column], errors="coerce")

            # Apply between with the specified inclusivity
            result_df[new_column] = numeric_col.between(
                min_value, max_value, inclusive=inclusive
            )

            # Count matches
            in_range_count = int(result_df[new_column].sum())

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "in_range_count": in_range_count,
                    "out_of_range_count": rows_before - in_range_count,
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
# case_when
# =============================================================================


@register_primitive
class CaseWhen(Primitive):
    """Apply multiple conditions to create values (like SQL CASE WHEN)."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="case_when",
            category="calculate",
            description="Assign values based on multiple conditions (like SQL CASE WHEN)",
            params=[
                ParamDef(
                    name="cases",
                    type="list[dict]",
                    required=True,
                    description="List of {condition: {column, operator, value}, result: value} pairs",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=True,
                    description="Name for the result column",
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
                    prompt="Create a tier column: 'Gold' if amount > 1000, 'Silver' if amount > 500, else 'Bronze'",
                    expected_params={
                        "cases": [
                            {"condition": {"column": "amount", "operator": "gt", "value": 1000}, "result": "Gold"},
                            {"condition": {"column": "amount", "operator": "gt", "value": 500}, "result": "Silver"},
                        ],
                        "new_column": "tier",
                        "default": "Bronze",
                    },
                    description="Tiered classification",
                ),
                TestPrompt(
                    prompt="Set status_label to 'Active' if status='active', 'Pending' if status='pending', else 'Other'",
                    expected_params={
                        "cases": [
                            {"condition": {"column": "status", "operator": "eq", "value": "active"}, "result": "Active"},
                            {"condition": {"column": "status", "operator": "eq", "value": "pending"}, "result": "Pending"},
                        ],
                        "new_column": "status_label",
                        "default": "Other",
                    },
                    description="Status mapping",
                ),
                TestPrompt(
                    prompt="Create age_group: 'Minor' if age < 18, 'Adult' if age < 65, 'Senior' otherwise",
                    expected_params={
                        "cases": [
                            {"condition": {"column": "age", "operator": "lt", "value": 18}, "result": "Minor"},
                            {"condition": {"column": "age", "operator": "lt", "value": 65}, "result": "Adult"},
                        ],
                        "new_column": "age_group",
                        "default": "Senior",
                    },
                    description="Age grouping",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        cases = params["cases"]
        new_column = params["new_column"]
        default = params.get("default")

        rows_before = len(df)
        cols_before = len(df.columns)

        try:
            result_df = df.copy()

            # Initialize with default value
            result_df[new_column] = default

            # Apply cases in reverse order (last match wins, but we want first match)
            # So we apply from last to first
            for case in reversed(cases):
                condition = case.get("condition", {})
                result_value = case.get("result")

                col_name = condition.get("column")
                operator = condition.get("operator", "eq")
                value = condition.get("value")

                if col_name not in df.columns:
                    return PrimitiveResult(
                        success=False,
                        error=f"Column '{col_name}' not found",
                        rows_before=rows_before,
                        cols_before=cols_before,
                    )

                col = result_df[col_name]

                # Build mask based on operator
                if operator == "eq":
                    mask = col == value
                elif operator == "ne":
                    mask = col != value
                elif operator == "gt":
                    mask = pd.to_numeric(col, errors="coerce") > value
                elif operator == "lt":
                    mask = pd.to_numeric(col, errors="coerce") < value
                elif operator == "gte":
                    mask = pd.to_numeric(col, errors="coerce") >= value
                elif operator == "lte":
                    mask = pd.to_numeric(col, errors="coerce") <= value
                elif operator == "contains":
                    mask = col.astype(str).str.contains(str(value), case=False, na=False)
                elif operator == "isnull":
                    mask = col.isna()
                elif operator == "notnull":
                    mask = col.notna()
                elif operator == "in":
                    mask = col.isin(value if isinstance(value, list) else [value])
                else:
                    return PrimitiveResult(
                        success=False,
                        error=f"Unknown operator: {operator}",
                        rows_before=rows_before,
                        cols_before=cols_before,
                    )

                result_df.loc[mask, new_column] = result_value

            # Count how many matched each case (for metadata)
            case_counts = {}
            for i, case in enumerate(cases):
                result_value = case.get("result")
                case_counts[f"case_{i}_{result_value}"] = int((result_df[new_column] == result_value).sum())

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "cases_applied": len(cases),
                    "default_count": int((result_df[new_column] == default).sum()) if default is not None else 0,
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
# dense_rank
# =============================================================================


@register_primitive
class DenseRank(Primitive):
    """Assign dense rank (no gaps) to rows based on column values."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="dense_rank",
            category="calculate",
            description="Assign dense rank (1,2,3 with no gaps for ties) like SQL DENSE_RANK()",
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
                    required=False,
                    default=None,
                    description="Name for the rank column (default: column_dense_rank)",
                ),
                ParamDef(
                    name="ascending",
                    type="bool",
                    required=False,
                    default=True,
                    description="If True, lowest value = rank 1; if False, highest = rank 1",
                ),
                ParamDef(
                    name="partition_by",
                    type="str | list[str]",
                    required=False,
                    default=None,
                    description="Column(s) to partition/group by (like SQL PARTITION BY)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Assign dense rank to employees by salary (highest first)",
                    expected_params={
                        "column": "salary",
                        "ascending": False,
                    },
                    description="Dense rank by salary descending",
                ),
                TestPrompt(
                    prompt="Dense rank products by sales within each category",
                    expected_params={
                        "column": "sales",
                        "partition_by": "category",
                        "ascending": False,
                    },
                    description="Partitioned dense rank",
                ),
                TestPrompt(
                    prompt="Add a dense rank column for scores from lowest to highest",
                    expected_params={
                        "column": "score",
                        "ascending": True,
                    },
                    description="Ascending dense rank",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        new_column = params.get("new_column") or f"{column}_dense_rank"
        ascending = params.get("ascending", True)
        partition_by = params.get("partition_by")

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Validate partition columns
        if partition_by:
            partition_cols = [partition_by] if isinstance(partition_by, str) else partition_by
            missing = [c for c in partition_cols if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Partition columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )
        else:
            partition_cols = None

        try:
            result_df = df.copy()

            if partition_cols:
                result_df[new_column] = result_df.groupby(partition_cols)[column].rank(
                    method="dense", ascending=ascending
                ).astype("Int64")
            else:
                result_df[new_column] = result_df[column].rank(
                    method="dense", ascending=ascending
                ).astype("Int64")

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "unique_ranks": int(result_df[new_column].nunique()),
                    "partitioned": partition_cols is not None,
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
# ntile
# =============================================================================


@register_primitive
class Ntile(Primitive):
    """Divide rows into N equal buckets (like SQL NTILE)."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="ntile",
            category="calculate",
            description="Divide rows into N approximately equal buckets (like SQL NTILE)",
            params=[
                ParamDef(
                    name="n",
                    type="int",
                    required=True,
                    description="Number of buckets to divide into",
                ),
                ParamDef(
                    name="order_by",
                    type="str",
                    required=True,
                    description="Column to order by before dividing into buckets",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for the bucket column (default: ntile_N)",
                ),
                ParamDef(
                    name="ascending",
                    type="bool",
                    required=False,
                    default=True,
                    description="Order direction (True = lowest values in bucket 1)",
                ),
                ParamDef(
                    name="partition_by",
                    type="str | list[str]",
                    required=False,
                    default=None,
                    description="Column(s) to partition by before bucketing",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Divide customers into 4 quartiles by purchase amount",
                    expected_params={
                        "n": 4,
                        "order_by": "purchase_amount",
                    },
                    description="Quartiles",
                ),
                TestPrompt(
                    prompt="Create 10 decile buckets for scores",
                    expected_params={
                        "n": 10,
                        "order_by": "score",
                        "new_column": "score_decile",
                    },
                    description="Deciles",
                ),
                TestPrompt(
                    prompt="Split employees into 3 groups by salary within each department",
                    expected_params={
                        "n": 3,
                        "order_by": "salary",
                        "partition_by": "department",
                    },
                    description="Partitioned ntile",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        n = params["n"]
        order_by = params["order_by"]
        new_column = params.get("new_column") or f"ntile_{n}"
        ascending = params.get("ascending", True)
        partition_by = params.get("partition_by")

        rows_before = len(df)
        cols_before = len(df.columns)

        if order_by not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{order_by}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if n < 1:
            return PrimitiveResult(
                success=False,
                error="n must be at least 1",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Validate partition columns
        if partition_by:
            partition_cols = [partition_by] if isinstance(partition_by, str) else partition_by
            missing = [c for c in partition_cols if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Partition columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )
        else:
            partition_cols = None

        try:
            result_df = df.copy()

            # Sort the dataframe
            result_df = result_df.sort_values(by=order_by, ascending=ascending)

            if partition_cols:
                # NTILE within each partition
                def apply_ntile(group):
                    size = len(group)
                    return pd.Series(
                        [int(i * n / size) + 1 for i in range(size)],
                        index=group.index
                    )
                result_df[new_column] = result_df.groupby(partition_cols, group_keys=False).apply(
                    lambda g: pd.Series([int(i * n / len(g)) + 1 for i in range(len(g))], index=g.index)
                )
            else:
                # Simple NTILE
                size = len(result_df)
                result_df[new_column] = [int(i * n / size) + 1 for i in range(size)]

            # Restore original order
            result_df = result_df.sort_index()

            # Get bucket distribution
            bucket_counts = result_df[new_column].value_counts().sort_index().to_dict()

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "buckets": n,
                    "bucket_counts": bucket_counts,
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
# lag
# =============================================================================


@register_primitive
class Lag(Primitive):
    """Get value from previous row (like SQL LAG)."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="lag",
            category="calculate",
            description="Get value from N rows before current row (like SQL LAG)",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to get lagged values from",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for the lagged value column (default: column_lag_N)",
                ),
                ParamDef(
                    name="offset",
                    type="int",
                    required=False,
                    default=1,
                    description="Number of rows to look back (default: 1)",
                ),
                ParamDef(
                    name="default",
                    type="Any",
                    required=False,
                    default=None,
                    description="Value to use when there's no previous row",
                ),
                ParamDef(
                    name="partition_by",
                    type="str | list[str]",
                    required=False,
                    default=None,
                    description="Column(s) to partition by",
                ),
                ParamDef(
                    name="order_by",
                    type="str",
                    required=False,
                    default=None,
                    description="Column to order by before applying lag",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Get the previous day's closing price",
                    expected_params={
                        "column": "close_price",
                        "new_column": "prev_close",
                        "offset": 1,
                    },
                    description="Previous value",
                ),
                TestPrompt(
                    prompt="Get last month's sales for each product",
                    expected_params={
                        "column": "sales",
                        "partition_by": "product_id",
                        "order_by": "month",
                    },
                    description="Partitioned lag",
                ),
                TestPrompt(
                    prompt="Get the value from 3 rows ago in amount column",
                    expected_params={
                        "column": "amount",
                        "offset": 3,
                    },
                    description="Multi-row lag",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        offset = params.get("offset", 1)
        new_column = params.get("new_column") or f"{column}_lag_{offset}"
        default = params.get("default")
        partition_by = params.get("partition_by")
        order_by = params.get("order_by")

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Validate partition and order columns
        if partition_by:
            partition_cols = [partition_by] if isinstance(partition_by, str) else partition_by
            missing = [c for c in partition_cols if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Partition columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )
        else:
            partition_cols = None

        if order_by and order_by not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Order by column '{order_by}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.copy()

            # Sort if order_by specified
            if order_by:
                result_df = result_df.sort_values(by=order_by)

            if partition_cols:
                result_df[new_column] = result_df.groupby(partition_cols)[column].shift(offset)
            else:
                result_df[new_column] = result_df[column].shift(offset)

            # Apply default value
            if default is not None:
                result_df[new_column] = result_df[new_column].fillna(default)

            # Restore original order if sorted
            if order_by:
                result_df = result_df.sort_index()

            # Count nulls introduced
            null_count = int(result_df[new_column].isna().sum())

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "offset": offset,
                    "null_values": null_count,
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
# lead
# =============================================================================


@register_primitive
class Lead(Primitive):
    """Get value from next row (like SQL LEAD)."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="lead",
            category="calculate",
            description="Get value from N rows after current row (like SQL LEAD)",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to get lead values from",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for the lead value column (default: column_lead_N)",
                ),
                ParamDef(
                    name="offset",
                    type="int",
                    required=False,
                    default=1,
                    description="Number of rows to look ahead (default: 1)",
                ),
                ParamDef(
                    name="default",
                    type="Any",
                    required=False,
                    default=None,
                    description="Value to use when there's no next row",
                ),
                ParamDef(
                    name="partition_by",
                    type="str | list[str]",
                    required=False,
                    default=None,
                    description="Column(s) to partition by",
                ),
                ParamDef(
                    name="order_by",
                    type="str",
                    required=False,
                    default=None,
                    description="Column to order by before applying lead",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Get the next day's opening price",
                    expected_params={
                        "column": "open_price",
                        "new_column": "next_open",
                        "offset": 1,
                    },
                    description="Next value",
                ),
                TestPrompt(
                    prompt="Get next month's forecast for each region",
                    expected_params={
                        "column": "forecast",
                        "partition_by": "region",
                        "order_by": "month",
                    },
                    description="Partitioned lead",
                ),
                TestPrompt(
                    prompt="Get the value from 2 rows ahead in quantity column",
                    expected_params={
                        "column": "quantity",
                        "offset": 2,
                    },
                    description="Multi-row lead",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        offset = params.get("offset", 1)
        new_column = params.get("new_column") or f"{column}_lead_{offset}"
        default = params.get("default")
        partition_by = params.get("partition_by")
        order_by = params.get("order_by")

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Validate partition and order columns
        if partition_by:
            partition_cols = [partition_by] if isinstance(partition_by, str) else partition_by
            missing = [c for c in partition_cols if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Partition columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )
        else:
            partition_cols = None

        if order_by and order_by not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Order by column '{order_by}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.copy()

            # Sort if order_by specified
            if order_by:
                result_df = result_df.sort_values(by=order_by)

            # Negative shift to look ahead
            if partition_cols:
                result_df[new_column] = result_df.groupby(partition_cols)[column].shift(-offset)
            else:
                result_df[new_column] = result_df[column].shift(-offset)

            # Apply default value
            if default is not None:
                result_df[new_column] = result_df[new_column].fillna(default)

            # Restore original order if sorted
            if order_by:
                result_df = result_df.sort_index()

            # Count nulls introduced
            null_count = int(result_df[new_column].isna().sum())

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "offset": offset,
                    "null_values": null_count,
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
# moving_average
# =============================================================================


@register_primitive
class MovingAverage(Primitive):
    """Calculate a rolling/moving average."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="moving_average",
            category="calculate",
            description="Calculate a rolling/moving average over a window of rows",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to calculate moving average for",
                ),
                ParamDef(
                    name="window",
                    type="int",
                    required=True,
                    description="Number of rows in the rolling window",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for the moving average column (default: column_ma_N)",
                ),
                ParamDef(
                    name="min_periods",
                    type="int",
                    required=False,
                    default=1,
                    description="Minimum number of observations required to have a value",
                ),
                ParamDef(
                    name="center",
                    type="bool",
                    required=False,
                    default=False,
                    description="Center the window (True) or trail (False, default)",
                ),
                ParamDef(
                    name="partition_by",
                    type="str | list[str]",
                    required=False,
                    default=None,
                    description="Calculate moving average within groups",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Calculate a 7-day moving average of sales",
                    expected_params={
                        "column": "sales",
                        "window": 7,
                    },
                    description="Basic moving average",
                ),
                TestPrompt(
                    prompt="Add a 30-day rolling average for temperature",
                    expected_params={
                        "column": "temperature",
                        "window": 30,
                        "new_column": "temp_30d_avg",
                    },
                    description="Rolling average with custom name",
                ),
                TestPrompt(
                    prompt="Calculate 5-period moving average of price for each stock symbol",
                    expected_params={
                        "column": "price",
                        "window": 5,
                        "partition_by": "symbol",
                    },
                    description="Partitioned moving average",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        window = params["window"]
        new_column = params.get("new_column") or f"{column}_ma_{window}"
        min_periods = params.get("min_periods", 1)
        center = params.get("center", False)
        partition_by = params.get("partition_by")

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if window < 1:
            return PrimitiveResult(
                success=False,
                error="Window must be at least 1",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Validate partition columns
        if partition_by:
            partition_cols = [partition_by] if isinstance(partition_by, str) else partition_by
            missing = [c for c in partition_cols if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Partition columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )
        else:
            partition_cols = None

        try:
            result_df = df.copy()

            # Convert to numeric
            numeric_col = pd.to_numeric(result_df[column], errors="coerce")

            if partition_cols:
                result_df[new_column] = numeric_col.groupby(result_df[partition_cols].apply(
                    lambda x: tuple(x), axis=1
                )).rolling(window=window, min_periods=min_periods, center=center).mean().reset_index(
                    level=0, drop=True
                )
            else:
                result_df[new_column] = numeric_col.rolling(
                    window=window,
                    min_periods=min_periods,
                    center=center
                ).mean()

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "window": window,
                    "min_periods": min_periods,
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
# percent_rank
# =============================================================================


@register_primitive
class PercentRank(Primitive):
    """Calculate percentile rank (0-1 scale)."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="percent_rank",
            category="calculate",
            description="Calculate percentile rank (0 to 1) for each value like SQL PERCENT_RANK()",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to calculate percent rank for",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for the percent rank column (default: column_pct_rank)",
                ),
                ParamDef(
                    name="ascending",
                    type="bool",
                    required=False,
                    default=True,
                    description="Rank direction (True = lowest value gets lowest rank)",
                ),
                ParamDef(
                    name="partition_by",
                    type="str | list[str]",
                    required=False,
                    default=None,
                    description="Calculate percent rank within groups",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Calculate percentile rank for scores",
                    expected_params={
                        "column": "score",
                    },
                    description="Basic percent rank",
                ),
                TestPrompt(
                    prompt="Add a percentile column for salaries, highest gets 1.0",
                    expected_params={
                        "column": "salary",
                        "new_column": "salary_percentile",
                        "ascending": False,
                    },
                    description="Percent rank descending",
                ),
                TestPrompt(
                    prompt="Calculate percent rank of sales within each region",
                    expected_params={
                        "column": "sales",
                        "partition_by": "region",
                    },
                    description="Partitioned percent rank",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        new_column = params.get("new_column") or f"{column}_pct_rank"
        ascending = params.get("ascending", True)
        partition_by = params.get("partition_by")

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Validate partition columns
        if partition_by:
            partition_cols = [partition_by] if isinstance(partition_by, str) else partition_by
            missing = [c for c in partition_cols if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Partition columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )
        else:
            partition_cols = None

        try:
            result_df = df.copy()

            if partition_cols:
                result_df[new_column] = result_df.groupby(partition_cols)[column].rank(
                    method="min", ascending=ascending, pct=True
                )
            else:
                result_df[new_column] = result_df[column].rank(
                    method="min", ascending=ascending, pct=True
                )

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "min_percentile": float(result_df[new_column].min()) if len(result_df) > 0 else None,
                    "max_percentile": float(result_df[new_column].max()) if len(result_df) > 0 else None,
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
# first_value
# =============================================================================


@register_primitive
class FirstValue(Primitive):
    """Get the first value in a window or partition."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="first_value",
            category="calculate",
            description="Get the first value within a partition or ordered window (like SQL FIRST_VALUE)",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to get the first value from",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Name for the first value column (default: column_first)",
                ),
                ParamDef(
                    name="partition_by",
                    type="str | list[str]",
                    required=True,
                    description="Column(s) to partition by",
                ),
                ParamDef(
                    name="order_by",
                    type="str",
                    required=False,
                    default=None,
                    description="Column to order by before getting first value",
                ),
                ParamDef(
                    name="ascending",
                    type="bool",
                    required=False,
                    default=True,
                    description="Order direction when order_by is specified",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Get the first order date for each customer",
                    expected_params={
                        "column": "order_date",
                        "partition_by": "customer_id",
                        "order_by": "order_date",
                    },
                    description="First order date per customer",
                ),
                TestPrompt(
                    prompt="Get the first recorded price for each product",
                    expected_params={
                        "column": "price",
                        "partition_by": "product_id",
                        "order_by": "date",
                    },
                    description="First price per product",
                ),
                TestPrompt(
                    prompt="Add the first sale amount for each salesperson as baseline",
                    expected_params={
                        "column": "sale_amount",
                        "new_column": "baseline_sale",
                        "partition_by": "salesperson",
                        "order_by": "date",
                    },
                    description="Baseline from first value",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        new_column = params.get("new_column") or f"{column}_first"
        partition_by = params["partition_by"]
        order_by = params.get("order_by")
        ascending = params.get("ascending", True)

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Validate partition columns
        partition_cols = [partition_by] if isinstance(partition_by, str) else partition_by
        missing = [c for c in partition_cols if c not in df.columns]
        if missing:
            return PrimitiveResult(
                success=False,
                error=f"Partition columns not found: {missing}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if order_by and order_by not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Order by column '{order_by}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.copy()

            if order_by:
                # Sort first, then get first value per partition
                result_df = result_df.sort_values(by=order_by, ascending=ascending)
                result_df[new_column] = result_df.groupby(partition_cols)[column].transform("first")
                result_df = result_df.sort_index()
            else:
                # Just get first value per partition (based on original order)
                result_df[new_column] = result_df.groupby(partition_cols)[column].transform("first")

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "unique_first_values": int(result_df[new_column].nunique()),
                },
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )
