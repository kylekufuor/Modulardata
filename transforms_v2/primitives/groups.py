# =============================================================================
# transforms_v2/primitives/groups.py - Aggregation Operations
# =============================================================================
# Primitives for grouping and aggregation: aggregate, pivot, unpivot.
# =============================================================================

from __future__ import annotations

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
# aggregate
# =============================================================================


@register_primitive
class Aggregate(Primitive):
    """Group by columns and calculate aggregations."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="aggregate",
            category="groups",
            description="Group rows by columns and calculate aggregations (sum, avg, count, etc.)",
            params=[
                ParamDef(
                    name="group_by",
                    type="list[str]",
                    required=True,
                    description="Columns to group by",
                ),
                ParamDef(
                    name="aggregations",
                    type="dict[str, str | list[str]]",
                    required=True,
                    description="Aggregations to perform: {column: 'sum'|'mean'|'count'|'min'|'max'|'first'|'last'}",
                ),
                ParamDef(
                    name="reset_index",
                    type="bool",
                    required=False,
                    default=True,
                    description="Whether to reset index after grouping",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Calculate total sales by region",
                    expected_params={
                        "group_by": ["region"],
                        "aggregations": {"sales": "sum"},
                    },
                    description="Sum by group",
                ),
                TestPrompt(
                    prompt="Get average price and count of products by category",
                    expected_params={
                        "group_by": ["category"],
                        "aggregations": {"price": "mean", "product_id": "count"},
                    },
                    description="Multiple aggregations",
                ),
                TestPrompt(
                    prompt="Find min and max order amount by customer",
                    expected_params={
                        "group_by": ["customer_id"],
                        "aggregations": {"amount": ["min", "max"]},
                    },
                    description="Multiple aggs on same column",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        group_by = params["group_by"]
        aggregations = params["aggregations"]
        reset_index = params.get("reset_index", True)

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate group by columns
        missing = [c for c in group_by if c not in df.columns]
        if missing:
            return PrimitiveResult(
                success=False,
                error=f"Group by columns not found: {missing}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Validate aggregation columns
        for col in aggregations.keys():
            if col not in df.columns:
                return PrimitiveResult(
                    success=False,
                    error=f"Aggregation column '{col}' not found",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )

        try:
            result_df = df.groupby(group_by, as_index=not reset_index).agg(aggregations)

            # Flatten multi-level columns if needed
            if isinstance(result_df.columns, pd.MultiIndex):
                result_df.columns = ["_".join(col).strip("_") for col in result_df.columns]

            if reset_index and not isinstance(result_df.index, pd.RangeIndex):
                result_df = result_df.reset_index()

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
# pivot
# =============================================================================


@register_primitive
class Pivot(Primitive):
    """Create a pivot table."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="pivot",
            category="groups",
            description="Create a pivot table with rows, columns, and values",
            params=[
                ParamDef(
                    name="index",
                    type="str | list[str]",
                    required=True,
                    description="Column(s) to use as row labels",
                ),
                ParamDef(
                    name="columns",
                    type="str",
                    required=True,
                    description="Column to use for new column headers",
                ),
                ParamDef(
                    name="values",
                    type="str | list[str]",
                    required=True,
                    description="Column(s) to aggregate",
                ),
                ParamDef(
                    name="aggfunc",
                    type="str",
                    required=False,
                    default="sum",
                    description="Aggregation function: 'sum', 'mean', 'count', 'min', 'max'",
                    choices=["sum", "mean", "count", "min", "max"],
                ),
                ParamDef(
                    name="fill_value",
                    type="Any",
                    required=False,
                    default=0,
                    description="Value to use for missing combinations",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Pivot sales data with products as rows and months as columns",
                    expected_params={
                        "index": "product",
                        "columns": "month",
                        "values": "sales",
                        "aggfunc": "sum",
                    },
                    description="Basic pivot",
                ),
                TestPrompt(
                    prompt="Create a pivot showing average score by student and subject",
                    expected_params={
                        "index": "student",
                        "columns": "subject",
                        "values": "score",
                        "aggfunc": "mean",
                    },
                    description="Pivot with average",
                ),
                TestPrompt(
                    prompt="Pivot order count by region and status",
                    expected_params={
                        "index": "region",
                        "columns": "status",
                        "values": "order_id",
                        "aggfunc": "count",
                    },
                    description="Pivot with count",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        index = params["index"]
        columns = params["columns"]
        values = params["values"]
        aggfunc = params.get("aggfunc", "sum")
        fill_value = params.get("fill_value", 0)

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate columns
        index_cols = [index] if isinstance(index, str) else index
        values_cols = [values] if isinstance(values, str) else values

        all_cols = index_cols + [columns] + values_cols
        missing = [c for c in all_cols if c not in df.columns]
        if missing:
            return PrimitiveResult(
                success=False,
                error=f"Columns not found: {missing}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = pd.pivot_table(
                df,
                index=index,
                columns=columns,
                values=values,
                aggfunc=aggfunc,
                fill_value=fill_value,
            )

            # Flatten column names if multi-level
            if isinstance(result_df.columns, pd.MultiIndex):
                result_df.columns = [
                    "_".join(str(c) for c in col).strip("_")
                    for col in result_df.columns
                ]

            result_df = result_df.reset_index()

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
# unpivot
# =============================================================================


@register_primitive
class Unpivot(Primitive):
    """Unpivot (melt) wide format to long format."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="unpivot",
            category="groups",
            description="Transform wide format to long format (melt columns into rows)",
            params=[
                ParamDef(
                    name="id_columns",
                    type="list[str]",
                    required=True,
                    description="Columns to keep as identifiers (not unpivoted)",
                ),
                ParamDef(
                    name="value_columns",
                    type="list[str]",
                    required=False,
                    default=None,
                    description="Columns to unpivot. If None, all non-ID columns.",
                ),
                ParamDef(
                    name="var_name",
                    type="str",
                    required=False,
                    default="variable",
                    description="Name for the new variable column",
                ),
                ParamDef(
                    name="value_name",
                    type="str",
                    required=False,
                    default="value",
                    description="Name for the new value column",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Unpivot monthly columns (Jan, Feb, Mar) into rows with product_id as identifier",
                    expected_params={
                        "id_columns": ["product_id"],
                        "value_columns": ["Jan", "Feb", "Mar"],
                        "var_name": "month",
                        "value_name": "sales",
                    },
                    description="Unpivot months",
                ),
                TestPrompt(
                    prompt="Melt the score columns into rows, keeping student_id and name",
                    expected_params={
                        "id_columns": ["student_id", "name"],
                        "var_name": "subject",
                        "value_name": "score",
                    },
                    description="Melt scores",
                ),
                TestPrompt(
                    prompt="Transform wide year columns (2020, 2021, 2022) to long format",
                    expected_params={
                        "id_columns": ["country"],
                        "value_columns": ["2020", "2021", "2022"],
                        "var_name": "year",
                        "value_name": "value",
                    },
                    description="Unpivot years",
                ),
            ],
            may_change_row_count=True,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        id_columns = params["id_columns"]
        value_columns = params.get("value_columns")
        var_name = params.get("var_name", "variable")
        value_name = params.get("value_name", "value")

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate ID columns
        missing = [c for c in id_columns if c not in df.columns]
        if missing:
            return PrimitiveResult(
                success=False,
                error=f"ID columns not found: {missing}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Validate value columns if specified
        if value_columns:
            missing = [c for c in value_columns if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Value columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )

        try:
            result_df = pd.melt(
                df,
                id_vars=id_columns,
                value_vars=value_columns,
                var_name=var_name,
                value_name=value_name,
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
