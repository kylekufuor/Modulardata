# =============================================================================
# agents/plan_translator.py - TechnicalPlan to transforms_v2 Translator
# =============================================================================
# Translates TechnicalPlan objects from the Strategist into transforms_v2
# primitive operations that the Engine can execute.
#
# This is the bridge between the old agent architecture and the new
# deterministic primitives library.
#
# Usage:
#   from agents.plan_translator import PlanTranslator
#
#   translator = PlanTranslator()
#   v2_plan = translator.translate(technical_plan)
#   # v2_plan = [{"op": "filter", "params": {...}}]
# =============================================================================

from __future__ import annotations

import logging
from typing import Any

from agents.models.technical_plan import (
    TechnicalPlan,
    TransformationType,
    FilterOperator,
)

logger = logging.getLogger(__name__)


class TranslationError(Exception):
    """Error during plan translation."""

    def __init__(self, message: str, transformation_type: str | None = None):
        super().__init__(message)
        self.transformation_type = transformation_type


class PlanTranslator:
    """
    Translates TechnicalPlan objects to transforms_v2 primitive format.

    The translator maps:
    - TransformationType enum -> primitive name
    - TechnicalPlan fields -> primitive params

    Some transformations map 1:1, others require multiple primitives,
    and some are not yet supported in transforms_v2.
    """

    # -------------------------------------------------------------------------
    # Mapping: TransformationType -> transforms_v2 primitive
    # -------------------------------------------------------------------------

    # Direct mappings (TransformationType -> transforms_v2 primitive name)
    TYPE_TO_PRIMITIVE: dict[str, str] = {
        # Row operations
        "drop_rows": "filter_rows",  # Inverted filter
        "filter_rows": "filter_rows",
        "deduplicate": "remove_duplicates",
        "sort_rows": "sort_rows",
        "slice_rows": "limit_rows",
        "sample_rows": "limit_rows",  # Use limit_rows with random

        # Column operations
        "drop_columns": "remove_columns",
        "select_columns": "select_columns",
        "rename_column": "rename_columns",
        "reorder_columns": "reorder_columns",
        "split_column": "split_column",
        "merge_columns": "merge_columns",
        "add_column": "add_column",

        # Value transformations
        "fill_nulls": "fill_blanks",
        "replace_values": "find_replace",
        "trim_whitespace": "trim_whitespace",
        "change_case": "change_text_casing",
        "standardize": "standardize_values",

        # Type operations
        "convert_type": "change_column_type",
        "parse_date": "format_date",
        "format_date": "format_date",

        # Numeric operations
        "round_numbers": "round_numbers",
        "percent_of_total": "percentage",
        "cumulative": "running_total",
        "rank": "rank",

        # Text operations
        "extract_pattern": "extract_text",
        "pad_string": "pad_text",
        "substring": "extract_text",

        # Restructuring
        "pivot": "pivot",
        "melt": "unpivot",

        # Aggregation
        "group_by": "aggregate",
        "join": "join_tables",

        # Validation
        "format_phone": "format_phone",

        # Advanced
        "conditional_replace": "conditional_value",
    }

    # Operations that need special handling
    COMPLEX_MAPPINGS = {
        "drop_rows",  # Needs condition inversion
        "drop_columns",  # Needs column list inversion
    }

    # Operations not yet in transforms_v2 (will fall back to old registry)
    UNSUPPORTED = {
        "sanitize_headers",
        "date_diff",
        "date_add",
        "extract_date_part",
        "date_to_epoch",
        "handle_outliers",
        "normalize",
        "abs_value",
        "bin_numeric",
        "floor_ceiling",
        "clean_text",
        "remove_html",
        "transpose",
        "validate_format",
        "mask_data",
        "flag_duplicates",
        "coalesce",
        "explode",
        "lag_lead",
        "undo",
        "custom",
    }

    def __init__(self):
        """Initialize the translator."""
        pass

    def can_translate(self, plan: TechnicalPlan) -> bool:
        """
        Check if a TechnicalPlan can be translated to transforms_v2.

        Returns:
            True if the plan can be translated, False if it should
            fall back to the old registry.
        """
        trans_type = self._get_type_value(plan.transformation_type)
        return trans_type not in self.UNSUPPORTED

    def translate(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """
        Translate a TechnicalPlan to transforms_v2 format.

        Args:
            plan: TechnicalPlan from the Strategist

        Returns:
            List of transforms_v2 operations:
            [{"op": "primitive_name", "params": {...}}, ...]

        Raises:
            TranslationError: If translation fails
        """
        trans_type = self._get_type_value(plan.transformation_type)

        if trans_type in self.UNSUPPORTED:
            raise TranslationError(
                f"Transformation '{trans_type}' not supported in transforms_v2",
                transformation_type=trans_type,
            )

        # Get the translation method
        method_name = f"_translate_{trans_type}"
        if hasattr(self, method_name):
            return getattr(self, method_name)(plan)

        # Try generic translation
        return self._translate_generic(plan)

    def _get_type_value(self, trans_type: TransformationType | str) -> str:
        """Get the string value of a TransformationType."""
        if hasattr(trans_type, "value"):
            return trans_type.value
        return str(trans_type)

    # -------------------------------------------------------------------------
    # Generic Translation
    # -------------------------------------------------------------------------

    def _translate_generic(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Generic translation using the TYPE_TO_PRIMITIVE mapping."""
        trans_type = self._get_type_value(plan.transformation_type)

        if trans_type not in self.TYPE_TO_PRIMITIVE:
            raise TranslationError(
                f"No mapping for transformation '{trans_type}'",
                transformation_type=trans_type,
            )

        primitive = self.TYPE_TO_PRIMITIVE[trans_type]
        params = self._extract_params(plan, primitive)

        return [{"op": primitive, "params": params}]

    # -------------------------------------------------------------------------
    # Specific Translators
    # -------------------------------------------------------------------------

    def _translate_filter_rows(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate filter_rows to filter_rows primitive."""
        if not plan.conditions:
            raise TranslationError("filter_rows requires conditions")

        # Build filter conditions list
        conditions = []
        for cond in plan.conditions:
            column = cond.column
            operator = self._get_type_value(cond.operator)
            value = cond.value

            condition = {"column": column, "operator": operator}
            if operator not in ("isnull", "notnull"):
                condition["value"] = value

            conditions.append(condition)

        params = {
            "conditions": conditions,
            "keep": True,  # filter_rows keeps matching rows
        }

        return [{"op": "filter_rows", "params": params}]

    def _translate_drop_rows(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate drop_rows to filter_rows with keep=False."""
        # drop_rows removes rows that match the condition
        # Use filter_rows with keep=False

        if not plan.conditions:
            raise TranslationError("drop_rows requires conditions")

        # Build filter conditions list
        conditions = []
        for cond in plan.conditions:
            column = cond.column
            operator = self._get_type_value(cond.operator)
            value = cond.value

            condition = {"column": column, "operator": operator}
            if operator not in ("isnull", "notnull"):
                condition["value"] = value

            conditions.append(condition)

        params = {
            "conditions": conditions,
            "keep": False,  # drop_rows removes matching rows
        }

        return [{"op": "filter_rows", "params": params}]

    def _translate_deduplicate(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate deduplicate to remove_duplicates primitive."""
        columns = plan.get_target_column_names() or None
        keep = plan.parameters.get("keep", "first")

        params = {"keep": keep}
        if columns:
            params["subset"] = columns

        return [{"op": "remove_duplicates", "params": params}]

    def _translate_sort_rows(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate sort_rows to sort_rows primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("sort_rows requires target columns")

        ascending = plan.parameters.get("ascending", True)

        params = {
            "columns": columns,
            "ascending": ascending,
        }

        return [{"op": "sort_rows", "params": params}]

    def _translate_select_columns(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate select_columns to select_columns primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("select_columns requires target columns")

        return [{"op": "select_columns", "params": {"columns": columns}}]

    def _translate_drop_columns(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate drop_columns to remove_columns primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("drop_columns requires target columns")

        return [{"op": "remove_columns", "params": {"columns": columns}}]

    def _translate_rename_column(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate rename_column to rename_columns primitive."""
        if not plan.target_columns:
            raise TranslationError("rename_column requires target columns")

        old_name = plan.target_columns[0].column_name
        new_name = plan.parameters.get("new_name")

        if not new_name:
            raise TranslationError("rename_column requires 'new_name' parameter")

        return [{"op": "rename_columns", "params": {"mapping": {old_name: new_name}}}]

    def _translate_fill_nulls(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate fill_nulls to fill_blanks primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("fill_nulls requires target columns")

        method = plan.parameters.get("method", "value")
        value = plan.parameters.get("fill_value")

        # Map fill methods
        method_map = {
            "value": "value",
            "mean": "mean",
            "median": "median",
            "mode": "mode",
            "forward": "forward_fill",
            "backward": "backward_fill",
        }

        params = {
            "column": columns[0],
            "method": method_map.get(method, method),
        }

        if method == "value" and value is not None:
            params["value"] = value

        return [{"op": "fill_blanks", "params": params}]

    def _translate_replace_values(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate replace_values to find_replace primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("replace_values requires target columns")

        find = plan.parameters.get("find")
        replace = plan.parameters.get("replace", "")
        use_regex = plan.parameters.get("use_regex", False)

        if find is None:
            raise TranslationError("replace_values requires 'find' parameter")

        return [{"op": "find_replace", "params": {
            "column": columns[0],
            "find": find,
            "replace": replace,
            "use_regex": use_regex,
        }}]

    def _translate_trim_whitespace(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate trim_whitespace to trim_whitespace primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("trim_whitespace requires target columns")

        trim_type = plan.parameters.get("trim_type", "both")

        return [{"op": "trim_whitespace", "params": {
            "columns": columns,
            "trim_type": trim_type,
        }}]

    def _translate_change_case(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate change_case to change_text_casing primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("change_case requires target columns")

        case_type = plan.parameters.get("case", "lower")

        return [{"op": "change_text_casing", "params": {
            "column": columns[0],
            "case": case_type,
        }}]

    def _translate_convert_type(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate convert_type to change_column_type primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("convert_type requires target columns")

        target_type = plan.parameters.get("target_type", "string")

        return [{"op": "change_column_type", "params": {
            "column": columns[0],
            "to_type": target_type,
        }}]

    def _translate_format_date(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate format_date to format_date primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("format_date requires target columns")

        output_format = plan.parameters.get("output_format", "%Y-%m-%d")

        return [{"op": "format_date", "params": {
            "column": columns[0],
            "output_format": output_format,
        }}]

    def _translate_parse_date(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate parse_date to format_date primitive."""
        # parse_date is essentially the same as format_date
        return self._translate_format_date(plan)

    def _translate_round_numbers(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate round_numbers to round_numbers primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("round_numbers requires target columns")

        decimals = plan.parameters.get("decimals", 2)

        return [{"op": "round_numbers", "params": {
            "column": columns[0],
            "decimals": decimals,
        }}]

    def _translate_group_by(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate group_by to aggregate primitive."""
        group_cols = plan.get_target_column_names()
        if not group_cols:
            raise TranslationError("group_by requires group columns")

        agg_column = plan.parameters.get("agg_column")
        agg_func = plan.parameters.get("agg_func", "sum")

        if not agg_column:
            raise TranslationError("group_by requires 'agg_column' parameter")

        return [{"op": "aggregate", "params": {
            "group_by": group_cols,
            "aggregations": {agg_column: agg_func},
        }}]

    def _translate_pivot(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate pivot to pivot primitive."""
        index = plan.parameters.get("index")
        columns = plan.parameters.get("columns")
        values = plan.parameters.get("values")
        aggfunc = plan.parameters.get("aggfunc", "sum")

        if not all([index, columns, values]):
            raise TranslationError("pivot requires index, columns, and values")

        return [{"op": "pivot", "params": {
            "index": index,
            "columns": columns,
            "values": values,
            "aggfunc": aggfunc,
        }}]

    def _translate_melt(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate melt to unpivot primitive."""
        id_columns = plan.parameters.get("id_columns", [])
        value_columns = plan.parameters.get("value_columns")
        var_name = plan.parameters.get("var_name", "variable")
        value_name = plan.parameters.get("value_name", "value")

        return [{"op": "unpivot", "params": {
            "id_columns": id_columns,
            "value_columns": value_columns,
            "var_name": var_name,
            "value_name": value_name,
        }}]

    def _translate_join(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate join to join_tables primitive."""
        left_on = plan.parameters.get("left_on")
        right_on = plan.parameters.get("right_on")
        how = plan.parameters.get("how", "left")
        right_table = plan.parameters.get("right_table")

        if not all([left_on, right_on]):
            raise TranslationError("join requires left_on and right_on")

        params = {
            "left_on": left_on,
            "right_on": right_on,
            "how": how,
        }

        if right_table is not None:
            params["right_table"] = right_table

        return [{"op": "join_tables", "params": params}]

    def _translate_rank(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate rank to rank primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("rank requires target columns")

        ascending = plan.parameters.get("ascending", True)
        method = plan.parameters.get("method", "average")
        new_column = plan.parameters.get("new_column", f"{columns[0]}_rank")

        return [{"op": "rank", "params": {
            "column": columns[0],
            "new_column": new_column,
            "ascending": ascending,
            "method": method,
        }}]

    def _translate_cumulative(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate cumulative to running_total primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("cumulative requires target columns")

        new_column = plan.parameters.get("new_column", f"{columns[0]}_cumulative")

        return [{"op": "running_total", "params": {
            "column": columns[0],
            "new_column": new_column,
        }}]

    def _translate_percent_of_total(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate percent_of_total to percentage primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("percent_of_total requires target columns")

        new_column = plan.parameters.get("new_column", f"{columns[0]}_pct")

        return [{"op": "percentage", "params": {
            "column": columns[0],
            "new_column": new_column,
            "mode": "of_total",
            "multiply_by_100": True,
        }}]

    def _translate_split_column(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate split_column to split primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("split_column requires target columns")

        delimiter = plan.parameters.get("delimiter", ",")
        new_columns = plan.parameters.get("new_columns")

        params = {
            "column": columns[0],
            "delimiter": delimiter,
        }

        if new_columns:
            params["new_columns"] = new_columns

        return [{"op": "split", "params": params}]

    def _translate_merge_columns(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate merge_columns to merge primitive."""
        columns = plan.get_target_column_names()
        if len(columns) < 2:
            raise TranslationError("merge_columns requires at least 2 columns")

        separator = plan.parameters.get("separator", " ")
        new_column = plan.parameters.get("new_column", "merged")

        return [{"op": "merge", "params": {
            "columns": columns,
            "separator": separator,
            "new_column": new_column,
        }}]

    def _translate_standardize(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate standardize to standardize_values primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("standardize requires target columns")

        mapping = plan.parameters.get("mapping", {})

        return [{"op": "standardize_values", "params": {
            "column": columns[0],
            "mapping": mapping,
        }}]

    def _translate_conditional_replace(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate conditional_replace to conditional_value primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("conditional_replace requires target columns")

        conditions_list = plan.parameters.get("conditions", [])
        default = plan.parameters.get("default")
        new_column = plan.parameters.get("new_column", columns[0])

        return [{"op": "conditional_value", "params": {
            "new_column": new_column,
            "conditions": conditions_list,
            "default": default,
        }}]

    def _translate_format_phone(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate format_phone to format_phone primitive."""
        columns = plan.get_target_column_names()
        if not columns:
            raise TranslationError("format_phone requires target columns")

        format_type = plan.parameters.get("format", "us_standard")

        return [{"op": "format_phone", "params": {
            "column": columns[0],
            "format": format_type,
        }}]

    def _translate_slice_rows(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate slice_rows to limit primitive."""
        n = plan.parameters.get("n", 10)
        position = plan.parameters.get("position", "first")

        return [{"op": "limit", "params": {
            "n": n,
            "position": position,
        }}]

    def _translate_sample_rows(self, plan: TechnicalPlan) -> list[dict[str, Any]]:
        """Translate sample_rows to sample primitive."""
        n = plan.parameters.get("n")
        fraction = plan.parameters.get("fraction")

        params = {}
        if n:
            params["n"] = n
        elif fraction:
            params["fraction"] = fraction
        else:
            params["n"] = 10

        return [{"op": "sample", "params": params}]

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def _extract_params(self, plan: TechnicalPlan, primitive: str) -> dict[str, Any]:
        """
        Extract parameters from TechnicalPlan for a given primitive.

        This is a fallback for operations not explicitly handled.
        """
        params = {}

        # Add target columns if available
        columns = plan.get_target_column_names()
        if columns:
            if len(columns) == 1:
                params["column"] = columns[0]
            else:
                params["columns"] = columns

        # Copy over parameters from the plan
        params.update(plan.parameters)

        return params
