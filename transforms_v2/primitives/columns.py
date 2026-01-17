# =============================================================================
# transforms_v2/primitives/columns.py - Column Operations
# =============================================================================
# Primitives that operate on columns: select, remove, rename, split, merge, etc.
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
# select_columns
# =============================================================================


@register_primitive
class SelectColumns(Primitive):
    """Keep only specified columns."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="select_columns",
            category="columns",
            description="Keep only the specified columns, removing all others",
            params=[
                ParamDef(
                    name="columns",
                    type="list[str]",
                    required=True,
                    description="List of column names to keep",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Keep only the name, email, and phone columns",
                    expected_params={"columns": ["name", "email", "phone"]},
                    description="Select multiple columns",
                ),
                TestPrompt(
                    prompt="I only need the customer_id and order_total fields",
                    expected_params={"columns": ["customer_id", "order_total"]},
                    description="Select specific fields",
                ),
                TestPrompt(
                    prompt="Show me just the first_name and last_name columns",
                    expected_params={"columns": ["first_name", "last_name"]},
                    description="Select name columns",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        columns = params["columns"]

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

        result_df = df[columns].copy()

        return PrimitiveResult(
            success=True,
            df=result_df,
            rows_before=rows_before,
            rows_after=len(result_df),
            cols_before=cols_before,
            cols_after=len(result_df.columns),
            metadata={"columns_removed": cols_before - len(result_df.columns)},
        )


# =============================================================================
# remove_columns
# =============================================================================


@register_primitive
class RemoveColumns(Primitive):
    """Remove specified columns."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="remove_columns",
            category="columns",
            description="Remove the specified columns from the data",
            params=[
                ParamDef(
                    name="columns",
                    type="list[str]",
                    required=True,
                    description="List of column names to remove",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Delete the internal_id and created_by columns",
                    expected_params={"columns": ["internal_id", "created_by"]},
                    description="Remove multiple columns",
                ),
                TestPrompt(
                    prompt="Remove the password field from the data",
                    expected_params={"columns": ["password"]},
                    description="Remove single column",
                ),
                TestPrompt(
                    prompt="Drop the temporary columns temp1 and temp2",
                    expected_params={"columns": ["temp1", "temp2"]},
                    description="Drop temp columns",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        columns = params["columns"]

        rows_before = len(df)
        cols_before = len(df.columns)

        # Check which columns exist
        existing = [c for c in columns if c in df.columns]
        missing = [c for c in columns if c not in df.columns]

        if not existing:
            return PrimitiveResult(
                success=False,
                error=f"None of the columns to remove were found: {columns}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.drop(columns=existing)

        warnings = []
        if missing:
            warnings.append(f"Columns not found (skipped): {missing}")

        return PrimitiveResult(
            success=True,
            df=result_df,
            rows_before=rows_before,
            rows_after=len(result_df),
            cols_before=cols_before,
            cols_after=len(result_df.columns),
            warnings=warnings,
            metadata={"columns_removed": len(existing)},
        )


# =============================================================================
# rename_columns
# =============================================================================


@register_primitive
class RenameColumns(Primitive):
    """Rename columns."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="rename_columns",
            category="columns",
            description="Rename one or more columns",
            params=[
                ParamDef(
                    name="mapping",
                    type="dict[str, str]",
                    required=True,
                    description="Mapping of old names to new names: {old_name: new_name}",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Rename 'cust_id' to 'customer_id'",
                    expected_params={"mapping": {"cust_id": "customer_id"}},
                    description="Rename single column",
                ),
                TestPrompt(
                    prompt="Change column names: fname to first_name and lname to last_name",
                    expected_params={"mapping": {"fname": "first_name", "lname": "last_name"}},
                    description="Rename multiple columns",
                ),
                TestPrompt(
                    prompt="Rename the 'amt' column to 'amount'",
                    expected_params={"mapping": {"amt": "amount"}},
                    description="Rename abbreviation to full name",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        mapping = params["mapping"]

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate old columns exist
        missing = [old for old in mapping.keys() if old not in df.columns]
        if missing:
            return PrimitiveResult(
                success=False,
                error=f"Columns to rename not found: {missing}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Check for conflicts with existing columns
        conflicts = [new for new in mapping.values() if new in df.columns and new not in mapping.keys()]
        if conflicts:
            return PrimitiveResult(
                success=False,
                error=f"New column names already exist: {conflicts}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.rename(columns=mapping)

        return PrimitiveResult(
            success=True,
            df=result_df,
            rows_before=rows_before,
            rows_after=len(result_df),
            cols_before=cols_before,
            cols_after=len(result_df.columns),
            metadata={"columns_renamed": len(mapping)},
        )


# =============================================================================
# reorder_columns
# =============================================================================


@register_primitive
class ReorderColumns(Primitive):
    """Reorder columns."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="reorder_columns",
            category="columns",
            description="Change the order of columns",
            params=[
                ParamDef(
                    name="order",
                    type="list[str]",
                    required=True,
                    description="New column order. Columns not listed will be appended at the end.",
                ),
                ParamDef(
                    name="strict",
                    type="bool",
                    required=False,
                    default=False,
                    description="If True, only include columns in the order list",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Move email to the first column",
                    expected_params={"order": ["email"]},
                    description="Move column to front",
                ),
                TestPrompt(
                    prompt="Arrange columns as: id, name, email, phone, status",
                    expected_params={"order": ["id", "name", "email", "phone", "status"]},
                    description="Full reorder",
                ),
                TestPrompt(
                    prompt="Put the date column at the beginning",
                    expected_params={"order": ["date"]},
                    description="Move date first",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        order = params["order"]
        strict = params.get("strict", False)

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate specified columns exist
        missing = [c for c in order if c not in df.columns]
        if missing:
            return PrimitiveResult(
                success=False,
                error=f"Columns not found: {missing}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if strict:
            # Only include specified columns
            new_order = order
        else:
            # Append remaining columns
            remaining = [c for c in df.columns if c not in order]
            new_order = order + remaining

        result_df = df[new_order]

        return PrimitiveResult(
            success=True,
            df=result_df,
            rows_before=rows_before,
            rows_after=len(result_df),
            cols_before=cols_before,
            cols_after=len(result_df.columns),
        )


# =============================================================================
# add_column
# =============================================================================


@register_primitive
class AddColumn(Primitive):
    """Add a new column with a constant value or based on other columns."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="add_column",
            category="columns",
            description="Add a new column with a constant value or derived from existing columns",
            params=[
                ParamDef(
                    name="name",
                    type="str",
                    required=True,
                    description="Name of the new column",
                ),
                ParamDef(
                    name="value",
                    type="Any",
                    required=False,
                    default=None,
                    description="Constant value for all rows",
                ),
                ParamDef(
                    name="from_column",
                    type="str",
                    required=False,
                    default=None,
                    description="Copy values from this column",
                ),
                ParamDef(
                    name="position",
                    type="int | str",
                    required=False,
                    default="end",
                    description="Position for new column: index number or 'start'/'end'",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Add a new column called 'source' with value 'import'",
                    expected_params={"name": "source", "value": "import"},
                    description="Add constant column",
                ),
                TestPrompt(
                    prompt="Create a 'processed' column set to False",
                    expected_params={"name": "processed", "value": False},
                    description="Add boolean column",
                ),
                TestPrompt(
                    prompt="Add an empty 'notes' column",
                    expected_params={"name": "notes", "value": ""},
                    description="Add empty column",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        name = params["name"]
        value = params.get("value")
        from_column = params.get("from_column")
        position = params.get("position", "end")

        rows_before = len(df)
        cols_before = len(df.columns)

        if name in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{name}' already exists",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()

        # Determine the value
        if from_column:
            if from_column not in df.columns:
                return PrimitiveResult(
                    success=False,
                    error=f"Source column '{from_column}' not found",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )
            result_df[name] = result_df[from_column]
        else:
            result_df[name] = value

        # Handle position
        if position == "start":
            cols = [name] + [c for c in result_df.columns if c != name]
            result_df = result_df[cols]
        elif isinstance(position, int):
            cols = list(result_df.columns)
            cols.remove(name)
            cols.insert(position, name)
            result_df = result_df[cols]
        # 'end' is default, column is already at end

        return PrimitiveResult(
            success=True,
            df=result_df,
            rows_before=rows_before,
            rows_after=len(result_df),
            cols_before=cols_before,
            cols_after=len(result_df.columns),
        )


# =============================================================================
# split_column
# =============================================================================


@register_primitive
class SplitColumn(Primitive):
    """Split a column into multiple columns."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="split_column",
            category="columns",
            description="Split a column into multiple columns based on a delimiter",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to split",
                ),
                ParamDef(
                    name="delimiter",
                    type="str",
                    required=True,
                    description="Character or string to split on",
                ),
                ParamDef(
                    name="new_columns",
                    type="list[str]",
                    required=True,
                    description="Names for the new columns",
                ),
                ParamDef(
                    name="keep_original",
                    type="bool",
                    required=False,
                    default=False,
                    description="Whether to keep the original column",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Split the 'full_name' column into 'first_name' and 'last_name' by space",
                    expected_params={
                        "column": "full_name",
                        "delimiter": " ",
                        "new_columns": ["first_name", "last_name"],
                    },
                    description="Split name by space",
                ),
                TestPrompt(
                    prompt="Separate the address column by comma into street, city, state",
                    expected_params={
                        "column": "address",
                        "delimiter": ",",
                        "new_columns": ["street", "city", "state"],
                    },
                    description="Split address by comma",
                ),
                TestPrompt(
                    prompt="Break apart the date column using '/' into month, day, year",
                    expected_params={
                        "column": "date",
                        "delimiter": "/",
                        "new_columns": ["month", "day", "year"],
                    },
                    description="Split date by slash",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        delimiter = params["delimiter"]
        new_columns = params["new_columns"]
        keep_original = params.get("keep_original", False)

        rows_before = len(df)
        cols_before = len(df.columns)

        if column not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{column}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        # Check for conflicts
        conflicts = [c for c in new_columns if c in df.columns and c != column]
        if conflicts:
            return PrimitiveResult(
                success=False,
                error=f"New column names already exist: {conflicts}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.copy()

            # Split the column
            split_data = result_df[column].astype(str).str.split(delimiter, expand=True)

            # Handle case where we have fewer splits than expected columns
            n_cols = min(len(new_columns), split_data.shape[1] if len(split_data.shape) > 1 else 1)

            for i in range(n_cols):
                result_df[new_columns[i]] = split_data[i] if split_data.shape[1] > i else None

            # Fill remaining columns with None if split produced fewer parts
            for i in range(n_cols, len(new_columns)):
                result_df[new_columns[i]] = None

            if not keep_original:
                result_df = result_df.drop(columns=[column])

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={"columns_added": len(new_columns)},
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# merge_columns
# =============================================================================


@register_primitive
class MergeColumns(Primitive):
    """Merge multiple columns into one."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="merge_columns",
            category="columns",
            description="Combine multiple columns into a single column",
            params=[
                ParamDef(
                    name="columns",
                    type="list[str]",
                    required=True,
                    description="Columns to merge",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=True,
                    description="Name of the merged column",
                ),
                ParamDef(
                    name="separator",
                    type="str",
                    required=False,
                    default=" ",
                    description="String to put between merged values",
                ),
                ParamDef(
                    name="keep_original",
                    type="bool",
                    required=False,
                    default=False,
                    description="Whether to keep the original columns",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Combine first_name and last_name into full_name with a space",
                    expected_params={
                        "columns": ["first_name", "last_name"],
                        "new_column": "full_name",
                        "separator": " ",
                    },
                    description="Merge names with space",
                ),
                TestPrompt(
                    prompt="Join city, state, and zip into 'location' separated by ', '",
                    expected_params={
                        "columns": ["city", "state", "zip"],
                        "new_column": "location",
                        "separator": ", ",
                    },
                    description="Merge address parts",
                ),
                TestPrompt(
                    prompt="Concatenate area_code and phone_number into phone with a dash",
                    expected_params={
                        "columns": ["area_code", "phone_number"],
                        "new_column": "phone",
                        "separator": "-",
                    },
                    description="Merge phone parts",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        columns = params["columns"]
        new_column = params["new_column"]
        separator = params.get("separator", " ")
        keep_original = params.get("keep_original", False)

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

        if new_column in df.columns and new_column not in columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{new_column}' already exists",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()

        # Merge columns
        result_df[new_column] = result_df[columns].astype(str).agg(separator.join, axis=1)

        if not keep_original:
            # Remove original columns (except if one of them is the new column name)
            cols_to_remove = [c for c in columns if c != new_column]
            result_df = result_df.drop(columns=cols_to_remove)

        return PrimitiveResult(
            success=True,
            df=result_df,
            rows_before=rows_before,
            rows_after=len(result_df),
            cols_before=cols_before,
            cols_after=len(result_df.columns),
        )


# =============================================================================
# copy_column
# =============================================================================


@register_primitive
class CopyColumn(Primitive):
    """Copy a column to a new column."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="copy_column",
            category="columns",
            description="Create a copy of an existing column with a new name",
            params=[
                ParamDef(
                    name="source",
                    type="str",
                    required=True,
                    description="Column to copy from",
                ),
                ParamDef(
                    name="destination",
                    type="str",
                    required=True,
                    description="Name for the new column",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Make a copy of the email column called email_backup",
                    expected_params={"source": "email", "destination": "email_backup"},
                    description="Copy for backup",
                ),
                TestPrompt(
                    prompt="Duplicate the price column as original_price",
                    expected_params={"source": "price", "destination": "original_price"},
                    description="Duplicate before modification",
                ),
                TestPrompt(
                    prompt="Create a copy of status called status_original",
                    expected_params={"source": "status", "destination": "status_original"},
                    description="Copy status column",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        source = params["source"]
        destination = params["destination"]

        rows_before = len(df)
        cols_before = len(df.columns)

        if source not in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Source column '{source}' not found",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        if destination in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Destination column '{destination}' already exists",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        result_df = df.copy()
        result_df[destination] = result_df[source]

        return PrimitiveResult(
            success=True,
            df=result_df,
            rows_before=rows_before,
            rows_after=len(result_df),
            cols_before=cols_before,
            cols_after=len(result_df.columns),
        )


# =============================================================================
# change_column_type
# =============================================================================


@register_primitive
class ChangeColumnType(Primitive):
    """Change a column's data type."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="change_column_type",
            category="columns",
            description="Convert a column to a different data type",
            params=[
                ParamDef(
                    name="column",
                    type="str",
                    required=True,
                    description="Column to convert",
                ),
                ParamDef(
                    name="to_type",
                    type="str",
                    required=True,
                    description="Target type: 'string', 'integer', 'float', 'boolean', 'datetime'",
                    choices=["string", "integer", "float", "boolean", "datetime"],
                ),
                ParamDef(
                    name="date_format",
                    type="str",
                    required=False,
                    default=None,
                    description="Date format for datetime conversion (e.g., '%Y-%m-%d')",
                ),
                ParamDef(
                    name="errors",
                    type="str",
                    required=False,
                    default="coerce",
                    description="How to handle errors: 'raise', 'coerce' (set to NaN), 'ignore'",
                    choices=["raise", "coerce", "ignore"],
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Convert the lead_score column to integers",
                    expected_params={"column": "lead_score", "to_type": "integer"},
                    description="Convert to int",
                ),
                TestPrompt(
                    prompt="Change the created_date column to datetime format",
                    expected_params={"column": "created_date", "to_type": "datetime"},
                    description="Convert to datetime",
                ),
                TestPrompt(
                    prompt="Make the price column a float",
                    expected_params={"column": "price", "to_type": "float"},
                    description="Convert to float",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        column = params["column"]
        to_type = params["to_type"]
        date_format = params.get("date_format")
        errors = params.get("errors", "coerce")

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
        warnings = []

        try:
            if to_type == "string":
                result_df[column] = result_df[column].astype(str)
            elif to_type == "integer":
                # First convert to numeric, then to int
                result_df[column] = pd.to_numeric(result_df[column], errors=errors)
                # Only convert to int if no NaN values (or coerce to nullable int)
                if result_df[column].isna().any():
                    result_df[column] = result_df[column].astype("Int64")  # nullable int
                else:
                    result_df[column] = result_df[column].astype(int)
            elif to_type == "float":
                result_df[column] = pd.to_numeric(result_df[column], errors=errors)
            elif to_type == "boolean":
                # Handle common boolean representations
                col = result_df[column]
                if col.dtype == object:
                    true_vals = {"true", "yes", "1", "t", "y"}
                    false_vals = {"false", "no", "0", "f", "n"}
                    result_df[column] = col.str.lower().map(
                        lambda x: True if x in true_vals else (False if x in false_vals else None)
                    )
                else:
                    result_df[column] = result_df[column].astype(bool)
            elif to_type == "datetime":
                result_df[column] = pd.to_datetime(
                    result_df[column],
                    format=date_format,
                    errors=errors,
                )

            # Count conversion failures
            if errors == "coerce":
                new_nulls = result_df[column].isna().sum() - df[column].isna().sum()
                if new_nulls > 0:
                    warnings.append(f"{new_nulls} values could not be converted and were set to null")

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                warnings=warnings,
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )


# =============================================================================
# coalesce
# =============================================================================


@register_primitive
class Coalesce(Primitive):
    """Return the first non-null value from multiple columns."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="coalesce",
            category="columns",
            description="Get the first non-null value from a list of columns for each row",
            params=[
                ParamDef(
                    name="columns",
                    type="list[str]",
                    required=True,
                    description="Columns to check in order (first non-null wins)",
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
                    description="Default value if all columns are null",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Get the first non-empty value from phone, mobile, or work_phone as contact_number",
                    expected_params={
                        "columns": ["phone", "mobile", "work_phone"],
                        "new_column": "contact_number",
                    },
                    description="Coalesce phone numbers",
                ),
                TestPrompt(
                    prompt="Combine email and backup_email into primary_email, using 'none@example.com' as default",
                    expected_params={
                        "columns": ["email", "backup_email"],
                        "new_column": "primary_email",
                        "default": "none@example.com",
                    },
                    description="Coalesce with default",
                ),
                TestPrompt(
                    prompt="Get the first available address from shipping_address, billing_address, home_address",
                    expected_params={
                        "columns": ["shipping_address", "billing_address", "home_address"],
                        "new_column": "address",
                    },
                    description="Coalesce addresses",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        columns = params["columns"]
        new_column = params["new_column"]
        default = params.get("default")

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
            result_df = df.copy()

            # Use pandas bfill on the columns to get the first non-null
            # This works by stacking columns and taking first non-null per row
            result_df[new_column] = result_df[columns].bfill(axis=1).iloc[:, 0]

            # Apply default if specified
            if default is not None:
                result_df[new_column] = result_df[new_column].fillna(default)

            # Count how many rows had all nulls
            all_null_count = int(df[columns].isna().all(axis=1).sum())

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "all_null_rows": all_null_count,
                    "columns_checked": len(columns),
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
# replace_null
# =============================================================================


@register_primitive
class ReplaceNull(Primitive):
    """Replace null values with a specified value."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="replace_null",
            category="columns",
            description="Replace null/missing values in one or more columns with a specified value",
            params=[
                ParamDef(
                    name="columns",
                    type="list[str]",
                    required=False,
                    default=None,
                    description="Columns to replace nulls in (default: all columns)",
                ),
                ParamDef(
                    name="value",
                    type="Any",
                    required=True,
                    description="Value to replace nulls with",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Replace null values in the status column with 'Unknown'",
                    expected_params={
                        "columns": ["status"],
                        "value": "Unknown",
                    },
                    description="Replace nulls in one column",
                ),
                TestPrompt(
                    prompt="Replace all null values with 0",
                    expected_params={
                        "value": 0,
                    },
                    description="Replace all nulls",
                ),
                TestPrompt(
                    prompt="Fill missing email and phone with 'N/A'",
                    expected_params={
                        "columns": ["email", "phone"],
                        "value": "N/A",
                    },
                    description="Replace nulls in multiple columns",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        columns = params.get("columns")
        value = params["value"]

        rows_before = len(df)
        cols_before = len(df.columns)

        try:
            result_df = df.copy()

            if columns:
                # Validate columns
                missing = [c for c in columns if c not in df.columns]
                if missing:
                    return PrimitiveResult(
                        success=False,
                        error=f"Columns not found: {missing}",
                        rows_before=rows_before,
                        cols_before=cols_before,
                    )
                # Count nulls before
                nulls_before = result_df[columns].isna().sum().sum()
                # Replace in specific columns
                result_df[columns] = result_df[columns].fillna(value)
                nulls_after = result_df[columns].isna().sum().sum()
            else:
                # Count nulls before
                nulls_before = result_df.isna().sum().sum()
                # Replace in all columns
                result_df = result_df.fillna(value)
                nulls_after = result_df.isna().sum().sum()

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "nulls_replaced": int(nulls_before - nulls_after),
                    "replacement_value": str(value),
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
# concat_columns
# =============================================================================


@register_primitive
class ConcatColumns(Primitive):
    """Concatenate multiple columns into one."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="concat_columns",
            category="columns",
            description="Combine multiple columns into a single column with optional separator",
            params=[
                ParamDef(
                    name="columns",
                    type="list[str]",
                    required=True,
                    description="Columns to concatenate",
                ),
                ParamDef(
                    name="new_column",
                    type="str",
                    required=True,
                    description="Name for the result column",
                ),
                ParamDef(
                    name="separator",
                    type="str",
                    required=False,
                    default="",
                    description="Separator between values (default: no separator)",
                ),
                ParamDef(
                    name="skip_null",
                    type="bool",
                    required=False,
                    default=True,
                    description="Skip null values when concatenating (default: True)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Combine first_name and last_name into full_name with a space",
                    expected_params={
                        "columns": ["first_name", "last_name"],
                        "new_column": "full_name",
                        "separator": " ",
                    },
                    description="Concat with space separator",
                ),
                TestPrompt(
                    prompt="Create an address column from street, city, state, zip separated by commas",
                    expected_params={
                        "columns": ["street", "city", "state", "zip"],
                        "new_column": "address",
                        "separator": ", ",
                    },
                    description="Multi-column concat",
                ),
                TestPrompt(
                    prompt="Combine code1 and code2 into combined_code with hyphen",
                    expected_params={
                        "columns": ["code1", "code2"],
                        "new_column": "combined_code",
                        "separator": "-",
                    },
                    description="Concat with hyphen",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        columns = params["columns"]
        new_column = params["new_column"]
        separator = params.get("separator", "")
        skip_null = params.get("skip_null", True)

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate columns
        missing = [c for c in columns if c not in df.columns]
        if missing:
            return PrimitiveResult(
                success=False,
                error=f"Columns not found: {missing}",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.copy()

            if skip_null:
                # Concatenate non-null values only
                def concat_row(row):
                    values = [str(v) for v in row if pd.notna(v)]
                    return separator.join(values) if values else None

                result_df[new_column] = result_df[columns].apply(concat_row, axis=1)
            else:
                # Convert all to string (including nulls as 'nan') and concat
                result_df[new_column] = result_df[columns].astype(str).agg(separator.join, axis=1)

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "columns_concatenated": len(columns),
                    "separator": separator,
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
# generate_uuid
# =============================================================================


@register_primitive
class GenerateUUID(Primitive):
    """Generate unique identifiers for each row."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="generate_uuid",
            category="columns",
            description="Add a column with unique identifiers (UUID) for each row",
            params=[
                ParamDef(
                    name="new_column",
                    type="str",
                    required=False,
                    default="uuid",
                    description="Name for the UUID column",
                ),
                ParamDef(
                    name="format",
                    type="str",
                    required=False,
                    default="uuid4",
                    description="UUID format: 'uuid4' (random), 'uuid1' (time-based), 'short' (8-char hex)",
                    choices=["uuid4", "uuid1", "short"],
                ),
                ParamDef(
                    name="prefix",
                    type="str",
                    required=False,
                    default="",
                    description="Optional prefix to add before each UUID",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Add a unique ID column to the data",
                    expected_params={"new_column": "unique_id"},
                    description="Basic UUID generation",
                ),
                TestPrompt(
                    prompt="Generate a short 8-character ID for each row in column 'row_id'",
                    expected_params={
                        "new_column": "row_id",
                        "format": "short",
                    },
                    description="Short ID generation",
                ),
                TestPrompt(
                    prompt="Create UUIDs with prefix 'TXN-' for transaction_id column",
                    expected_params={
                        "new_column": "transaction_id",
                        "prefix": "TXN-",
                    },
                    description="UUID with prefix",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        import uuid

        new_column = params.get("new_column", "uuid")
        uuid_format = params.get("format", "uuid4")
        prefix = params.get("prefix", "")

        rows_before = len(df)
        cols_before = len(df.columns)

        if new_column in df.columns:
            return PrimitiveResult(
                success=False,
                error=f"Column '{new_column}' already exists",
                rows_before=rows_before,
                cols_before=cols_before,
            )

        try:
            result_df = df.copy()

            # Generate UUIDs based on format
            if uuid_format == "uuid4":
                uuids = [str(uuid.uuid4()) for _ in range(len(df))]
            elif uuid_format == "uuid1":
                uuids = [str(uuid.uuid1()) for _ in range(len(df))]
            elif uuid_format == "short":
                # 8-character hex strings
                uuids = [uuid.uuid4().hex[:8] for _ in range(len(df))]
            else:
                uuids = [str(uuid.uuid4()) for _ in range(len(df))]

            # Add prefix if specified
            if prefix:
                uuids = [f"{prefix}{u}" for u in uuids]

            result_df[new_column] = uuids

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "format": uuid_format,
                    "uuids_generated": len(uuids),
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
# fill_forward
# =============================================================================


@register_primitive
class FillForward(Primitive):
    """Fill null values with the previous non-null value."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="fill_forward",
            category="columns",
            description="Replace null values with the most recent non-null value (forward fill)",
            params=[
                ParamDef(
                    name="columns",
                    type="list[str]",
                    required=False,
                    default=None,
                    description="Columns to fill (default: all columns)",
                ),
                ParamDef(
                    name="limit",
                    type="int",
                    required=False,
                    default=None,
                    description="Maximum number of consecutive nulls to fill",
                ),
                ParamDef(
                    name="group_by",
                    type="str | list[str]",
                    required=False,
                    default=None,
                    description="Fill within groups (doesn't carry over between groups)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Fill missing values in the price column with the previous price",
                    expected_params={
                        "columns": ["price"],
                    },
                    description="Fill single column forward",
                ),
                TestPrompt(
                    prompt="Forward fill all null values in the dataset",
                    expected_params={},
                    description="Fill all columns",
                ),
                TestPrompt(
                    prompt="Fill missing stock prices forward but within each ticker symbol",
                    expected_params={
                        "columns": ["price"],
                        "group_by": "ticker",
                    },
                    description="Grouped forward fill",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        columns = params.get("columns")
        limit = params.get("limit")
        group_by = params.get("group_by")

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate columns
        if columns:
            missing = [c for c in columns if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )

        # Validate group_by
        if group_by:
            group_cols = [group_by] if isinstance(group_by, str) else group_by
            missing = [c for c in group_cols if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Group by columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )
        else:
            group_cols = None

        try:
            result_df = df.copy()

            # Count nulls before
            if columns:
                nulls_before = result_df[columns].isna().sum().sum()
            else:
                nulls_before = result_df.isna().sum().sum()

            # Apply forward fill
            if group_cols:
                if columns:
                    result_df[columns] = result_df.groupby(group_cols)[columns].ffill(limit=limit)
                else:
                    # Fill all columns within groups
                    result_df = result_df.groupby(group_cols, group_keys=False).apply(
                        lambda g: g.ffill(limit=limit)
                    )
            else:
                if columns:
                    result_df[columns] = result_df[columns].ffill(limit=limit)
                else:
                    result_df = result_df.ffill(limit=limit)

            # Count nulls after
            if columns:
                nulls_after = result_df[columns].isna().sum().sum()
            else:
                nulls_after = result_df.isna().sum().sum()

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "nulls_filled": int(nulls_before - nulls_after),
                    "nulls_remaining": int(nulls_after),
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
# fill_backward
# =============================================================================


@register_primitive
class FillBackward(Primitive):
    """Fill null values with the next non-null value."""

    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="fill_backward",
            category="columns",
            description="Replace null values with the next non-null value (backward fill)",
            params=[
                ParamDef(
                    name="columns",
                    type="list[str]",
                    required=False,
                    default=None,
                    description="Columns to fill (default: all columns)",
                ),
                ParamDef(
                    name="limit",
                    type="int",
                    required=False,
                    default=None,
                    description="Maximum number of consecutive nulls to fill",
                ),
                ParamDef(
                    name="group_by",
                    type="str | list[str]",
                    required=False,
                    default=None,
                    description="Fill within groups (doesn't carry over between groups)",
                ),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Fill missing values in the target column with the next available value",
                    expected_params={
                        "columns": ["target"],
                    },
                    description="Fill single column backward",
                ),
                TestPrompt(
                    prompt="Backward fill all null values",
                    expected_params={},
                    description="Fill all columns backward",
                ),
                TestPrompt(
                    prompt="Fill missing budget values backward within each department",
                    expected_params={
                        "columns": ["budget"],
                        "group_by": "department",
                    },
                    description="Grouped backward fill",
                ),
            ],
            may_change_row_count=False,
            may_change_col_count=False,
        )

    def execute(self, df: pd.DataFrame, params: dict[str, Any]) -> PrimitiveResult:
        columns = params.get("columns")
        limit = params.get("limit")
        group_by = params.get("group_by")

        rows_before = len(df)
        cols_before = len(df.columns)

        # Validate columns
        if columns:
            missing = [c for c in columns if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )

        # Validate group_by
        if group_by:
            group_cols = [group_by] if isinstance(group_by, str) else group_by
            missing = [c for c in group_cols if c not in df.columns]
            if missing:
                return PrimitiveResult(
                    success=False,
                    error=f"Group by columns not found: {missing}",
                    rows_before=rows_before,
                    cols_before=cols_before,
                )
        else:
            group_cols = None

        try:
            result_df = df.copy()

            # Count nulls before
            if columns:
                nulls_before = result_df[columns].isna().sum().sum()
            else:
                nulls_before = result_df.isna().sum().sum()

            # Apply backward fill
            if group_cols:
                if columns:
                    result_df[columns] = result_df.groupby(group_cols)[columns].bfill(limit=limit)
                else:
                    # Fill all columns within groups
                    result_df = result_df.groupby(group_cols, group_keys=False).apply(
                        lambda g: g.bfill(limit=limit)
                    )
            else:
                if columns:
                    result_df[columns] = result_df[columns].bfill(limit=limit)
                else:
                    result_df = result_df.bfill(limit=limit)

            # Count nulls after
            if columns:
                nulls_after = result_df[columns].isna().sum().sum()
            else:
                nulls_after = result_df.isna().sum().sum()

            return PrimitiveResult(
                success=True,
                df=result_df,
                rows_before=rows_before,
                rows_after=len(result_df),
                cols_before=cols_before,
                cols_after=len(result_df.columns),
                metadata={
                    "nulls_filled": int(nulls_before - nulls_after),
                    "nulls_remaining": int(nulls_after),
                },
            )
        except Exception as e:
            return PrimitiveResult(
                success=False,
                error=str(e),
                rows_before=rows_before,
                cols_before=cols_before,
            )
