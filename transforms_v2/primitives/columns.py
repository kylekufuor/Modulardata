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
