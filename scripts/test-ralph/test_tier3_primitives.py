#!/usr/bin/env python3
"""
Test Tier 3 primitives against sample data.

Tier 3 primitives:
1. row_number - sequential row numbers
2. explode_column - split delimited values into rows
3. string_contains - check if text contains substring
4. substring - extract portion of string
5. replace_null - replace null values
6. concat_columns - concatenate columns
7. case_when - multi-condition value assignment
8. is_duplicate - flag duplicate rows
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from transforms_v2 import Engine, list_primitives


def print_section(title: str):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def get_metadata(res):
    """Extract metadata from ExecutionResult."""
    if res.success and res.steps:
        return res.steps[-1].result.metadata
    return {}


def main():
    print_section("TIER 3 PRIMITIVES TEST")

    primitives = list_primitives()
    print(f"\nTotal primitives: {len(primitives)}")
    print("\nTier 3 primitives:")
    tier3 = ["row_number", "explode_column", "string_contains", "substring",
             "replace_null", "concat_columns", "case_when", "is_duplicate"]
    for p in tier3:
        status = "✓" if p in primitives else "✗"
        print(f"  {status} {p}")

    # Load test data
    df = pd.read_csv(project_root / "test_data" / "sample.csv")
    print(f"\nLoaded sample.csv: {len(df)} rows, {len(df.columns)} columns")

    engine = Engine()
    results = []

    # =========================================================================
    # Test 1: row_number
    # =========================================================================
    print_section("TEST 1: row_number")

    plan = [{"op": "row_number", "params": {"new_column": "row_num", "start": 1}}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df[["row_num", "id", "name"]].head(5).to_string())
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 2: explode_column
    # =========================================================================
    print_section("TEST 2: explode_column")

    # Create test data with delimited values
    df_explode = pd.DataFrame({
        "id": [1, 2, 3],
        "tags": ["red, green, blue", "yellow", "black, white"],
        "name": ["Item 1", "Item 2", "Item 3"]
    })
    print("Test data:")
    print(df_explode.to_string())

    plan = [{"op": "explode_column", "params": {"column": "tags"}}]
    result = engine.execute(df_explode, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(f"Rows: {len(df_explode)} -> {len(result.df)}")
        print(result.df.to_string())
        print(f"Metadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 3: string_contains
    # =========================================================================
    print_section("TEST 3: string_contains")

    plan = [{"op": "string_contains", "params": {
        "column": "email",
        "substring": "gmail",
        "new_column": "is_gmail"
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df[["name", "email", "is_gmail"]].to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 4: substring
    # =========================================================================
    print_section("TEST 4: substring")

    plan = [{"op": "substring", "params": {
        "column": "phone",
        "start": 0,
        "length": 5,
        "new_column": "phone_prefix"
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df[["name", "phone", "phone_prefix"]].head(6).to_string())
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 5: replace_null
    # =========================================================================
    print_section("TEST 5: replace_null")

    plan = [{"op": "replace_null", "params": {
        "columns": ["email", "notes"],
        "value": "N/A"
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        # Show rows that had nulls
        null_rows = df[df["email"].isna() | df["notes"].isna()].index.tolist()
        print(f"Rows with nulls: {null_rows}")
        print(result.df.loc[null_rows, ["name", "email", "notes"]].to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 6: concat_columns
    # =========================================================================
    print_section("TEST 6: concat_columns")

    # Create test data for concat
    df_concat = pd.DataFrame({
        "first": ["John", "Jane", "Bob", None],
        "last": ["Doe", "Smith", None, "Wilson"],
        "id": [1, 2, 3, 4]
    })
    print("Test data:")
    print(df_concat.to_string())

    plan = [{"op": "concat_columns", "params": {
        "columns": ["first", "last"],
        "new_column": "full_name",
        "separator": " "
    }}]
    result = engine.execute(df_concat, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(result.df[["first", "last", "full_name"]].to_string())
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 7: case_when
    # =========================================================================
    print_section("TEST 7: case_when")

    plan = [{"op": "case_when", "params": {
        "cases": [
            {"condition": {"column": "amount", "operator": "gt", "value": 2000}, "result": "High"},
            {"condition": {"column": "amount", "operator": "gt", "value": 1000}, "result": "Medium"},
        ],
        "new_column": "tier",
        "default": "Low"
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df[["name", "amount", "tier"]].to_string())
        print(f"\nTier counts: {result.df['tier'].value_counts().to_dict()}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 8: is_duplicate
    # =========================================================================
    print_section("TEST 8: is_duplicate")

    # Create data with duplicates
    df_dup = pd.DataFrame({
        "email": ["a@test.com", "b@test.com", "a@test.com", "c@test.com", "b@test.com"],
        "name": ["Alice", "Bob", "Alice2", "Carol", "Bob2"]
    })
    print("Test data:")
    print(df_dup.to_string())

    plan = [{"op": "is_duplicate", "params": {"subset": ["email"]}}]
    result = engine.execute(df_dup, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(result.df.to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Summary
    # =========================================================================
    print_section("SUMMARY")

    test_names = [
        "row_number",
        "explode_column",
        "string_contains",
        "substring",
        "replace_null",
        "concat_columns",
        "case_when",
        "is_duplicate",
    ]

    print(f"\n{'Test':<25} {'Result':<10}")
    print("─" * 35)
    for name, passed in zip(test_names, results):
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{name:<25} {status}")

    print("─" * 35)
    passed_count = sum(results)
    total_count = len(results)
    print(f"\nFINAL: {passed_count}/{total_count} ({100*passed_count/total_count:.0f}%)")

    return all(results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
