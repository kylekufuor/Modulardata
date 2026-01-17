#!/usr/bin/env python3
"""
Test Tier 2 primitives against sample data.

Tier 2 primitives:
1. head_rows - first N rows
2. tail_rows - last N rows
3. shuffle_rows - randomize row order
4. absolute_value - absolute value of numbers
5. is_between - check if value in range
6. coalesce - first non-null from columns
7. validate_pattern - regex pattern validation
8. date_add - add time to dates
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
    print_section("TIER 2 PRIMITIVES TEST")

    # Show tier 2 primitives
    primitives = list_primitives()
    print(f"\nTotal primitives: {len(primitives)}")
    print("\nTier 2 primitives:")
    tier2 = ["head_rows", "tail_rows", "shuffle_rows", "absolute_value",
             "is_between", "coalesce", "validate_pattern", "date_add"]
    for p in tier2:
        status = "✓" if p in primitives else "✗"
        print(f"  {status} {p}")

    # Load test data
    df = pd.read_csv(project_root / "test_data" / "sample.csv")
    print(f"\nLoaded sample.csv: {len(df)} rows, {len(df.columns)} columns")

    engine = Engine()
    results = []

    # =========================================================================
    # Test 1: head_rows
    # =========================================================================
    print_section("TEST 1: head_rows")

    plan = [{"op": "head_rows", "params": {"n": 5}}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Got first {len(result.df)} rows from {len(df)}")
        print(result.df[["id", "name"]].to_string())
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 2: tail_rows
    # =========================================================================
    print_section("TEST 2: tail_rows")

    plan = [{"op": "tail_rows", "params": {"n": 3}}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Got last {len(result.df)} rows from {len(df)}")
        print(result.df[["id", "name"]].to_string())
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 3: shuffle_rows
    # =========================================================================
    print_section("TEST 3: shuffle_rows")

    plan = [{"op": "shuffle_rows", "params": {"seed": 42}}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Shuffled {len(result.df)} rows")
        print(f"Original first 3 IDs: {df['id'].head(3).tolist()}")
        print(f"Shuffled first 3 IDs: {result.df['id'].head(3).tolist()}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 4: absolute_value
    # =========================================================================
    print_section("TEST 4: absolute_value")

    # Create test data with negatives
    df_abs = pd.DataFrame({
        "value": [10, -20, 30, -40, 50],
        "name": ["a", "b", "c", "d", "e"]
    })
    print("Test data:")
    print(df_abs.to_string())

    plan = [{"op": "absolute_value", "params": {"column": "value", "new_column": "abs_value"}}]
    result = engine.execute(df_abs, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(f"Original: {df_abs['value'].tolist()}")
        print(f"Absolute: {result.df['abs_value'].tolist()}")
        print(f"Metadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 5: is_between
    # =========================================================================
    print_section("TEST 5: is_between")

    plan = [{"op": "is_between", "params": {
        "column": "amount",
        "min_value": 500,
        "max_value": 2000,
        "new_column": "mid_range"
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df[["name", "amount", "mid_range"]].to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 6: coalesce
    # =========================================================================
    print_section("TEST 6: coalesce")

    # Create test data with nulls
    df_coalesce = pd.DataFrame({
        "primary_phone": [None, "555-1234", None, "555-5678", None],
        "mobile": ["555-0001", None, "555-0003", None, None],
        "work_phone": ["555-9999", "555-8888", "555-7777", "555-6666", None],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve"]
    })
    print("Test data:")
    print(df_coalesce.to_string())

    plan = [{"op": "coalesce", "params": {
        "columns": ["primary_phone", "mobile", "work_phone"],
        "new_column": "contact_number",
        "default": "NO PHONE"
    }}]
    result = engine.execute(df_coalesce, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(result.df[["name", "contact_number"]].to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 7: validate_pattern
    # =========================================================================
    print_section("TEST 7: validate_pattern")

    plan = [{"op": "validate_pattern", "params": {
        "column": "email",
        "pattern_type": "email"
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df[["name", "email", "email_valid"]].to_string())
        meta = get_metadata(result)
        print(f"\nValid: {meta.get('valid_count')}, Invalid: {meta.get('invalid_count')}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 8: date_add
    # =========================================================================
    print_section("TEST 8: date_add")

    plan = [{"op": "date_add", "params": {
        "column": "date",
        "amount": 30,
        "unit": "days",
        "new_column": "due_date"
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df[["name", "date", "due_date"]].head(5).to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # Test subtracting months
    plan = [{"op": "date_add", "params": {
        "column": "date",
        "amount": -1,
        "unit": "months",
        "new_column": "prev_month"
    }}]
    result = engine.execute(df, plan)
    print(f"\nSubtract 1 month:")
    print(result.df[["date", "prev_month"]].head(3).to_string())

    # =========================================================================
    # Summary
    # =========================================================================
    print_section("SUMMARY")

    test_names = [
        "head_rows",
        "tail_rows",
        "shuffle_rows",
        "absolute_value",
        "is_between",
        "coalesce",
        "validate_pattern",
        "date_add",
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
