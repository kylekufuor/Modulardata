#!/usr/bin/env python3
"""
Test Tier 5 primitives against sample data.

Tier 5 primitives:
1. regex_replace - transform text using regex with capture groups
2. regex_extract - extract text matching a pattern
3. fill_forward - fill nulls with previous value
4. fill_backward - fill nulls with next value
5. moving_average - rolling/moving average
6. percent_rank - percentile rank (0-1)
7. first_value - get first value in partition
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
    print_section("TIER 5 PRIMITIVES TEST")

    primitives = list_primitives()
    print(f"\nTotal primitives: {len(primitives)}")
    print("\nTier 5 primitives:")
    tier5 = ["regex_replace", "regex_extract", "fill_forward", "fill_backward",
             "moving_average", "percent_rank", "first_value"]
    for p in tier5:
        status = "✓" if p in primitives else "✗"
        print(f"  {status} {p}")

    # Load test data
    df = pd.read_csv(project_root / "test_data" / "sample.csv")
    print(f"\nLoaded sample.csv: {len(df)} rows, {len(df.columns)} columns")

    engine = Engine()
    results = []

    # =========================================================================
    # Test 1: regex_replace - Transform ABCDHED to ABC-DHED-XX format
    # =========================================================================
    print_section("TEST 1: regex_replace")

    # Create test data for the user's example
    df_codes = pd.DataFrame({
        "code": ["ABCDHED", "XYZQRST", "1234567", "AAABBBB"],
        "name": ["Item 1", "Item 2", "Item 3", "Item 4"]
    })
    print("Test data:")
    print(df_codes.to_string())

    plan = [{"op": "regex_replace", "params": {
        "column": "code",
        "pattern": "^(.{3})(.{4})(.*)$",
        "replacement": "\\1-\\2-XX",
        "new_column": "formatted_code"
    }}]
    result = engine.execute(df_codes, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print("Transformed:")
        print(result.df[["code", "formatted_code"]].to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 2: regex_extract - Extract numbers from text
    # =========================================================================
    print_section("TEST 2: regex_extract")

    df_extract = pd.DataFrame({
        "text": ["Order #12345 placed", "Invoice 67890", "No number here", "Reference: ABC-999"],
    })
    print("Test data:")
    print(df_extract.to_string())

    plan = [{"op": "regex_extract", "params": {
        "column": "text",
        "pattern": "\\d+",
        "new_column": "extracted_number"
    }}]
    result = engine.execute(df_extract, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(result.df.to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 3: fill_forward
    # =========================================================================
    print_section("TEST 3: fill_forward")

    df_fill = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=6),
        "price": [100.0, None, None, 110.0, None, 120.0]
    })
    print("Test data:")
    print(df_fill.to_string())

    plan = [{"op": "fill_forward", "params": {"columns": ["price"]}}]
    result = engine.execute(df_fill, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(result.df.to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 4: fill_backward
    # =========================================================================
    print_section("TEST 4: fill_backward")

    df_fill_back = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=6),
        "target": [None, None, 50.0, None, 60.0, 70.0]
    })
    print("Test data:")
    print(df_fill_back.to_string())

    plan = [{"op": "fill_backward", "params": {"columns": ["target"]}}]
    result = engine.execute(df_fill_back, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(result.df.to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 5: moving_average
    # =========================================================================
    print_section("TEST 5: moving_average")

    df_ma = pd.DataFrame({
        "day": range(1, 8),
        "sales": [100, 120, 90, 110, 130, 95, 105]
    })
    print("Test data:")
    print(df_ma.to_string())

    plan = [{"op": "moving_average", "params": {
        "column": "sales",
        "window": 3,
        "new_column": "sales_ma3"
    }}]
    result = engine.execute(df_ma, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(result.df.to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 6: percent_rank
    # =========================================================================
    print_section("TEST 6: percent_rank")

    plan = [{"op": "percent_rank", "params": {
        "column": "amount",
        "new_column": "amount_percentile"
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df[["name", "amount", "amount_percentile"]].to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 7: first_value
    # =========================================================================
    print_section("TEST 7: first_value")

    df_first = pd.DataFrame({
        "customer": ["A", "A", "A", "B", "B", "B"],
        "date": ["2024-01-01", "2024-01-15", "2024-02-01", "2024-01-05", "2024-01-10", "2024-02-05"],
        "amount": [100, 150, 200, 50, 75, 125]
    })
    print("Test data:")
    print(df_first.to_string())

    plan = [{"op": "first_value", "params": {
        "column": "amount",
        "partition_by": "customer",
        "order_by": "date",
        "new_column": "first_purchase"
    }}]
    result = engine.execute(df_first, plan)
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
        "regex_replace",
        "regex_extract",
        "fill_forward",
        "fill_backward",
        "moving_average",
        "percent_rank",
        "first_value",
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
