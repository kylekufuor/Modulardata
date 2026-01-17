#!/usr/bin/env python3
"""
Test Tier 1 primitives against sample data.

Tier 1 primitives:
1. sample_rows - random sampling
2. offset_rows - skip first N rows
3. extract_date_part - year/month/day extraction
4. date_diff - calculate date differences
5. floor_ceil - floor/ceiling for numbers
6. bin_values - create buckets
7. detect_nulls - null analysis
8. profile_column - column statistics
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from transforms_v2 import Engine, list_primitives


def print_section(title: str):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def main():
    print_section("TIER 1 PRIMITIVES TEST")

    # Show all primitives
    primitives = list_primitives()
    print(f"\nTotal primitives: {len(primitives)}")
    print("\nNew Tier 1 primitives:")
    tier1 = ["sample_rows", "offset_rows", "extract_date_part", "date_diff",
             "floor_ceil", "bin_values", "detect_nulls", "profile_column"]
    for p in tier1:
        status = "✓" if p in primitives else "✗"
        print(f"  {status} {p}")

    # Load test data
    df = pd.read_csv(project_root / "test_data" / "sample.csv")
    print(f"\nLoaded sample.csv: {len(df)} rows, {len(df.columns)} columns")
    print(df.to_string())

    engine = Engine()
    results = []

    # Helper to get metadata from result
    def get_metadata(res):
        """Extract metadata from ExecutionResult (stored in step results)."""
        if res.success and res.steps:
            return res.steps[-1].result.metadata
        return {}

    # =========================================================================
    # Test 1: sample_rows
    # =========================================================================
    print_section("TEST 1: sample_rows")

    plan = [{"op": "sample_rows", "params": {"n": 5, "seed": 42}}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Sampled {len(result.df)} rows from {len(df)}")
        print(result.df[["id", "name"]].to_string())
        print(f"Metadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # Test fraction sampling
    plan = [{"op": "sample_rows", "params": {"fraction": 0.5, "seed": 42}}]
    result = engine.execute(df, plan)
    print(f"\n50% sample: {len(result.df)} rows")
    assert result.success

    # =========================================================================
    # Test 2: offset_rows
    # =========================================================================
    print_section("TEST 2: offset_rows")

    plan = [{"op": "offset_rows", "params": {"offset": 3}}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Skipped 3 rows: {len(df)} -> {len(result.df)}")
        print(f"First row after offset: {result.df.iloc[0]['name']}")
        print(f"Metadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 3: extract_date_part
    # =========================================================================
    print_section("TEST 3: extract_date_part")

    plan = [{"op": "extract_date_part", "params": {
        "column": "date",
        "part": "year",
        "new_column": "order_year"
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Extracted years: {result.df['order_year'].tolist()}")
        print(f"Metadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # Test month extraction
    plan = [{"op": "extract_date_part", "params": {
        "column": "date",
        "part": "month"
    }}]
    result = engine.execute(df, plan)
    print(f"\nMonths: {result.df['date_month'].tolist()}")

    # Test day of week
    plan = [{"op": "extract_date_part", "params": {
        "column": "date",
        "part": "weekday_name"
    }}]
    result = engine.execute(df, plan)
    print(f"Day names: {result.df['date_weekday_name'].tolist()}")

    # =========================================================================
    # Test 4: date_diff
    # =========================================================================
    print_section("TEST 4: date_diff")

    # Create a DataFrame with two date columns
    df_dates = pd.DataFrame({
        "start_date": ["2024-01-01", "2024-02-15", "2024-03-01"],
        "end_date": ["2024-01-15", "2024-03-15", "2024-06-01"],
    })
    print("Test data:")
    print(df_dates.to_string())

    plan = [{"op": "date_diff", "params": {
        "start_column": "start_date",
        "end_column": "end_date",
        "unit": "days"
    }}]
    result = engine.execute(df_dates, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(f"Days difference: {result.df['date_diff_days'].tolist()}")
        print(f"Metadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # Test months
    plan = [{"op": "date_diff", "params": {
        "start_column": "start_date",
        "end_column": "end_date",
        "unit": "months"
    }}]
    result = engine.execute(df_dates, plan)
    print(f"Months difference: {result.df['date_diff_months'].tolist()}")

    # =========================================================================
    # Test 5: floor_ceil
    # =========================================================================
    print_section("TEST 5: floor_ceil")

    plan = [{"op": "floor_ceil", "params": {
        "column": "amount",
        "method": "floor",
        "precision": 0
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Original amounts: {df['amount'].tolist()}")
        print(f"Floored amounts: {result.df['amount'].tolist()}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # Test ceiling
    plan = [{"op": "floor_ceil", "params": {
        "column": "amount",
        "method": "ceil",
        "precision": 0
    }}]
    result = engine.execute(df, plan)
    print(f"Ceiling amounts: {result.df['amount'].tolist()}")

    # =========================================================================
    # Test 6: bin_values
    # =========================================================================
    print_section("TEST 6: bin_values")

    plan = [{"op": "bin_values", "params": {
        "column": "amount",
        "bins": [0, 1000, 2000, 3000, 5000],
        "labels": ["Low", "Medium", "High", "Very High"],
        "new_column": "amount_tier"
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df[["name", "amount", "amount_tier"]].to_string())
        print(f"Metadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # Test equal-width bins
    plan = [{"op": "bin_values", "params": {
        "column": "amount",
        "bins": 3,
        "new_column": "amount_bin_auto"
    }}]
    result = engine.execute(df, plan)
    print(f"\nAuto bins: {result.df['amount_bin_auto'].value_counts().to_dict()}")

    # =========================================================================
    # Test 7: detect_nulls
    # =========================================================================
    print_section("TEST 7: detect_nulls")

    plan = [{"op": "detect_nulls", "params": {
        "add_null_flag": True,
        "add_null_count": True
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        meta = get_metadata(result)
        print(f"Null counts: {meta.get('null_counts')}")
        print(f"Null rates: {meta.get('null_rates')}")
        print(f"Rows with any null: {meta.get('rows_with_any_null')}")
        print(f"\nRows flagged:")
        print(result.df[["name", "_has_null", "_null_count"]].to_string())
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 8: profile_column
    # =========================================================================
    print_section("TEST 8: profile_column")

    # Profile numeric column
    plan = [{"op": "profile_column", "params": {"column": "amount"}}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        profile = get_metadata(result).get("profile", {})
        print(f"\nProfile for 'amount' column:")
        for k, v in profile.items():
            if k != "top_values":
                print(f"  {k}: {v}")
        print(f"  top_values: {profile.get('top_values', {})}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # Profile text column
    plan = [{"op": "profile_column", "params": {"column": "status"}}]
    result = engine.execute(df, plan)
    print(f"\nProfile for 'status' column:")
    profile = get_metadata(result).get("profile", {})
    for k, v in profile.items():
        if k != "top_values":
            print(f"  {k}: {v}")
    print(f"  top_values: {profile.get('top_values', {})}")

    # =========================================================================
    # Summary
    # =========================================================================
    print_section("SUMMARY")

    test_names = [
        "sample_rows",
        "offset_rows",
        "extract_date_part",
        "date_diff",
        "floor_ceil",
        "bin_values",
        "detect_nulls",
        "profile_column",
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
