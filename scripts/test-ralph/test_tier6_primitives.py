#!/usr/bin/env python3
"""
Test Tier 6 primitives against sample data.

Tier 6 primitives (Schema Drift):
1. compare_schemas - compare expected vs observed schema
2. detect_renamed_columns - fuzzy match to suggest renames
3. detect_enum_drift - find new/missing categorical values
4. detect_format_drift - detect format changes
5. detect_distribution_drift - compare value distributions
6. normalize_boolean - convert yes/no, Y/N, 0/1 to boolean
7. normalize_enum_values - map variants to canonical values
8. generate_drift_report - summarize all detected drifts
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
    print_section("TIER 6 PRIMITIVES TEST (Schema Drift)")

    primitives = list_primitives()
    print(f"\nTotal primitives: {len(primitives)}")
    print("\nTier 6 primitives:")
    tier6 = ["compare_schemas", "detect_renamed_columns", "detect_enum_drift",
             "detect_format_drift", "detect_distribution_drift", "normalize_boolean",
             "normalize_enum_values", "generate_drift_report"]
    for p in tier6:
        status = "✓" if p in primitives else "✗"
        print(f"  {status} {p}")

    # Load test data
    df = pd.read_csv(project_root / "test_data" / "sample.csv")
    print(f"\nLoaded sample.csv: {len(df)} rows, {len(df.columns)} columns")

    engine = Engine()
    results = []

    # =========================================================================
    # Test 1: compare_schemas
    # =========================================================================
    print_section("TEST 1: compare_schemas")

    # Create test data with schema drift
    df_test = pd.DataFrame({
        "name": ["Alice", "Bob", "Carol"],
        "email": ["a@test.com", "b@test.com", "c@test.com"],
        "age": [25, 30, 35],
        "extra_col": [1, 2, 3]  # Extra column not expected
    })
    print("Test data columns:", list(df_test.columns))

    plan = [{"op": "compare_schemas", "params": {
        "expected_columns": ["name", "email", "age", "missing_col"],
        "expected_types": {"name": "string", "age": "integer"},
    }}]
    result = engine.execute(df_test, plan)
    print(f"Success: {result.success}")
    if result.success:
        meta = get_metadata(result)
        print(f"Has drift: {meta.get('has_drift')}")
        print(f"Missing columns: {meta.get('missing_columns')}")
        print(f"Extra columns: {meta.get('extra_columns')}")
        print(f"Type mismatches: {meta.get('type_mismatches')}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 2: detect_renamed_columns
    # =========================================================================
    print_section("TEST 2: detect_renamed_columns")

    # Create data with similar column names (simulating renames)
    df_renamed = pd.DataFrame({
        "customer_name": ["Alice", "Bob"],
        "email_address": ["a@test.com", "b@test.com"],
        "total_amount": [100.0, 200.0],
    })
    print("Actual columns:", list(df_renamed.columns))

    plan = [{"op": "detect_renamed_columns", "params": {
        "expected_columns": ["name", "email", "amount"],
        "similarity_threshold": 0.5
    }}]
    result = engine.execute(df_renamed, plan)
    print(f"Success: {result.success}")
    if result.success:
        meta = get_metadata(result)
        print(f"Rename suggestions: {meta.get('rename_suggestions')}")
        print(f"Unmatched expected: {meta.get('unmatched_expected')}")
        print(f"Unmatched actual: {meta.get('unmatched_actual')}")
        print(f"Has potential renames: {meta.get('has_potential_renames')}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 3: detect_enum_drift
    # =========================================================================
    print_section("TEST 3: detect_enum_drift")

    df_enum = pd.DataFrame({
        "status": ["active", "inactive", "pending", "new_status", "archived"]
    })
    print("Test data status values:", df_enum["status"].tolist())

    plan = [{"op": "detect_enum_drift", "params": {
        "column": "status",
        "expected_values": ["active", "inactive", "pending"],
        "add_drift_flag": True
    }}]
    result = engine.execute(df_enum, plan)
    print(f"Success: {result.success}")
    if result.success:
        meta = get_metadata(result)
        print(f"New values found: {meta.get('new_values')}")
        print(f"Missing values: {meta.get('missing_values')}")
        print(f"Value counts for new: {meta.get('new_value_counts')}")
        print(f"Data with drift flag:\n{result.df.to_string()}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 4: detect_format_drift
    # =========================================================================
    print_section("TEST 4: detect_format_drift")

    df_format = pd.DataFrame({
        "date_col": ["2024-01-15", "2024-02-20", "01/25/2024", "2024-03-10", "invalid"]
    })
    print("Test data dates:", df_format["date_col"].tolist())

    plan = [{"op": "detect_format_drift", "params": {
        "column": "date_col",
        "format_type": "date_iso"
    }}]
    result = engine.execute(df_format, plan)
    print(f"Success: {result.success}")
    if result.success:
        meta = get_metadata(result)
        match_rate = meta.get('match_rate', 0)
        print(f"Format match rate: {match_rate:.1%}")
        print(f"Non-matching count: {meta.get('non_match_count')}")
        print(f"Non-matching samples: {meta.get('non_matching_samples')}")
        print(f"Has drift: {meta.get('has_drift')}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 5: detect_distribution_drift
    # =========================================================================
    print_section("TEST 5: detect_distribution_drift")

    # Numeric distribution drift
    df_dist = pd.DataFrame({
        "sales": [100, 110, 95, 105, 98, 500, 102]  # 500 is an outlier
    })
    print("Test data sales:", df_dist["sales"].tolist())

    plan = [{"op": "detect_distribution_drift", "params": {
        "column": "sales",
        "baseline_stats": {"mean": 100, "std": 10},
        "drift_threshold": 0.5
    }}]
    result = engine.execute(df_dist, plan)
    print(f"Success: {result.success}")
    if result.success:
        meta = get_metadata(result)
        print(f"Distribution drift detected: {meta.get('has_drift')}")
        print(f"Drift score: {meta.get('drift_score'):.2f}")
        print(f"Drifted metrics: {meta.get('drifted_metrics')}")
        observed = meta.get('observed_stats', {})
        print(f"Observed stats: mean={observed.get('mean', 0):.1f}, std={observed.get('std', 0):.1f}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 6: normalize_boolean
    # =========================================================================
    print_section("TEST 6: normalize_boolean")

    df_bool = pd.DataFrame({
        "active": ["yes", "no", "Y", "N", "true", "false", "1", "0", "on", "off"],
        "name": [f"User{i}" for i in range(10)]
    })
    print("Test data active values:", df_bool["active"].tolist())

    plan = [{"op": "normalize_boolean", "params": {
        "column": "active",
        "output_format": "boolean"
    }}]
    result = engine.execute(df_bool, plan)
    print(f"Success: {result.success}")
    if result.success:
        meta = get_metadata(result)
        print(f"Converted count: {meta.get('converted_count')}")
        print(f"True count: {meta.get('true_count')}, False count: {meta.get('false_count')}")
        print(f"Result:\n{result.df.to_string()}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 7: normalize_enum_values
    # =========================================================================
    print_section("TEST 7: normalize_enum_values")

    df_enum_norm = pd.DataFrame({
        "country": ["USA", "U.S.A.", "United States", "US", "Canada", "CAN", "CA"]
    })
    print("Test data countries:", df_enum_norm["country"].tolist())

    plan = [{"op": "normalize_enum_values", "params": {
        "column": "country",
        "mapping": {
            "United States": ["USA", "U.S.A.", "US"],
            "Canada": ["CAN", "CA"]
        }
    }}]
    result = engine.execute(df_enum_norm, plan)
    print(f"Success: {result.success}")
    if result.success:
        meta = get_metadata(result)
        print(f"Normalized count: {meta.get('normalized_count')}")
        print(f"Unique before: {meta.get('unique_before')}, after: {meta.get('unique_after')}")
        print(f"Result:\n{result.df.to_string()}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 8: generate_drift_report
    # =========================================================================
    print_section("TEST 8: generate_drift_report")

    # Create data with multiple types of drift
    df_drift = pd.DataFrame({
        "name": ["Alice", "Bob", "Carol"],
        "email": ["a@test.com", "b@test.com", "c@test.com"],
        "status": ["active", "inactive", "new_status"],
        "amount": [100.0, 200.0, 150.0]
    })
    print("Test data:")
    print(df_drift.to_string())

    plan = [{"op": "generate_drift_report", "params": {
        "expected_columns": ["name", "email", "status", "missing_col"],
        "expected_types": {"amount": "integer"},  # Intentional mismatch - it's float
        "enum_columns": {"status": ["active", "inactive", "pending"]},
        "include_recommendations": True
    }}]
    result = engine.execute(df_drift, plan)
    print(f"Success: {result.success}")
    if result.success:
        meta = get_metadata(result)
        print(f"\nDrift Report Summary:")
        print(f"  Overall severity: {meta.get('summary', {}).get('overall_severity')}")
        print(f"  Total issues: {meta.get('summary', {}).get('total_issues')}")
        print(f"\nColumn drift: {meta.get('column_drift')}")
        print(f"Type drift: {meta.get('type_drift')}")
        print(f"Enum drift: {meta.get('enum_drift')}")
        if meta.get('recommendations'):
            print(f"\nRecommendations:")
            for rec in meta.get('recommendations', []):
                print(f"  - {rec}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Summary
    # =========================================================================
    print_section("SUMMARY")

    test_names = [
        "compare_schemas",
        "detect_renamed_columns",
        "detect_enum_drift",
        "detect_format_drift",
        "detect_distribution_drift",
        "normalize_boolean",
        "normalize_enum_values",
        "generate_drift_report",
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
