#!/usr/bin/env python3
"""
Test Tier 4 primitives against sample data.

Tier 4 primitives:
1. detect_header - find header row in messy data
2. validate_schema - validate data matches expected schema
3. infer_types - auto-detect and convert column types
4. generate_uuid - add unique identifiers
5. dense_rank - dense ranking (no gaps)
6. ntile - divide into N buckets
7. lag - get previous row value
8. lead - get next row value
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
    print_section("TIER 4 PRIMITIVES TEST")

    primitives = list_primitives()
    print(f"\nTotal primitives: {len(primitives)}")
    print("\nTier 4 primitives:")
    tier4 = ["detect_header", "validate_schema", "infer_types", "generate_uuid",
             "dense_rank", "ntile", "lag", "lead"]
    for p in tier4:
        status = "✓" if p in primitives else "✗"
        print(f"  {status} {p}")

    # Load test data
    df = pd.read_csv(project_root / "test_data" / "sample.csv")
    print(f"\nLoaded sample.csv: {len(df)} rows, {len(df.columns)} columns")

    engine = Engine()
    results = []

    # =========================================================================
    # Test 1: detect_header
    # =========================================================================
    print_section("TEST 1: detect_header")

    # Create messy data with junk rows before header
    messy_data = pd.DataFrame({
        0: ["Report Generated: 2024-01-15", "", "name", "Alice", "Bob", "Carol"],
        1: ["System: Production", "", "email", "alice@test.com", "bob@test.com", "carol@test.com"],
        2: ["", "", "amount", "100", "200", "300"],
    })
    print("Messy test data (header at row 2):")
    print(messy_data.to_string())

    plan = [{"op": "detect_header", "params": {
        "expected_columns": ["name", "email", "amount"],
        "auto_apply": True
    }}]
    result = engine.execute(messy_data, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(f"Detected header row: {get_metadata(result).get('detected_row')}")
        print(f"Cleaned data:\n{result.df.to_string()}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 2: validate_schema
    # =========================================================================
    print_section("TEST 2: validate_schema")

    plan = [{"op": "validate_schema", "params": {
        "expected_columns": ["name", "email", "amount", "status"],
        "expected_types": {"amount": "numeric", "email": "string"},
        "allow_extra_columns": True
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        meta = get_metadata(result)
        print(f"Schema valid: {meta.get('is_valid')}")
        print(f"Validation: {meta.get('validation_results', {})}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 3: infer_types
    # =========================================================================
    print_section("TEST 3: infer_types")

    # Create data with mixed types stored as strings
    df_types = pd.DataFrame({
        "id": ["1", "2", "3", "4"],
        "price": ["10.5", "20.0", "15.75", "8.25"],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
        "active": ["true", "false", "true", "false"],
        "name": ["Alice", "Bob", "Carol", "Dave"]
    })
    print("Test data (all strings):")
    print(df_types.dtypes)

    plan = [{"op": "infer_types", "params": {"apply_conversion": True}}]
    result = engine.execute(df_types, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(f"Inferred types: {get_metadata(result).get('inferred_types')}")
        print(f"\nConverted types:")
        print(result.df.dtypes)
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 4: generate_uuid
    # =========================================================================
    print_section("TEST 4: generate_uuid")

    plan = [{"op": "generate_uuid", "params": {
        "new_column": "record_id",
        "format": "short",
        "prefix": "REC-"
    }}]
    result = engine.execute(df.head(5), plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df[["name", "record_id"]].to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 5: dense_rank
    # =========================================================================
    print_section("TEST 5: dense_rank")

    plan = [{"op": "dense_rank", "params": {
        "column": "amount",
        "new_column": "amount_rank",
        "ascending": False
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df[["name", "amount", "amount_rank"]].to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 6: ntile
    # =========================================================================
    print_section("TEST 6: ntile")

    plan = [{"op": "ntile", "params": {
        "n": 4,
        "order_by": "amount",
        "new_column": "quartile"
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df[["name", "amount", "quartile"]].to_string())
        print(f"\nBucket counts: {get_metadata(result).get('bucket_counts')}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 7: lag
    # =========================================================================
    print_section("TEST 7: lag")

    # Create time series-like data
    df_ts = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=6),
        "sales": [100, 150, 120, 180, 200, 170]
    })
    print("Test data:")
    print(df_ts.to_string())

    plan = [{"op": "lag", "params": {
        "column": "sales",
        "new_column": "prev_sales",
        "offset": 1,
        "default": 0
    }}]
    result = engine.execute(df_ts, plan)
    print(f"\nSuccess: {result.success}")
    if result.success:
        print(result.df.to_string())
        print(f"\nMetadata: {get_metadata(result)}")
        results.append(True)
    else:
        print(f"Error: {result.error}")
        results.append(False)

    # =========================================================================
    # Test 8: lead
    # =========================================================================
    print_section("TEST 8: lead")

    plan = [{"op": "lead", "params": {
        "column": "sales",
        "new_column": "next_sales",
        "offset": 1
    }}]
    result = engine.execute(df_ts, plan)
    print(f"Success: {result.success}")
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
        "detect_header",
        "validate_schema",
        "infer_types",
        "generate_uuid",
        "dense_rank",
        "ntile",
        "lag",
        "lead",
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
