#!/usr/bin/env python3
"""
Test harness for transforms_v2 library.

Tests all primitives against the messy marketing leads data.
Validates that each primitive:
1. Registers correctly
2. Has 3+ test prompts
3. Executes without errors
4. Produces expected results
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd

from transforms_v2 import (
    Engine,
    get_primitive,
    list_primitives,
    get_all_test_prompts,
    export_primitives_documentation,
    PRIMITIVE_REGISTRY,
)


def load_test_data() -> pd.DataFrame:
    """Load the messy marketing leads data."""
    csv_path = Path(__file__).parent / "messy_marketing_leads.csv"
    return pd.read_csv(csv_path)


def test_registry():
    """Test that all primitives are registered."""
    print("\n" + "=" * 60)
    print("TEST: Registry")
    print("=" * 60)

    primitives = list_primitives()
    print(f"Total primitives registered: {len(primitives)}")

    # Group by category
    by_category = {}
    for name in primitives:
        info = get_primitive(name).info()
        cat = info.category
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append(name)

    print("\nBy category:")
    for cat, names in sorted(by_category.items()):
        print(f"  {cat}: {len(names)} primitives")
        for name in sorted(names):
            print(f"    - {name}")

    # Check expected counts
    expected = {"rows": 7, "columns": 9, "format": 5}
    all_pass = True
    for cat, expected_count in expected.items():
        actual = len(by_category.get(cat, []))
        status = "✓" if actual == expected_count else "✗"
        print(f"\n  {cat}: {actual}/{expected_count} {status}")
        if actual != expected_count:
            all_pass = False

    return all_pass


def test_test_prompts():
    """Test that each primitive has 3+ test prompts."""
    print("\n" + "=" * 60)
    print("TEST: Test Prompts (for Strategist training)")
    print("=" * 60)

    all_prompts = get_all_test_prompts()
    print(f"Total test prompts: {len(all_prompts)}")

    # Count by primitive
    by_primitive = {}
    for prompt in all_prompts:
        name = prompt["primitive_name"]
        if name not in by_primitive:
            by_primitive[name] = []
        by_primitive[name].append(prompt)

    all_pass = True
    for name, prompts in sorted(by_primitive.items()):
        status = "✓" if len(prompts) >= 3 else "✗"
        print(f"  {name}: {len(prompts)} prompts {status}")
        if len(prompts) < 3:
            all_pass = False

    return all_pass


def test_rows_primitives(df: pd.DataFrame):
    """Test all ROWS primitives."""
    print("\n" + "=" * 60)
    print("TEST: ROWS Primitives")
    print("=" * 60)

    engine = Engine()
    results = {}

    # Test sort_rows
    print("\n1. sort_rows:")
    plan = [{"op": "sort_rows", "params": {"columns": ["lead_score"], "ascending": False}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   First row lead_score: {result.df['lead_score'].iloc[0]}")
    results["sort_rows"] = result.success

    # Test filter_rows
    print("\n2. filter_rows:")
    plan = [{"op": "filter_rows", "params": {
        "conditions": [{"column": "status", "operator": "notnull"}],
        "keep": True
    }}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Rows before: {len(df)}, after: {len(result.df)}")
    results["filter_rows"] = result.success

    # Test limit_rows
    print("\n3. limit_rows:")
    plan = [{"op": "limit_rows", "params": {"count": 5}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Rows: {len(result.df)}")
    results["limit_rows"] = result.success

    # Test remove_duplicates
    print("\n4. remove_duplicates:")
    plan = [{"op": "remove_duplicates", "params": {"subset": ["lead_id"]}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Rows before: {len(df)}, after: {len(result.df)}")
        print(f"   Duplicates removed: {result.steps[0].result.metadata.get('duplicates_removed', 0)}")
    results["remove_duplicates"] = result.success

    # Test fill_blanks
    print("\n5. fill_blanks:")
    plan = [{"op": "fill_blanks", "params": {"column": "status", "method": "value", "value": "Unknown"}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        nulls_after = result.df["status"].isna().sum()
        print(f"   Nulls after: {nulls_after}")
    results["fill_blanks"] = result.success

    # Test merge_duplicates
    print("\n6. merge_duplicates:")
    plan = [{"op": "merge_duplicates", "params": {
        "group_by": ["email"],
        "aggregations": {"lead_score": "max"},
        "default_agg": "first"
    }}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Rows before: {len(df)}, after: {len(result.df)}")
    results["merge_duplicates"] = result.success

    # Test add_rows
    print("\n7. add_rows:")
    plan = [{"op": "add_rows", "params": {
        "rows": [{"lead_id": 9999, "first_name": "Test", "email": "test@test.com"}],
        "position": "bottom"
    }}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Rows after: {len(result.df)}")
    results["add_rows"] = result.success

    return results


def test_columns_primitives(df: pd.DataFrame):
    """Test all COLUMNS primitives."""
    print("\n" + "=" * 60)
    print("TEST: COLUMNS Primitives")
    print("=" * 60)

    engine = Engine()
    results = {}

    # Test select_columns
    print("\n1. select_columns:")
    plan = [{"op": "select_columns", "params": {"columns": ["lead_id", "first_name", "email"]}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Columns: {list(result.df.columns)}")
    results["select_columns"] = result.success

    # Test remove_columns
    print("\n2. remove_columns:")
    plan = [{"op": "remove_columns", "params": {"columns": ["campaign_source"]}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   'campaign_source' removed: {'campaign_source' not in result.df.columns}")
    results["remove_columns"] = result.success

    # Test rename_columns
    print("\n3. rename_columns:")
    plan = [{"op": "rename_columns", "params": {"mapping": {"first_name": "fname", "last_name": "lname"}}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Has 'fname': {'fname' in result.df.columns}")
    results["rename_columns"] = result.success

    # Test reorder_columns
    print("\n4. reorder_columns:")
    plan = [{"op": "reorder_columns", "params": {"order": ["email", "first_name"]}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   First column: {result.df.columns[0]}")
    results["reorder_columns"] = result.success

    # Test add_column
    print("\n5. add_column:")
    plan = [{"op": "add_column", "params": {"name": "source", "value": "import"}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Has 'source': {'source' in result.df.columns}")
    results["add_column"] = result.success

    # Test split_column (create test data first)
    print("\n6. split_column:")
    # Create a test df with full_name
    test_df = df.copy()
    test_df["full_name"] = test_df["first_name"].astype(str) + " " + test_df["last_name"].astype(str)
    plan = [{"op": "split_column", "params": {
        "column": "full_name",
        "delimiter": " ",
        "new_columns": ["fname_split", "lname_split"]
    }}]
    result = engine.execute(test_df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Has 'fname_split': {'fname_split' in result.df.columns}")
    results["split_column"] = result.success

    # Test merge_columns
    print("\n7. merge_columns:")
    plan = [{"op": "merge_columns", "params": {
        "columns": ["first_name", "last_name"],
        "new_column": "full_name",
        "separator": " "
    }}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Sample full_name: {result.df['full_name'].iloc[0]}")
    results["merge_columns"] = result.success

    # Test copy_column
    print("\n8. copy_column:")
    plan = [{"op": "copy_column", "params": {"source": "email", "destination": "email_backup"}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Has 'email_backup': {'email_backup' in result.df.columns}")
    results["copy_column"] = result.success

    # Test change_column_type
    print("\n9. change_column_type:")
    plan = [{"op": "change_column_type", "params": {"column": "lead_id", "to_type": "string"}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Type: {result.df['lead_id'].dtype}")
    results["change_column_type"] = result.success

    return results


def test_format_primitives(df: pd.DataFrame):
    """Test all FORMAT primitives."""
    print("\n" + "=" * 60)
    print("TEST: FORMAT Primitives")
    print("=" * 60)

    engine = Engine()
    results = {}

    # Test format_date
    print("\n1. format_date:")
    plan = [{"op": "format_date", "params": {"column": "created_date", "output_format": "%Y-%m-%d"}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Sample date: {result.df['created_date'].iloc[0]}")
    results["format_date"] = result.success

    # Test format_phone
    print("\n2. format_phone:")
    plan = [{"op": "format_phone", "params": {"column": "phone_number", "format": "(XXX) XXX-XXXX"}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Sample phone: {result.df['phone_number'].iloc[0]}")
        print(f"   Phones formatted: {result.steps[0].result.metadata.get('phones_formatted', 0)}")
    results["format_phone"] = result.success

    # Test change_text_casing
    print("\n3. change_text_casing:")
    plan = [{"op": "change_text_casing", "params": {"column": "first_name", "case": "title"}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Sample names: {result.df['first_name'].head(3).tolist()}")
    results["change_text_casing"] = result.success

    # Test trim_whitespace
    print("\n4. trim_whitespace:")
    plan = [{"op": "trim_whitespace", "params": {"columns": ["first_name", "last_name"], "trim_type": "all"}}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    results["trim_whitespace"] = result.success

    # Test standardize_values
    print("\n5. standardize_values:")
    plan = [{"op": "standardize_values", "params": {
        "column": "campaign_source",
        "mapping": {
            "Google Ads": ["google ads", "Google ads", "GOOGLE ADS"],
            "LinkedIn": ["linkedin", "LINKEDIN"]
        }
    }}]
    result = engine.execute(df, plan)
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Unique sources: {result.df['campaign_source'].unique().tolist()}")
    results["standardize_values"] = result.success

    return results


def test_full_pipeline(df: pd.DataFrame):
    """Test a full cleaning pipeline."""
    print("\n" + "=" * 60)
    print("TEST: Full Cleaning Pipeline")
    print("=" * 60)

    engine = Engine()

    plan = [
        # 1. Remove duplicates by lead_id
        {"op": "remove_duplicates", "params": {"subset": ["lead_id"]}},

        # 2. Fill blank statuses
        {"op": "fill_blanks", "params": {"column": "status", "method": "value", "value": "Unknown"}},

        # 3. Format names to title case
        {"op": "change_text_casing", "params": {"column": "first_name", "case": "title"}},
        {"op": "change_text_casing", "params": {"column": "last_name", "case": "title"}},

        # 4. Format emails to lowercase
        {"op": "change_text_casing", "params": {"column": "email", "case": "lower"}},

        # 5. Format phone numbers
        {"op": "format_phone", "params": {"column": "phone_number", "format": "(XXX) XXX-XXXX"}},

        # 6. Standardize campaign sources
        {"op": "standardize_values", "params": {
            "column": "campaign_source",
            "mapping": {
                "Google Ads": ["google ads", "Google ads"],
                "LinkedIn": ["linkedin", "LINKEDIN"]
            }
        }},

        # 7. Standardize statuses
        {"op": "standardize_values", "params": {
            "column": "status",
            "mapping": {
                "Contacted": ["contacted", "CONTACTED"],
                "Qualified": ["qualified", "QUALIFIED", "Qualifed"],
                "New": ["new", "NEW"]
            }
        }},

        # 8. Format dates
        {"op": "format_date", "params": {"column": "created_date", "output_format": "%Y-%m-%d"}},

        # 9. Sort by lead_score descending
        {"op": "sort_rows", "params": {"columns": ["lead_score"], "ascending": False}},
    ]

    print(f"Executing {len(plan)} step pipeline...")
    print(f"Input: {len(df)} rows, {len(df.columns)} columns\n")

    result = engine.execute(df, plan)

    if result.success:
        print(f"✓ Pipeline completed successfully!")
        print(f"  Output: {len(result.df)} rows, {len(result.df.columns)} columns")
        print(f"  Total time: {result.total_duration_ms:.2f}ms")

        print("\n  Step results:")
        for step in result.steps:
            print(f"    {step.step_index + 1}. {step.operation}: {step.duration_ms:.1f}ms")

        print("\n  Sample output (first 5 rows):")
        print(result.df[["lead_id", "first_name", "last_name", "email", "phone_number", "status"]].head().to_string())
    else:
        print(f"✗ Pipeline failed at step {result.error_step}")
        print(f"  Error: {result.error}")

    return result.success


def main():
    """Run all tests."""
    print("=" * 60)
    print("transforms_v2 Test Harness")
    print("=" * 60)

    # Load test data
    print("\nLoading test data...")
    df = load_test_data()
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")

    # Run tests
    all_results = {}

    # Test registry
    all_results["registry"] = test_registry()

    # Test test prompts
    all_results["test_prompts"] = test_test_prompts()

    # Test primitives by category
    all_results["rows"] = all(test_rows_primitives(df).values())
    all_results["columns"] = all(test_columns_primitives(df).values())
    all_results["format"] = all(test_format_primitives(df).values())

    # Test full pipeline
    all_results["pipeline"] = test_full_pipeline(df)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    total_pass = 0
    total_tests = len(all_results)

    for test_name, passed in all_results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {test_name}: {status}")
        if passed:
            total_pass += 1

    print(f"\nTotal: {total_pass}/{total_tests} tests passed")

    # Print documentation sample
    print("\n" + "=" * 60)
    print("DOCUMENTATION (first 50 lines)")
    print("=" * 60)
    docs = export_primitives_documentation()
    for line in docs.split("\n")[:50]:
        print(line)
    print("...")

    # Print test prompts sample
    print("\n" + "=" * 60)
    print("TEST PROMPTS (first 10)")
    print("=" * 60)
    prompts = get_all_test_prompts()
    for p in prompts[:10]:
        print(f"  [{p['category']}:{p['primitive_name']}]")
        print(f"    \"{p['prompt']}\"")
        print(f"    → {p['expected_params']}")
        print()

    return total_pass == total_tests


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
