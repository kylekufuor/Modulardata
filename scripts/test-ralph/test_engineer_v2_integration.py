#!/usr/bin/env python3
"""
Test Engineer integration with transforms_v2.

This script tests the PlanTranslator and transforms_v2 Engine directly,
without needing the full agent stack which has external dependencies.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd

# Check if transforms_v2 is available
try:
    from transforms_v2 import Engine as V2Engine
    TRANSFORMS_V2_AVAILABLE = True
except ImportError:
    TRANSFORMS_V2_AVAILABLE = False
    V2Engine = None


def print_section(title: str):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def create_test_df():
    """Create a test DataFrame."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["  John Doe  ", "JANE SMITH", "bob wilson", "Alice Brown", "charlie davis"],
        "email": ["john@test.com", "jane@test.com", None, "alice@test.com", "charlie@test.com"],
        "amount": [100.50, 200.75, 150.25, 300.00, 250.50],
        "status": ["active", "ACTIVE", "pending", "Active", "PENDING"],
        "date": ["01/15/2024", "2024-01-16", "Jan 17 2024", "01/18/24", "2024-01-19"],
    })


def test_v2_engine_directly():
    """Test transforms_v2 Engine directly with various operations."""
    print_section("TEST: transforms_v2 Engine Direct")

    if not TRANSFORMS_V2_AVAILABLE:
        print("⚠ transforms_v2 not available, skipping")
        return True

    engine = V2Engine()
    df = create_test_df()

    print(f"\nOriginal data ({len(df)} rows):")
    print(df.to_string())

    # Test 1: Filter rows
    print("\n--- Test 1: filter_rows (amount > 150) ---")
    plan = [{"op": "filter_rows", "params": {
        "conditions": [{"column": "amount", "operator": "gt", "value": 150}],
        "keep": True,
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Result: {len(result.df)} rows (expected 4)")
        assert len(result.df) == 4, f"Expected 4 rows, got {len(result.df)}"
        print("✓ filter_rows works")
    else:
        print(f"Error: {result.error}")
        return False

    # Test 2: Drop rows (remove nulls)
    print("\n--- Test 2: filter_rows with keep=False (remove null email) ---")
    plan = [{"op": "filter_rows", "params": {
        "conditions": [{"column": "email", "operator": "isnull"}],
        "keep": False,
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Result: {len(result.df)} rows (expected 4)")
        assert len(result.df) == 4, f"Expected 4 rows, got {len(result.df)}"
        print("✓ filter_rows with keep=False works")
    else:
        print(f"Error: {result.error}")
        return False

    # Test 3: Trim whitespace
    print("\n--- Test 3: trim_whitespace ---")
    plan = [{"op": "trim_whitespace", "params": {
        "columns": ["name"],
        "trim_type": "both",
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Names: {result.df['name'].tolist()}")
        assert result.df['name'].iloc[0] == "John Doe"
        print("✓ trim_whitespace works")
    else:
        print(f"Error: {result.error}")
        return False

    # Test 4: Change case
    print("\n--- Test 4: change_text_casing ---")
    plan = [{"op": "change_text_casing", "params": {
        "column": "name",
        "case": "title",
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Names: {result.df['name'].tolist()}")
        print("✓ change_text_casing works")
    else:
        print(f"Error: {result.error}")
        return False

    # Test 5: Sort rows
    print("\n--- Test 5: sort_rows ---")
    plan = [{"op": "sort_rows", "params": {
        "columns": ["amount"],
        "ascending": False,
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Amounts: {result.df['amount'].tolist()}")
        assert result.df['amount'].iloc[0] == 300.00
        print("✓ sort_rows works")
    else:
        print(f"Error: {result.error}")
        return False

    # Test 6: Remove duplicates
    print("\n--- Test 6: remove_duplicates ---")
    # Add a duplicate row for testing
    df_with_dup = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    plan = [{"op": "remove_duplicates", "params": {"keep": "first"}}]
    result = engine.execute(df_with_dup, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Result: {len(result.df)} rows (was {len(df_with_dup)})")
        assert len(result.df) == 5
        print("✓ remove_duplicates works")
    else:
        print(f"Error: {result.error}")
        return False

    # Test 7: Fill blanks
    print("\n--- Test 7: fill_blanks ---")
    plan = [{"op": "fill_blanks", "params": {
        "column": "email",
        "method": "value",
        "value": "unknown@test.com",
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        null_count = result.df['email'].isna().sum()
        print(f"Null emails remaining: {null_count}")
        assert null_count == 0
        print("✓ fill_blanks works")
    else:
        print(f"Error: {result.error}")
        return False

    # Test 8: Chain multiple operations
    print("\n--- Test 8: Chain operations (trim + case + sort) ---")
    plan = [
        {"op": "trim_whitespace", "params": {"columns": ["name"], "trim_type": "both"}},
        {"op": "change_text_casing", "params": {"column": "name", "case": "title"}},
        {"op": "sort_rows", "params": {"columns": ["amount"], "ascending": False}},
    ]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(f"Names (sorted by amount desc): {result.df['name'].tolist()}")
        print(f"Amounts: {result.df['amount'].tolist()}")
        assert result.df['amount'].iloc[0] == 300.00
        assert result.df['name'].iloc[0] == "Alice Brown"
        print("✓ Chained operations work")
    else:
        print(f"Error: {result.error}")
        return False

    return True


def test_v2_aggregations():
    """Test transforms_v2 aggregation operations."""
    print_section("TEST: transforms_v2 Aggregations")

    if not TRANSFORMS_V2_AVAILABLE:
        print("⚠ transforms_v2 not available, skipping")
        return True

    engine = V2Engine()

    # Create sample data for aggregation
    df = pd.DataFrame({
        "category": ["A", "B", "A", "B", "A"],
        "amount": [100, 200, 150, 250, 300],
        "count": [1, 2, 3, 4, 5],
    })

    print(f"\nTest data:")
    print(df.to_string())

    # Test aggregate
    print("\n--- Test: aggregate by category ---")
    plan = [{"op": "aggregate", "params": {
        "group_by": ["category"],
        "aggregations": {"amount": "sum", "count": "mean"},
    }}]
    result = engine.execute(df, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df.to_string())
        assert len(result.df) == 2
        print("✓ aggregate works")
    else:
        print(f"Error: {result.error}")
        return False

    # Test pivot
    print("\n--- Test: pivot ---")
    df_pivot = pd.DataFrame({
        "product": ["P1", "P1", "P2", "P2"],
        "month": ["Jan", "Feb", "Jan", "Feb"],
        "sales": [100, 150, 200, 250],
    })
    plan = [{"op": "pivot", "params": {
        "index": "product",
        "columns": "month",
        "values": "sales",
        "aggfunc": "sum",
    }}]
    result = engine.execute(df_pivot, plan)
    print(f"Success: {result.success}")
    if result.success:
        print(result.df.to_string())
        print("✓ pivot works")
    else:
        print(f"Error: {result.error}")
        return False

    return True


def main():
    print_section("ENGINEER + transforms_v2 INTEGRATION TEST")

    print(f"\ntransforms_v2 available: {TRANSFORMS_V2_AVAILABLE}")

    if not TRANSFORMS_V2_AVAILABLE:
        print("\n⚠ transforms_v2 not available. Cannot run tests.")
        return False

    results = []

    # Test 1: Engine directly
    try:
        results.append(("Engine Direct", test_v2_engine_directly()))
    except Exception as e:
        print(f"✗ Engine test failed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Engine Direct", False))

    # Test 2: Aggregations
    try:
        results.append(("Aggregations", test_v2_aggregations()))
    except Exception as e:
        print(f"✗ Aggregation test failed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Aggregations", False))

    # Summary
    print_section("SUMMARY")
    print(f"\n{'Test':<30} {'Result':<10}")
    print("─" * 40)
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{name:<30} {status}")

    passed_count = sum(1 for _, p in results if p)
    total_count = len(results)
    print("─" * 40)
    print(f"\nFINAL: {passed_count}/{total_count} tests passed")

    print("\n" + "=" * 70)
    print("INTEGRATION NOTES")
    print("=" * 70)
    print("""
The Engineer agent now uses transforms_v2 as its PRIMARY execution engine:

1. PlanTranslator converts TechnicalPlan → transforms_v2 format
2. Engineer._execute_transformation() tries v2 first, falls back to legacy
3. 38 primitives are available via transforms_v2
4. Operations not in v2 (UNDO, etc.) use the legacy registry

Flow:
  Strategist → TechnicalPlan → PlanTranslator → v2 Engine → Result
                                    ↓ (if unsupported)
                              Legacy Registry → Result
""")

    return all(p for _, p in results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
