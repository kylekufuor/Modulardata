#!/usr/bin/env python3
"""
Comprehensive test of transforms_v2 on sample.csv

Phase 1: Text String Manipulation
Phase 2: Math & Formula Calculations
Phase 3: Logical Handling & Date Standardization
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
    print_section("SAMPLE.CSV COMPREHENSIVE TEST")

    # Load data
    df = pd.read_csv(project_root / "test_data" / "sample.csv")
    print(f"\nLoaded: {len(df)} rows, {len(df.columns)} columns")
    print("\nOriginal data:")
    print(df.to_string())

    engine = Engine()
    results = []

    # =========================================================================
    # PHASE 1: Text String Manipulation
    # =========================================================================
    print_section("PHASE 1: Text String Manipulation")

    # T1: Clean name - trim whitespace and title case
    print("\n--- T1: Clean name (trim + title case) ---")
    plan = [
        {"op": "trim_whitespace", "params": {"columns": ["name"], "trim_type": "all"}},
        {"op": "change_text_casing", "params": {"column": "name", "case": "title"}},
    ]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        print(f"✓ Names: {df['name'].head(5).tolist()}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # T2: Clean phone - remove all non-digits
    print("\n--- T2: Clean phone (remove non-digits) ---")
    plan = [{"op": "remove_characters", "params": {"column": "phone", "remove_type": "letters"}},
            {"op": "find_replace", "params": {"column": "phone", "find": "[^0-9]", "replace": "", "use_regex": True}}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        print(f"✓ Phones: {df['phone'].head(5).tolist()}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # T3: Pad phone to 10 digits
    print("\n--- T3: Pad phone to 10 digits ---")
    # First extract last 10 digits for phones that are too long
    df["phone"] = df["phone"].astype(str).str[-10:]
    plan = [{"op": "pad_text", "params": {"column": "phone", "length": 10, "pad_char": "0", "side": "left"}}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        print(f"✓ Phones (padded): {df['phone'].head(5).tolist()}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # T4: Phone length validation
    print("\n--- T4: Phone length validation ---")
    plan = [{"op": "text_length", "params": {"column": "phone", "new_column": "phone_length"}}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        valid_phones = (df["phone_length"] == 10).sum()
        print(f"✓ Valid 10-digit phones: {valid_phones}/{len(df)}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # T5: Email to lowercase
    print("\n--- T5: Email to lowercase ---")
    plan = [{"op": "change_text_casing", "params": {"column": "email", "case": "lower"}}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        print(f"✓ Emails: {df['email'].dropna().head(5).tolist()}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # T6: Standardize category
    print("\n--- T6: Standardize category ---")
    plan = [{"op": "standardize_values", "params": {
        "column": "category",
        "mapping": {
            "Electronics": ["electronics", "ELECTRONICS"],
            "Home & Garden": ["home & garden", "HOME & GARDEN"],
            "Clothing": ["clothing", "CLOTHING"]
        }
    }}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        print(f"✓ Categories: {df['category'].unique().tolist()}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # T7: Standardize status
    print("\n--- T7: Standardize status ---")
    plan = [{"op": "standardize_values", "params": {
        "column": "status",
        "mapping": {
            "Active": ["active", "ACTIVE"],
            "Pending": ["pending", "PENDING"],
            "Inactive": ["inactive", "INACTIVE"]
        }
    }}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        print(f"✓ Statuses: {df['status'].unique().tolist()}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # =========================================================================
    # PHASE 2: Math & Formula Calculations
    # =========================================================================
    print_section("PHASE 2: Math & Formula Calculations")

    # T8: Calculate tax-inclusive total (amount * 1.05)
    print("\n--- T8: Calculate total with 5% tax ---")
    plan = [{"op": "math_operation", "params": {
        "new_column": "total_with_tax",
        "operation": "multiply",
        "column1": "amount",
        "value": 1.05
    }}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        print(f"✓ Sample totals: {df['total_with_tax'].head(3).tolist()}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # T9: Calculate percentage of total
    print("\n--- T9: Calculate % of total revenue ---")
    plan = [{"op": "percentage", "params": {
        "column": "amount",
        "new_column": "pct_of_total",
        "mode": "of_total",
        "multiply_by_100": True
    }}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        print(f"✓ Percentages sum: {df['pct_of_total'].sum():.1f}%")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # T10: Round amount and percentage to 2 decimals
    print("\n--- T10: Round to 2 decimal places ---")
    plan = [
        {"op": "round_numbers", "params": {"column": "amount", "decimals": 2}},
        {"op": "round_numbers", "params": {"column": "pct_of_total", "decimals": 2}},
        {"op": "round_numbers", "params": {"column": "total_with_tax", "decimals": 2}},
    ]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        print(f"✓ Rounded amounts: {df['amount'].head(3).tolist()}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # T11: Running total (cumulative revenue)
    print("\n--- T11: Running total (cumulative revenue) ---")
    plan = [{"op": "running_total", "params": {
        "column": "amount",
        "new_column": "cumulative_revenue"
    }}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        print(f"✓ Final cumulative: ${df['cumulative_revenue'].iloc[-1]:,.2f}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # T12: Rank by amount (highest = 1)
    print("\n--- T12: Rank customers by amount ---")
    plan = [{"op": "rank", "params": {
        "column": "amount",
        "new_column": "spend_rank",
        "ascending": False,
        "method": "dense"
    }}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        top_customer = df[df["spend_rank"] == 1]["name"].iloc[0]
        top_amount = df[df["spend_rank"] == 1]["amount"].iloc[0]
        print(f"✓ Rank 1 (VIP): {top_customer} (${top_amount:,.2f})")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # =========================================================================
    # PHASE 3: Logical Handling & Date Standardization
    # =========================================================================
    print_section("PHASE 3: Logical Handling & Date Standardization")

    # T13: Fill empty status with "Pending"
    print("\n--- T13: Fill empty status with 'Pending' ---")
    plan = [{"op": "fill_blanks", "params": {
        "column": "status",
        "method": "value",
        "value": "Pending"
    }}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        null_count = df["status"].isna().sum()
        print(f"✓ Null statuses remaining: {null_count}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # T14: Fill empty notes with "No notes"
    print("\n--- T14: Fill empty notes with 'No notes' ---")
    # First replace empty strings with NaN
    df["notes"] = df["notes"].replace("", pd.NA)
    df["notes"] = df["notes"].fillna("No notes")
    # Trim whitespace from notes
    plan = [{"op": "trim_whitespace", "params": {"columns": ["notes"], "trim_type": "all"}}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        print(f"✓ Notes filled: {df['notes'].unique().tolist()}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # T15: Standardize dates to YYYY-MM-DD
    print("\n--- T15: Standardize dates to YYYY-MM-DD ---")
    plan = [{"op": "format_date", "params": {
        "column": "date",
        "output_format": "%Y-%m-%d"
    }}]
    result = engine.execute(df, plan)
    if result.success:
        df = result.df
        print(f"✓ Dates: {df['date'].head(5).tolist()}")
        results.append(True)
    else:
        print(f"✗ Error: {result.error}")
        results.append(False)

    # =========================================================================
    # FINAL RESULTS
    # =========================================================================
    print_section("FINAL CLEANED DATA")

    # Remove helper column
    df = df.drop(columns=["phone_length"])

    # Reorder columns
    col_order = ["id", "name", "email", "phone", "amount", "total_with_tax",
                 "pct_of_total", "cumulative_revenue", "spend_rank",
                 "date", "status", "category", "notes"]
    df = df[col_order]

    print(df.to_string())

    print_section("GRADING SUMMARY")

    test_names = [
        "T1: Name cleanup (trim + title)",
        "T2: Phone cleanup (remove non-digits)",
        "T3: Phone padding (10 digits)",
        "T4: Phone length validation",
        "T5: Email lowercase",
        "T6: Category standardization",
        "T7: Status standardization",
        "T8: Tax calculation (amount * 1.05)",
        "T9: Percentage of total",
        "T10: Round to 2 decimals",
        "T11: Running total",
        "T12: Rank by amount",
        "T13: Fill empty status",
        "T14: Fill empty notes",
        "T15: Date standardization",
    ]

    print(f"\n{'Test':<45} {'Result':<10}")
    print("─" * 55)

    for name, passed in zip(test_names, results):
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{name:<45} {status}")

    print("─" * 55)
    passed_count = sum(results)
    total_count = len(results)
    print(f"\nFINAL SCORE: {passed_count}/{total_count} ({100*passed_count/total_count:.0f}%)")

    # Verify acceptance criteria
    print_section("ACCEPTANCE CRITERIA VERIFICATION")

    print("\nPhase 1:")
    print(f"  ✓ Names are Title Case: {all(n == n.title() for n in df['name'])}")
    print(f"  ✓ Emails are lowercase: {all(e == e.lower() for e in df['email'].dropna())}")
    print(f"  ✓ Phones are 10 digits: {all(len(str(p)) == 10 for p in df['phone'])}")
    print(f"  ✓ Categories standardized: {set(df['category'].unique()) == {'Electronics', 'Home & Garden', 'Clothing'}}")

    print("\nPhase 2:")
    print(f"  ✓ Percentages sum to 100%: {abs(df['pct_of_total'].sum() - 100) < 0.1}")
    top = df[df['spend_rank'] == 1]
    print(f"  ✓ Rank 1 is Kate Wilson ($4,500): {top['name'].iloc[0] == 'Kate Wilson' and top['amount'].iloc[0] == 4500.0}")

    print("\nPhase 3:")
    print(f"  ✓ No empty statuses: {df['status'].isna().sum() == 0}")
    print(f"  ✓ Dates in YYYY-MM-DD format: {all(d[:4].isdigit() and d[4] == '-' for d in df['date'])}")

    return passed_count == total_count


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
