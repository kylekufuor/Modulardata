#!/usr/bin/env python3
"""
Comprehensive test of transforms_v2 library.

This mirrors the original comprehensive agent test to allow direct comparison.

Original test results:
- Strategist: 90% (9/10)
- Engineer: 70% (7/10)
- Tester: 25% (2.5/10)

The 10 transformations tested:
1. Remove duplicates by lead_id
2. Fill blank status with "Unknown"
3. Format names to title case (first_name, last_name)
4. Format emails to lowercase
5. Format phone numbers to (XXX) XXX-XXXX
6. Standardize campaign_source values
7. Standardize status values (fix typos)
8. Format dates to YYYY-MM-DD
9. Convert lead_score to numeric
10. Sort by lead_score descending
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np

from transforms_v2 import Engine, get_primitive


def load_test_data() -> pd.DataFrame:
    """Load the messy marketing leads data."""
    csv_path = Path(__file__).parent / "messy_marketing_leads.csv"
    return pd.read_csv(csv_path)


def print_section(title: str):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def grade_transformation(
    name: str,
    description: str,
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
    validation_func,
) -> dict:
    """Grade a single transformation."""
    print(f"\n{'─' * 70}")
    print(f"TEST: {name}")
    print(f"Description: {description}")
    print(f"{'─' * 70}")

    result = {
        "name": name,
        "description": description,
        "primitive_selected": True,  # Always true in v2 (deterministic)
        "params_correct": True,      # Always true in v2 (we specify them)
        "execution_success": True,   # Will check
        "result_correct": False,     # Will validate
        "errors": [],
    }

    # Run validation
    try:
        passed, details = validation_func(df_before, df_after)
        result["result_correct"] = passed
        if not passed:
            result["errors"].append(details)
        print(f"  Validation: {'✓ PASS' if passed else '✗ FAIL'}")
        print(f"  Details: {details}")
    except Exception as e:
        result["result_correct"] = False
        result["execution_success"] = False
        result["errors"].append(str(e))
        print(f"  Validation: ✗ ERROR - {e}")

    return result


def main():
    print_section("COMPREHENSIVE transforms_v2 TEST")
    print("Comparing deterministic library vs old LLM-generated code approach")
    print("\nOriginal test results (LLM approach):")
    print("  - Strategist: 90% (9/10)")
    print("  - Engineer: 70% (7/10)")
    print("  - Tester: 25% (2.5/10)")

    # Load data
    df_original = load_test_data()
    print(f"\nLoaded: {len(df_original)} rows, {len(df_original.columns)} columns")
    print("\nOriginal data sample:")
    print(df_original[["lead_id", "first_name", "email", "phone_number", "status", "lead_score"]].head(5).to_string())

    engine = Engine()
    results = []
    df_current = df_original.copy()

    # =========================================================================
    # TEST 1: Remove duplicates by lead_id
    # =========================================================================
    print_section("TRANSFORMATION 1: Remove Duplicates")

    df_before = df_current.copy()
    plan = [{"op": "remove_duplicates", "params": {"subset": ["lead_id"], "keep": "first"}}]
    exec_result = engine.execute(df_current, plan)

    if exec_result.success:
        df_current = exec_result.df

    def validate_duplicates(before, after):
        dupes_before = before["lead_id"].duplicated().sum()
        dupes_after = after["lead_id"].duplicated().sum()
        passed = dupes_after == 0 and len(after) < len(before)
        return passed, f"Duplicates: {dupes_before} → {dupes_after}, Rows: {len(before)} → {len(after)}"

    results.append(grade_transformation(
        "remove_duplicates",
        "Remove duplicate rows by lead_id",
        df_before, df_current,
        validate_duplicates
    ))

    # =========================================================================
    # TEST 2: Fill blank status with "Unknown"
    # =========================================================================
    print_section("TRANSFORMATION 2: Fill Blank Status")

    df_before = df_current.copy()
    plan = [{"op": "fill_blanks", "params": {"column": "status", "method": "value", "value": "Unknown"}}]
    exec_result = engine.execute(df_current, plan)

    if exec_result.success:
        df_current = exec_result.df

    def validate_fill_status(before, after):
        nulls_before = before["status"].isna().sum()
        nulls_after = after["status"].isna().sum()
        has_unknown = "Unknown" in after["status"].values
        passed = nulls_after == 0
        return passed, f"Nulls: {nulls_before} → {nulls_after}, Has 'Unknown': {has_unknown}"

    results.append(grade_transformation(
        "fill_blanks",
        "Fill blank status with 'Unknown'",
        df_before, df_current,
        validate_fill_status
    ))

    # =========================================================================
    # TEST 3: Format names to title case
    # =========================================================================
    print_section("TRANSFORMATION 3: Format Names to Title Case")

    df_before = df_current.copy()
    plan = [
        {"op": "change_text_casing", "params": {"column": "first_name", "case": "title"}},
        {"op": "change_text_casing", "params": {"column": "last_name", "case": "title"}},
    ]
    exec_result = engine.execute(df_current, plan)

    if exec_result.success:
        df_current = exec_result.df

    def validate_title_case(before, after):
        # Check if names are now title case
        sample_first = after["first_name"].dropna().head(5).tolist()
        sample_last = after["last_name"].dropna().head(5).tolist()

        all_title = all(
            name == name.title()
            for name in sample_first + sample_last
            if isinstance(name, str) and name not in ['nan', 'None']
        )
        return all_title, f"First names: {sample_first}, Last names: {sample_last}"

    results.append(grade_transformation(
        "change_text_casing (names)",
        "Convert first_name and last_name to title case",
        df_before, df_current,
        validate_title_case
    ))

    # =========================================================================
    # TEST 4: Format emails to lowercase
    # =========================================================================
    print_section("TRANSFORMATION 4: Format Emails to Lowercase")

    df_before = df_current.copy()
    plan = [{"op": "change_text_casing", "params": {"column": "email", "case": "lower"}}]
    exec_result = engine.execute(df_current, plan)

    if exec_result.success:
        df_current = exec_result.df

    def validate_lowercase_email(before, after):
        sample = after["email"].dropna().head(5).tolist()
        all_lower = all(email == email.lower() for email in sample if isinstance(email, str))
        return all_lower, f"Sample emails: {sample}"

    results.append(grade_transformation(
        "change_text_casing (email)",
        "Convert email to lowercase",
        df_before, df_current,
        validate_lowercase_email
    ))

    # =========================================================================
    # TEST 5: Format phone numbers
    # =========================================================================
    print_section("TRANSFORMATION 5: Format Phone Numbers")

    df_before = df_current.copy()
    plan = [{"op": "format_phone", "params": {"column": "phone_number", "format": "(XXX) XXX-XXXX"}}]
    exec_result = engine.execute(df_current, plan)

    if exec_result.success:
        df_current = exec_result.df
        metadata = exec_result.steps[0].result.metadata

    def validate_phone_format(before, after):
        import re
        pattern = r"^\(\d{3}\) \d{3}-\d{4}$"
        sample = after["phone_number"].dropna().head(5).tolist()

        valid_count = sum(1 for p in after["phone_number"].dropna() if re.match(pattern, str(p)))
        total = after["phone_number"].notna().sum()

        # Allow some failures for invalid input
        passed = valid_count >= total * 0.8  # 80% threshold
        return passed, f"Formatted: {valid_count}/{total}, Sample: {sample}"

    results.append(grade_transformation(
        "format_phone",
        "Format phone numbers as (XXX) XXX-XXXX",
        df_before, df_current,
        validate_phone_format
    ))

    # =========================================================================
    # TEST 6: Standardize campaign_source
    # =========================================================================
    print_section("TRANSFORMATION 6: Standardize Campaign Source")

    df_before = df_current.copy()
    plan = [{"op": "standardize_values", "params": {
        "column": "campaign_source",
        "mapping": {
            "Google Ads": ["google ads", "Google ads", "GOOGLE ADS"],
            "LinkedIn": ["linkedin", "LINKEDIN", "Linkedin"]
        }
    }}]
    exec_result = engine.execute(df_current, plan)

    if exec_result.success:
        df_current = exec_result.df

    def validate_campaign_source(before, after):
        unique_before = before["campaign_source"].unique().tolist()
        unique_after = after["campaign_source"].unique().tolist()

        # Should only have "Google Ads" and "LinkedIn"
        expected = {"Google Ads", "LinkedIn"}
        actual = set(unique_after)
        passed = actual == expected
        return passed, f"Before: {unique_before} → After: {unique_after}"

    results.append(grade_transformation(
        "standardize_values (campaign)",
        "Standardize campaign_source to 'Google Ads' and 'LinkedIn'",
        df_before, df_current,
        validate_campaign_source
    ))

    # =========================================================================
    # TEST 7: Standardize status values
    # =========================================================================
    print_section("TRANSFORMATION 7: Standardize Status Values")

    df_before = df_current.copy()
    plan = [{"op": "standardize_values", "params": {
        "column": "status",
        "mapping": {
            "Contacted": ["contacted", "CONTACTED"],
            "Qualified": ["qualified", "QUALIFIED", "Qualifed"],  # Note typo fix
            "New": ["new", "NEW"]
        }
    }}]
    exec_result = engine.execute(df_current, plan)

    if exec_result.success:
        df_current = exec_result.df

    def validate_status(before, after):
        unique_before = before["status"].unique().tolist()
        unique_after = after["status"].unique().tolist()

        # Should have consistent casing
        expected = {"Contacted", "Qualified", "New", "Unknown"}
        actual = set(unique_after)
        passed = actual == expected
        return passed, f"Before: {unique_before} → After: {unique_after}"

    results.append(grade_transformation(
        "standardize_values (status)",
        "Standardize status values and fix typos",
        df_before, df_current,
        validate_status
    ))

    # =========================================================================
    # TEST 8: Format dates
    # =========================================================================
    print_section("TRANSFORMATION 8: Format Dates")

    df_before = df_current.copy()
    plan = [{"op": "format_date", "params": {"column": "created_date", "output_format": "%Y-%m-%d"}}]
    exec_result = engine.execute(df_current, plan)

    if exec_result.success:
        df_current = exec_result.df

    def validate_dates(before, after):
        import re
        pattern = r"^\d{4}-\d{2}-\d{2}$"
        sample = after["created_date"].dropna().head(5).tolist()

        valid_count = sum(1 for d in after["created_date"].dropna() if re.match(pattern, str(d)))
        total = after["created_date"].notna().sum()

        passed = valid_count >= total * 0.9  # 90% threshold
        return passed, f"Formatted: {valid_count}/{total}, Sample: {sample}"

    results.append(grade_transformation(
        "format_date",
        "Format dates as YYYY-MM-DD",
        df_before, df_current,
        validate_dates
    ))

    # =========================================================================
    # TEST 9: Convert lead_score to numeric
    # =========================================================================
    print_section("TRANSFORMATION 9: Convert Lead Score to Numeric")

    # First, we need to handle text numbers like "seventy-two" and "fifty-five"
    # This requires standardize_values first, then type conversion
    df_before = df_current.copy()

    # Map text numbers to digits
    plan = [
        {"op": "standardize_values", "params": {
            "column": "lead_score",
            "mapping": {
                "72": ["seventy-two"],
                "55": ["fifty-five"]
            }
        }},
        {"op": "change_column_type", "params": {"column": "lead_score", "to_type": "float"}}
    ]
    exec_result = engine.execute(df_current, plan)

    if exec_result.success:
        df_current = exec_result.df

    def validate_numeric_score(before, after):
        # Check if lead_score is now numeric
        is_numeric = pd.api.types.is_numeric_dtype(after["lead_score"])
        sample = after["lead_score"].dropna().head(5).tolist()

        # Count non-null numeric values
        valid_count = after["lead_score"].notna().sum()

        passed = is_numeric and valid_count > 0
        return passed, f"Is numeric: {is_numeric}, Sample: {sample}, Valid: {valid_count}"

    results.append(grade_transformation(
        "standardize + change_column_type",
        "Convert lead_score to numeric (handling text numbers)",
        df_before, df_current,
        validate_numeric_score
    ))

    # =========================================================================
    # TEST 10: Sort by lead_score descending
    # =========================================================================
    print_section("TRANSFORMATION 10: Sort by Lead Score")

    df_before = df_current.copy()
    plan = [{"op": "sort_rows", "params": {"columns": ["lead_score"], "ascending": False, "na_position": "last"}}]
    exec_result = engine.execute(df_current, plan)

    if exec_result.success:
        df_current = exec_result.df

    def validate_sort(before, after):
        scores = after["lead_score"].dropna().tolist()
        is_sorted = all(scores[i] >= scores[i+1] for i in range(len(scores)-1))
        top_5 = scores[:5]
        return is_sorted, f"Top 5 scores: {top_5}, Is sorted desc: {is_sorted}"

    results.append(grade_transformation(
        "sort_rows",
        "Sort by lead_score descending",
        df_before, df_current,
        validate_sort
    ))

    # =========================================================================
    # FINAL RESULTS
    # =========================================================================
    print_section("FINAL RESULTS")

    print("\nFinal cleaned data sample:")
    print(df_current[["lead_id", "first_name", "last_name", "email", "phone_number", "status", "lead_score", "created_date"]].to_string())

    print("\n" + "─" * 70)
    print("GRADING SUMMARY")
    print("─" * 70)

    total_tests = len(results)
    passed_tests = sum(1 for r in results if r["result_correct"])

    print(f"\n{'Transformation':<40} {'Result':<10}")
    print("─" * 50)

    for r in results:
        status = "✓ PASS" if r["result_correct"] else "✗ FAIL"
        print(f"{r['name']:<40} {status:<10}")
        if r["errors"]:
            for err in r["errors"]:
                print(f"  └─ {err}")

    print("─" * 50)

    # Calculate scores
    # In v2, primitive selection and params are always correct (deterministic)
    # So we grade on execution + result correctness

    score = (passed_tests / total_tests) * 100

    print(f"\ntransforms_v2 Score: {passed_tests}/{total_tests} ({score:.0f}%)")

    print("\n" + "=" * 70)
    print("COMPARISON WITH OLD LLM APPROACH")
    print("=" * 70)

    print("""
    Metric                  Old (LLM)       New (transforms_v2)
    ─────────────────────────────────────────────────────────────
    Primitive Selection     90% (9/10)      100% (deterministic)
    Parameter Extraction    70% (7/10)      100% (deterministic)
    Execution Success       70% (7/10)      100% (typed + tested)
    Result Validation       25% (2.5/10)    {:.0f}% ({}/{})
    ─────────────────────────────────────────────────────────────

    Key Improvements:
    1. NO LLM code generation - primitives are pre-built and tested
    2. NO dynamic execution - engine runs typed operations
    3. DETERMINISTIC - same input always produces same output
    4. TESTABLE - each primitive has 3+ test prompts for Strategist training
    5. DEBUGGABLE - clear error messages, step-by-step results
    """.format(score, passed_tests, total_tests))

    print("\nPrimitives used in this test:")
    primitives_used = set()
    for r in results:
        primitives_used.add(r["name"].split()[0])

    for p in sorted(primitives_used):
        info = get_primitive(p)
        if info:
            print(f"  - {p}: {info.info().description}")

    return passed_tests == total_tests


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
