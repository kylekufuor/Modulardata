#!/usr/bin/env python3
"""
Quick test of the full 3-agent pipeline: Strategist → Engineer → Tester
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import numpy as np
from unittest.mock import patch

from agents.strategist import StrategistAgent
from agents.engineer import EngineerAgent
from agents.tester import TesterAgent
from lib.memory import ConversationContext
from lib.profiler import generate_profile


def create_test_data():
    """Create test data with various issues."""
    np.random.seed(42)
    return pd.DataFrame({
        'id': range(1, 101),
        'name': ['  Alice  ', 'Bob', '  Carol', None, 'Eve'] * 20,
        'email': [f"user{i}@test.com" if i % 10 != 0 else None for i in range(1, 101)],
        'age': [np.random.randint(18, 80) if i % 5 != 0 else None for i in range(1, 101)],
        'salary': [float(np.random.randint(30000, 150000)) for i in range(1, 101)],
    })


def create_context(df):
    """Create ConversationContext from DataFrame."""
    profile = generate_profile(df)
    return ConversationContext(
        session_id="test-session",
        current_node_id="node-001",
        parent_node_id="node-000",
        current_profile=profile,
        messages=[],
        recent_transformations=[],
        original_filename="test_data.csv",
        current_row_count=len(df),
        current_column_count=len(df.columns),
    )


def test_pipeline(request: str):
    """Test the full pipeline for a given request."""
    print(f"\n{'='*60}")
    print(f"REQUEST: \"{request}\"")
    print(f"{'='*60}")

    # Setup
    df = create_test_data()
    context = create_context(df)
    print(f"\nINPUT: {len(df)} rows × {len(df.columns)} columns")

    # 1. STRATEGIST
    print("\n--- STRATEGIST ---")
    with patch("agents.strategist.build_conversation_context") as mock:
        mock.return_value = context
        strategist = StrategistAgent()
        plan = strategist.create_plan("test-session", request)

    trans_type = plan.transformation_type
    if hasattr(trans_type, 'value'):
        trans_type = trans_type.value
    print(f"Plan: {trans_type}")
    print(f"Columns: {plan.get_target_column_names()}")
    print(f"Explanation: {plan.explanation}")

    # 2. ENGINEER
    print("\n--- ENGINEER ---")
    engineer = EngineerAgent()
    result_df, code = engineer.execute_on_dataframe(df, plan)
    print(f"Result: {len(result_df)} rows × {len(result_df.columns)} columns")
    print(f"Code: {code}")

    # 3. TESTER
    print("\n--- TESTER ---")
    tester = TesterAgent()
    test_result = tester.validate(df, result_df, plan)

    status = "✅ PASSED" if test_result.passed else "❌ FAILED"
    print(f"Status: {status}")
    print(f"Checks run: {len(test_result.checks_run)}")

    if test_result.issues:
        print("Issues:")
        for issue in test_result.issues:
            icon = "⚠️" if issue.severity.value == "warning" else "❌" if issue.severity.value == "error" else "ℹ️"
            print(f"  {icon} {issue.message}")

    print(f"\nSummary: {test_result.summary}")

    return test_result.passed


def main():
    """Run pipeline tests."""
    tests = [
        "remove rows where email is missing",
        "fill missing ages with the average",
        "trim whitespace from names",
        "sort by salary descending",
        "group by name and count",
    ]

    results = []
    for request in tests:
        try:
            passed = test_pipeline(request)
            results.append((request, "PASS" if passed else "WARN"))
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            results.append((request, "FAIL"))

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    for request, status in results:
        icon = "✅" if status == "PASS" else "⚠️" if status == "WARN" else "❌"
        print(f"{icon} {status}: {request}")


if __name__ == "__main__":
    main()
