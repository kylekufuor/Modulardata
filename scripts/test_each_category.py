#!/usr/bin/env python3
"""
Test one transformation from each category interactively.
Tests the full Strategist → Engineer pipeline.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import numpy as np
from unittest.mock import patch

from agents.strategist import StrategistAgent, StrategyError
from agents.engineer import EngineerAgent
from lib.memory import ConversationContext, ChatMessage
from lib.profiler import generate_profile
from core.models.profile import DataProfile, ColumnProfile, SemanticType


def create_test_df():
    """Create a test DataFrame with diverse data for all transformation types."""
    np.random.seed(42)
    n_rows = 100

    return pd.DataFrame({
        'id': range(1, n_rows + 1),
        'name': ['  Alice  ', 'Bob', '  Carol', 'Dave  ', 'Eve'] * 20,  # Has whitespace
        'email': [f"user{i}@test.com" if i % 10 != 0 else None for i in range(1, n_rows + 1)],
        'age': [np.random.randint(18, 80) if i % 5 != 0 else None for i in range(1, n_rows + 1)],
        'salary': [float(np.random.randint(30000, 150000)) for i in range(1, n_rows + 1)],
        'department': ['Sales', 'Engineering', 'Marketing', 'Engineering', 'Sales'] * 20,
        'hire_date': [f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}" for i in range(1, n_rows + 1)],
        'status': ['active', 'ACTIVE', 'Active', 'inactive', 'INACTIVE'] * 20,  # Inconsistent case
    })


def create_context(df):
    """Create a ConversationContext from DataFrame."""
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


def test_transformation(category, request, df, context):
    """Test a single transformation through Strategist → Engineer."""
    print(f"\n{'='*60}")
    print(f"CATEGORY: {category}")
    print(f"REQUEST: \"{request}\"")
    print(f"{'='*60}")

    # Show before state
    print(f"\nBEFORE: {len(df)} rows x {len(df.columns)} columns")

    try:
        # Get plan from Strategist
        with patch("agents.strategist.build_conversation_context") as mock_build:
            mock_build.return_value = context

            agent = StrategistAgent()
            plan = agent.create_plan(
                session_id=context.session_id,
                user_message=request
            )

        trans_type = plan.transformation_type
        if hasattr(trans_type, 'value'):
            trans_type = trans_type.value

        print(f"\nSTRATEGIST PLAN:")
        print(f"  Type: {trans_type}")
        print(f"  Columns: {plan.get_target_column_names()}")
        print(f"  Parameters: {plan.parameters}")
        print(f"  Explanation: {plan.explanation}")

        # Execute through Engineer
        engineer = EngineerAgent()
        result_df, code = engineer.execute_on_dataframe(df, plan)

        print(f"\nENGINEER RESULT:")
        print(f"  AFTER: {len(result_df)} rows x {len(result_df.columns)} columns")
        print(f"  CODE: {code}")

        # Show sample of affected data
        print(f"\nSAMPLE (first 3 rows):")
        print(result_df.head(3).to_string(index=False))

        print(f"\n✅ SUCCESS")
        return True, result_df

    except StrategyError as e:
        print(f"\n❌ STRATEGIST ERROR: {e.message}")
        if e.suggestion:
            print(f"   Suggestion: {e.suggestion}")
        return False, df

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False, df


def main():
    """Run tests for each category."""
    df = create_test_df()
    context = create_context(df)

    print("\n" + "="*60)
    print("TESTING EACH TRANSFORMATION CATEGORY")
    print("="*60)
    print(f"Test data: {len(df)} rows x {len(df.columns)} columns")
    print(f"Columns: {list(df.columns)}")

    # Define tests for each category
    tests = [
        # Category, Request
        ("1. CLEANING - trim_whitespace", "trim whitespace from the name column"),
        ("2. CLEANING - change_case", "convert the status column to lowercase"),
        ("3. CLEANING - fill_nulls", "fill missing ages with the average"),
        ("4. FILTERING - drop_rows", "remove rows where email is missing"),
        ("5. FILTERING - sort_rows", "sort by salary descending"),
        ("6. FILTERING - select_columns", "keep only id, name, and email columns"),
        ("7. RESTRUCTURING - rename_column", "rename 'hire_date' to 'start_date'"),
        ("8. COLUMN_MATH - round_numbers", "round salary to 0 decimal places"),
        ("9. AGGREGATION - group_by", "group by department and calculate average salary"),
    ]

    results = []

    for category, request in tests:
        # Create fresh context and df for each test
        df = create_test_df()
        context = create_context(df)

        success, _ = test_transformation(category, request, df, context)
        results.append((category, success))

    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    passed = sum(1 for _, success in results if success)
    failed = len(results) - passed

    for category, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {status}: {category}")

    print(f"\nTotal: {passed}/{len(results)} passed")

    if failed > 0:
        print(f"\n⚠️  {failed} test(s) failed - investigate above errors")
        sys.exit(1)
    else:
        print("\n✅ All categories working!")


if __name__ == "__main__":
    main()
