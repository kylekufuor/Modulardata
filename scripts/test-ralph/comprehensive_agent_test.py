#!/usr/bin/env python3
"""
Comprehensive Agent Test Suite
Tests all 3 agents (Strategist, Engineer, Tester) on messy marketing leads data.

Grades each agent on:
- Strategist: Correct transformation type selection, parameter accuracy, acceptance criteria
- Engineer: Code correctness, execution success
- Tester: Validation accuracy, catching failures
"""

import sys
import os
import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np

# Import agents
from agents.strategist import StrategistAgent
from agents.engineer import EngineerAgent
from agents.tester import TesterAgent
from agents.models.technical_plan import TechnicalPlan
from lib.profiler import generate_profile
from lib.memory import ConversationContext


@dataclass
class TestResult:
    """Result of a single test."""
    phase: str
    test_name: str
    user_request: str

    # Strategist results
    strategist_transformation_type: str = ""
    strategist_expected_type: str = ""
    strategist_correct_type: bool = False
    strategist_has_acceptance_criteria: bool = False
    strategist_acceptance_criteria_count: int = 0
    strategist_plan_json: dict = field(default_factory=dict)
    strategist_error: str = ""

    # Engineer results
    engineer_executed: bool = False
    engineer_code: str = ""
    engineer_error: str = ""
    engineer_rows_before: int = 0
    engineer_rows_after: int = 0

    # Tester results
    tester_passed: bool = False
    tester_issues: list = field(default_factory=list)
    tester_acceptance_criteria_validated: bool = False
    tester_caught_failure: bool = False

    # Outcome
    data_actually_changed: bool = False
    transformation_successful: bool = False
    notes: str = ""


@dataclass
class AgentGrade:
    """Grade for an agent."""
    agent_name: str
    total_tests: int = 0
    passed_tests: int = 0
    score: float = 0.0
    weaknesses: list = field(default_factory=list)
    strengths: list = field(default_factory=list)


class ComprehensiveAgentTest:
    """Run comprehensive tests on all agents."""

    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.original_df = pd.read_csv(csv_path)
        self.current_df = self.original_df.copy()
        self.results: list[TestResult] = []

        # Initialize agents
        self.strategist = StrategistAgent()
        self.engineer = EngineerAgent()
        self.tester = TesterAgent()

        # Grades
        self.strategist_grade = AgentGrade("Strategist")
        self.engineer_grade = AgentGrade("Engineer")
        self.tester_grade = AgentGrade("Tester")

    def run_test(
        self,
        phase: str,
        test_name: str,
        user_request: str,
        expected_transformation_type: str,
        validation_func: callable = None,
    ) -> TestResult:
        """Run a single test through all agents."""

        result = TestResult(
            phase=phase,
            test_name=test_name,
            user_request=user_request,
            strategist_expected_type=expected_transformation_type,
        )

        print(f"\n{'='*60}")
        print(f"TEST: {test_name}")
        print(f"Request: {user_request}")
        print(f"Expected transformation: {expected_transformation_type}")
        print(f"{'='*60}")

        # Get profile for strategist
        profile = generate_profile(self.current_df)

        # Create a context for the strategist
        context = ConversationContext(
            session_id="test-session",
            current_node_id="test-node-1",
            parent_node_id="test-node-0",
            current_profile=profile,
            messages=[],
            recent_transformations=[],
            current_row_count=len(self.current_df),
            current_column_count=len(self.current_df.columns),
        )

        # =====================================================================
        # STRATEGIST TEST
        # =====================================================================
        print("\n--- STRATEGIST ---")
        try:
            plan = self.strategist.create_plan_with_context(
                context=context,
                user_message=user_request,
            )

            if plan:
                result.strategist_transformation_type = plan.transformation_type
                result.strategist_correct_type = (
                    plan.transformation_type == expected_transformation_type or
                    # Allow some flexibility for similar types
                    (expected_transformation_type == "change_case" and plan.transformation_type == "standardize") or
                    (expected_transformation_type == "standardize" and plan.transformation_type == "change_case")
                )
                result.strategist_has_acceptance_criteria = len(plan.acceptance_criteria) > 0
                result.strategist_acceptance_criteria_count = len(plan.acceptance_criteria)
                result.strategist_plan_json = plan.model_dump()

                print(f"  Transformation type: {plan.transformation_type}")
                print(f"  Correct type: {result.strategist_correct_type}")
                print(f"  Acceptance criteria: {len(plan.acceptance_criteria)}")
                print(f"  Explanation: {plan.explanation}")

                if plan.acceptance_criteria:
                    for ac in plan.acceptance_criteria:
                        print(f"    - {ac.type}: {ac.description}")
            else:
                result.strategist_error = "No plan returned"
                print(f"  ERROR: No plan returned")

        except Exception as e:
            result.strategist_error = str(e)
            print(f"  ERROR: {e}")
            plan = None

        # =====================================================================
        # ENGINEER TEST
        # =====================================================================
        print("\n--- ENGINEER ---")
        result_df = None

        if plan and not result.strategist_error:
            try:
                before_df = self.current_df.copy()
                result.engineer_rows_before = len(before_df)

                result_df, code = self.engineer.execute_on_dataframe(before_df, plan)

                result.engineer_executed = True
                result.engineer_code = code
                result.engineer_rows_after = len(result_df)

                # Check if data actually changed
                result.data_actually_changed = not before_df.equals(result_df)

                print(f"  Executed: YES")
                print(f"  Rows: {result.engineer_rows_before} → {result.engineer_rows_after}")
                print(f"  Data changed: {result.data_actually_changed}")
                print(f"  Code: {code[:100]}..." if len(code) > 100 else f"  Code: {code}")

            except Exception as e:
                result.engineer_error = str(e)
                result.engineer_executed = False
                print(f"  ERROR: {e}")
        else:
            print(f"  SKIPPED (no valid plan)")

        # =====================================================================
        # TESTER TEST
        # =====================================================================
        print("\n--- TESTER ---")

        if result.engineer_executed and result_df is not None:
            try:
                test_result = self.tester.validate(
                    before_df=self.current_df,
                    after_df=result_df,
                    plan=plan,
                )

                result.tester_passed = test_result.passed
                result.tester_issues = [i.message for i in test_result.issues]
                result.tester_acceptance_criteria_validated = "acceptance_criteria" in test_result.checks_run

                # Check if tester caught a real failure
                if not result.data_actually_changed and not test_result.passed:
                    result.tester_caught_failure = True

                print(f"  Passed: {test_result.passed}")
                print(f"  Checks run: {test_result.checks_run}")

                if test_result.issues:
                    print(f"  Issues:")
                    for issue in test_result.issues[:3]:  # Show first 3
                        print(f"    - {issue.message}")

            except Exception as e:
                print(f"  ERROR: {e}")
                result.tester_issues = [str(e)]
        else:
            print(f"  SKIPPED (no execution)")

        # =====================================================================
        # CUSTOM VALIDATION
        # =====================================================================
        if validation_func and result_df is not None:
            try:
                validation_passed, validation_msg = validation_func(self.current_df, result_df)
                result.transformation_successful = validation_passed
                result.notes = validation_msg
                print(f"\n--- VALIDATION ---")
                print(f"  Result: {'PASS' if validation_passed else 'FAIL'}")
                print(f"  Notes: {validation_msg}")
            except Exception as e:
                result.notes = f"Validation error: {e}"

        # Update current_df if transformation was successful
        if result.engineer_executed and result_df is not None and result.tester_passed:
            self.current_df = result_df
            print(f"\n  ✓ DataFrame updated for next test")

        self.results.append(result)
        return result

    def run_all_tests(self):
        """Run all test phases."""

        print("\n" + "="*80)
        print("COMPREHENSIVE AGENT TEST SUITE")
        print("="*80)
        print(f"Data: {self.csv_path}")
        print(f"Initial shape: {self.original_df.shape}")
        print(f"Columns: {list(self.original_df.columns)}")

        # =====================================================================
        # PHASE 1: Structural Integrity & Deduplication
        # =====================================================================
        print("\n\n" + "="*80)
        print("PHASE 1: STRUCTURAL INTEGRITY & DEDUPLICATION")
        print("="*80)

        # T2: Deduplication
        self.run_test(
            phase="Phase 1",
            test_name="T2: Deduplication",
            user_request="Remove duplicate rows. I see lead_id 1003 appears twice.",
            expected_transformation_type="deduplicate",
            validation_func=lambda before, after: (
                after['lead_id'].nunique() == len(after),
                f"Unique lead_ids: {after['lead_id'].nunique()}, Total rows: {len(after)}"
            )
        )

        # =====================================================================
        # PHASE 2: Format Standardization
        # =====================================================================
        print("\n\n" + "="*80)
        print("PHASE 2: FORMAT STANDARDIZATION")
        print("="*80)

        # T3: Name Normalization
        self.run_test(
            phase="Phase 2",
            test_name="T3: Name Normalization",
            user_request="Convert first_name and last_name to Title Case (e.g., MICHAEL → Michael)",
            expected_transformation_type="change_case",
            validation_func=lambda before, after: (
                all(after['first_name'].dropna().str.istitle()) and
                all(after['last_name'].dropna().str.istitle()),
                f"Sample: {after['first_name'].iloc[0]}, {after['last_name'].iloc[0]}"
            )
        )

        # T4: Email Cleaning
        self.run_test(
            phase="Phase 2",
            test_name="T4: Email Cleaning",
            user_request="Convert all email addresses to lowercase",
            expected_transformation_type="change_case",
            validation_func=lambda before, after: (
                all(after['email'].dropna().str.islower()),
                f"Sample: {after['email'].iloc[0]}"
            )
        )

        # T5: Phone Number Uniformity
        self.run_test(
            phase="Phase 2",
            test_name="T5: Phone Number Uniformity",
            user_request="Standardize phone numbers to the format nnn-nnn-nnnn",
            expected_transformation_type="format_phone",
            validation_func=lambda before, after: (
                after['phone_number'].dropna().str.match(r'^\d{3}-\d{3}-\d{4}$').sum() /
                len(after['phone_number'].dropna()) > 0.8,
                f"Sample: {after['phone_number'].iloc[0]}"
            )
        )

        # T6a: Campaign Source Standardization
        self.run_test(
            phase="Phase 2",
            test_name="T6a: Campaign Source Standardization",
            user_request="Standardize campaign_source values: google ads → Google Ads, linkedin → LinkedIn",
            expected_transformation_type="replace_values",
            validation_func=lambda before, after: (
                set(after['campaign_source'].dropna().unique()) <= {'Google Ads', 'LinkedIn'},
                f"Unique values: {after['campaign_source'].unique()}"
            )
        )

        # T6b: Status Standardization
        self.run_test(
            phase="Phase 2",
            test_name="T6b: Status Standardization",
            user_request="Standardize status values: fix typo 'Qualifed' → 'Qualified', make all Title Case (NEW → New, QUALIFIED → Qualified, contacted → Contacted)",
            expected_transformation_type="replace_values",
            validation_func=lambda before, after: (
                set(after['status'].dropna().unique()) <= {'New', 'Qualified', 'Contacted'},
                f"Unique values: {after['status'].unique()}"
            )
        )

        # =====================================================================
        # PHASE 3: Data Type Conversion
        # =====================================================================
        print("\n\n" + "="*80)
        print("PHASE 3: DATA TYPE CONVERSION")
        print("="*80)

        # T7: Lead Score Conversion
        self.run_test(
            phase="Phase 3",
            test_name="T7: Lead Score Conversion",
            user_request="Convert text-based numbers in lead_score to integers: 'seventy-two' → 72, 'fifty-five' → 55",
            expected_transformation_type="replace_values",
            validation_func=lambda before, after: (
                pd.to_numeric(after['lead_score'], errors='coerce').notna().sum() >
                pd.to_numeric(before['lead_score'], errors='coerce').notna().sum(),
                f"Numeric values: {pd.to_numeric(after['lead_score'], errors='coerce').dropna().tolist()[:5]}"
            )
        )

        # T8: Date Uniformity
        self.run_test(
            phase="Phase 3",
            test_name="T8: Date Uniformity",
            user_request="Parse all dates in created_date to ISO format YYYY-MM-DD",
            expected_transformation_type="parse_date",
            validation_func=lambda before, after: (
                after['created_date'].dropna().str.match(r'^\d{4}-\d{2}-\d{2}').sum() /
                len(after['created_date'].dropna()) > 0.8,
                f"Sample: {after['created_date'].iloc[0]}"
            )
        )

        # =====================================================================
        # PHASE 4: Handling Missing Values
        # =====================================================================
        print("\n\n" + "="*80)
        print("PHASE 4: HANDLING MISSING VALUES")
        print("="*80)

        # T9a: Lead Score Null Handling
        self.run_test(
            phase="Phase 4",
            test_name="T9a: Lead Score Null Handling",
            user_request="Fill missing lead_score values with 0",
            expected_transformation_type="fill_nulls",
            validation_func=lambda before, after: (
                after['lead_score'].isna().sum() < before['lead_score'].isna().sum(),
                f"Nulls before: {before['lead_score'].isna().sum()}, after: {after['lead_score'].isna().sum()}"
            )
        )

        # T9b: Status Null Handling
        self.run_test(
            phase="Phase 4",
            test_name="T9b: Status Null Handling",
            user_request="Fill missing status values with 'New'",
            expected_transformation_type="fill_nulls",
            validation_func=lambda before, after: (
                after['status'].isna().sum() < before['status'].isna().sum() or
                (after['status'].fillna('') == '').sum() < (before['status'].fillna('') == '').sum(),
                f"Nulls/empty before: {before['status'].isna().sum()}, after: {after['status'].isna().sum()}"
            )
        )

        print("\n\n" + "="*80)
        print("TEST SUITE COMPLETE")
        print("="*80)

    def calculate_grades(self):
        """Calculate grades for each agent."""

        for result in self.results:
            # Strategist grading
            self.strategist_grade.total_tests += 1
            if result.strategist_correct_type and result.strategist_has_acceptance_criteria:
                self.strategist_grade.passed_tests += 1
            elif result.strategist_correct_type and not result.strategist_has_acceptance_criteria:
                self.strategist_grade.passed_tests += 0.5  # Partial credit

            # Engineer grading
            self.engineer_grade.total_tests += 1
            if result.engineer_executed and not result.engineer_error:
                if result.data_actually_changed or result.transformation_successful:
                    self.engineer_grade.passed_tests += 1
                else:
                    self.engineer_grade.passed_tests += 0.5  # Executed but no change

            # Tester grading
            self.tester_grade.total_tests += 1
            if result.tester_acceptance_criteria_validated:
                self.tester_grade.passed_tests += 1
            elif result.tester_passed and result.transformation_successful:
                self.tester_grade.passed_tests += 0.5  # Passed but didn't validate criteria

        # Calculate scores
        for grade in [self.strategist_grade, self.engineer_grade, self.tester_grade]:
            if grade.total_tests > 0:
                grade.score = (grade.passed_tests / grade.total_tests) * 100

        # Identify weaknesses
        self._identify_weaknesses()

    def _identify_weaknesses(self):
        """Identify weaknesses for each agent."""

        # Strategist weaknesses
        no_acceptance_criteria = sum(1 for r in self.results if not r.strategist_has_acceptance_criteria)
        wrong_type = sum(1 for r in self.results if not r.strategist_correct_type)

        if no_acceptance_criteria > 0:
            self.strategist_grade.weaknesses.append(
                f"Missing acceptance criteria in {no_acceptance_criteria}/{len(self.results)} tests"
            )
        if wrong_type > 0:
            self.strategist_grade.weaknesses.append(
                f"Wrong transformation type in {wrong_type}/{len(self.results)} tests"
            )

        # Check for specific issues
        for r in self.results:
            if r.strategist_error:
                self.strategist_grade.weaknesses.append(f"{r.test_name}: {r.strategist_error}")

        # Engineer weaknesses
        execution_failures = sum(1 for r in self.results if not r.engineer_executed)
        no_change = sum(1 for r in self.results if r.engineer_executed and not r.data_actually_changed)

        if execution_failures > 0:
            self.engineer_grade.weaknesses.append(
                f"Execution failed in {execution_failures}/{len(self.results)} tests"
            )
        if no_change > 0:
            self.engineer_grade.weaknesses.append(
                f"No data change in {no_change}/{len(self.results)} tests"
            )

        for r in self.results:
            if r.engineer_error:
                self.engineer_grade.weaknesses.append(f"{r.test_name}: {r.engineer_error}")

        # Tester weaknesses
        no_criteria_validation = sum(1 for r in self.results if not r.tester_acceptance_criteria_validated)
        missed_failures = sum(1 for r in self.results
                            if not r.transformation_successful and r.tester_passed)

        if no_criteria_validation > 0:
            self.tester_grade.weaknesses.append(
                f"Didn't validate acceptance criteria in {no_criteria_validation}/{len(self.results)} tests"
            )
        if missed_failures > 0:
            self.tester_grade.weaknesses.append(
                f"Missed {missed_failures} transformation failures"
            )

    def print_report(self):
        """Print final report."""

        print("\n\n" + "="*80)
        print("AGENT PERFORMANCE REPORT")
        print("="*80)

        # Test Summary
        print("\n--- TEST SUMMARY ---")
        for r in self.results:
            status = "✓" if r.transformation_successful else "✗"
            print(f"  {status} {r.test_name}")
            print(f"      Strategist: type={r.strategist_transformation_type}, criteria={r.strategist_acceptance_criteria_count}")
            print(f"      Engineer: executed={r.engineer_executed}, changed={r.data_actually_changed}")
            print(f"      Tester: passed={r.tester_passed}, criteria_validated={r.tester_acceptance_criteria_validated}")
            if r.notes:
                print(f"      Notes: {r.notes}")

        # Agent Grades
        print("\n\n" + "="*80)
        print("AGENT GRADES")
        print("="*80)

        for grade in [self.strategist_grade, self.engineer_grade, self.tester_grade]:
            print(f"\n--- {grade.agent_name.upper()} ---")
            print(f"  Score: {grade.score:.1f}% ({grade.passed_tests}/{grade.total_tests})")

            if grade.weaknesses:
                print(f"  Weaknesses:")
                for w in grade.weaknesses[:5]:  # Show top 5
                    print(f"    - {w}")

            if grade.strengths:
                print(f"  Strengths:")
                for s in grade.strengths:
                    print(f"    - {s}")

        # Final Data State
        print("\n\n" + "="*80)
        print("FINAL DATA STATE")
        print("="*80)
        print(f"Shape: {self.current_df.shape}")
        print("\nSample (first 3 rows):")
        print(self.current_df.head(3).to_string())

        return {
            "results": self.results,
            "strategist_grade": self.strategist_grade,
            "engineer_grade": self.engineer_grade,
            "tester_grade": self.tester_grade,
        }


if __name__ == "__main__":
    # Run the test suite
    csv_path = Path(__file__).parent / "messy_marketing_leads.csv"

    tester = ComprehensiveAgentTest(str(csv_path))
    tester.run_all_tests()
    tester.calculate_grades()
    tester.print_report()
