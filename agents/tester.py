# =============================================================================
# agents/tester.py - Tester Agent (Agent C)
# =============================================================================
# This module implements the Tester Agent, the third agent in the 3-agent
# data transformation pipeline.
#
# The Tester's job:
# 1. Validate transformation results
# 2. Check data quality (nulls, duplicates, bounds)
# 3. Detect potential issues before they propagate
# 4. Generate human-readable quality reports
#
# Pipeline: User → Strategist (Plan) → Engineer (Execute) → Tester (Validate)
#
# Usage:
#   from agents.tester import TesterAgent
#   tester = TesterAgent()
#   result = tester.validate(before_df, after_df, plan)
#   if not result.passed:
#       print(result.format_for_display())
# =============================================================================

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd

from agents.models.technical_plan import TechnicalPlan, TransformationType, AcceptanceCriterion
from agents.models.test_result import (
    TestResult,
    CheckResult,
    QualityIssue,
    Severity,
)
from agents.models.execution_result import ExecutionResult
from agents.quality_checks import get_checks_for_type, get_all_checks

# Set up logging
logger = logging.getLogger(__name__)


# =============================================================================
# Exceptions
# =============================================================================

class TesterError(Exception):
    """Error during validation."""

    def __init__(
        self,
        message: str,
        code: str = "TESTER_ERROR",
        details: dict[str, Any] | None = None
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"


# =============================================================================
# Tester Agent
# =============================================================================

class TesterAgent:
    """
    Agent C: The Tester.

    Validates transformation results and checks data quality.
    Runs relevant checks based on transformation type and reports issues.

    Example:
        tester = TesterAgent()

        # Validate a transformation
        result = tester.validate(
            before_df=original_data,
            after_df=transformed_data,
            plan=technical_plan
        )

        if result.passed:
            print("✅ All checks passed")
        else:
            print(result.format_for_display())
    """

    def __init__(self, strict_mode: bool = False):
        """
        Initialize the Tester Agent.

        Args:
            strict_mode: If True, warnings are treated as errors
        """
        self.strict_mode = strict_mode
        logger.info(f"TesterAgent initialized (strict_mode={strict_mode})")

    # -------------------------------------------------------------------------
    # Main Entry Point
    # -------------------------------------------------------------------------

    def validate(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        plan: TechnicalPlan,
        execution_result: ExecutionResult | None = None,
    ) -> TestResult:
        """
        Validate a transformation result.

        Runs all relevant quality checks for the transformation type
        and produces a comprehensive TestResult.

        Args:
            before_df: DataFrame before transformation
            after_df: DataFrame after transformation
            plan: The TechnicalPlan that was executed
            execution_result: Optional ExecutionResult from Engineer

        Returns:
            TestResult with pass/fail status and detailed issues

        Example:
            result = tester.validate(df_before, df_after, plan)
            if not result.passed:
                for issue in result.issues:
                    print(f"⚠️ {issue.message}")
        """
        start_time = time.time()

        # Get transformation type
        trans_type = plan.transformation_type
        if isinstance(trans_type, TransformationType):
            trans_type_str = trans_type.value
        else:
            trans_type_str = str(trans_type)

        logger.info(f"Validating transformation: {trans_type_str}")

        # Get relevant checks
        checks = get_checks_for_type(trans_type_str)
        logger.debug(f"Running {len(checks)} checks")

        # Run all checks
        all_issues: list[QualityIssue] = []
        check_results: list[CheckResult] = []
        checks_passed: list[str] = []
        checks_failed: list[str] = []

        for check_name, check_func in checks:
            check_start = time.time()
            try:
                issues = check_func(before_df, after_df, plan)
                check_time = (time.time() - check_start) * 1000

                # Determine if check passed
                has_errors = any(i.severity == Severity.ERROR for i in issues)
                has_warnings = any(i.severity == Severity.WARNING for i in issues)

                passed = not has_errors
                if self.strict_mode:
                    passed = not has_errors and not has_warnings

                if passed:
                    checks_passed.append(check_name)
                else:
                    checks_failed.append(check_name)

                check_results.append(CheckResult(
                    check_name=check_name,
                    passed=passed,
                    issues=issues,
                    execution_time_ms=check_time
                ))

                all_issues.extend(issues)

            except Exception as e:
                logger.error(f"Check '{check_name}' failed with error: {e}")
                check_results.append(CheckResult(
                    check_name=check_name,
                    passed=False,
                    issues=[QualityIssue(
                        check_name=check_name,
                        severity=Severity.ERROR,
                        message=f"Check failed with error: {str(e)}",
                        details={"error": str(e)}
                    )],
                    execution_time_ms=(time.time() - check_start) * 1000
                ))
                checks_failed.append(check_name)

        # Validate acceptance criteria from the Strategist
        if plan.acceptance_criteria:
            logger.info(f"Validating {len(plan.acceptance_criteria)} acceptance criteria")
            acceptance_issues = self._validate_acceptance_criteria(before_df, after_df, plan)

            if acceptance_issues:
                check_results.append(CheckResult(
                    check_name="acceptance_criteria",
                    passed=not any(i.severity == Severity.ERROR for i in acceptance_issues),
                    issues=acceptance_issues,
                    execution_time_ms=0
                ))

                # Add to overall issues
                all_issues.extend(acceptance_issues)

                # Track pass/fail
                if any(i.severity == Severity.ERROR for i in acceptance_issues):
                    checks_failed.append("acceptance_criteria")
                else:
                    checks_passed.append("acceptance_criteria")
            else:
                checks_passed.append("acceptance_criteria")

        # Calculate statistics
        rows_before = len(before_df)
        rows_after = len(after_df)
        rows_changed = rows_after - rows_before
        row_change_percent = (abs(rows_changed) / rows_before * 100) if rows_before > 0 else 0

        # Determine overall status
        has_errors = any(i.severity == Severity.ERROR for i in all_issues)
        has_warnings = any(i.severity == Severity.WARNING for i in all_issues)

        if has_errors:
            passed = False
            severity = Severity.ERROR
        elif has_warnings:
            passed = not self.strict_mode
            severity = Severity.WARNING
        else:
            passed = True
            severity = Severity.SUCCESS

        # Generate summary and suggestion
        summary = self._generate_summary(
            passed, trans_type_str, rows_changed, row_change_percent, all_issues
        )
        suggestion = self._generate_suggestion(all_issues)

        # Extract warnings and errors for convenience
        warnings = [i.message for i in all_issues if i.severity == Severity.WARNING]
        errors = [i.message for i in all_issues if i.severity == Severity.ERROR]

        validation_time = (time.time() - start_time) * 1000

        return TestResult(
            passed=passed,
            severity=severity,
            transformation_type=trans_type_str,
            rows_before=rows_before,
            rows_after=rows_after,
            cols_before=len(before_df.columns),
            cols_after=len(after_df.columns),
            rows_changed=rows_changed,
            row_change_percent=row_change_percent,
            checks_run=[cr.check_name for cr in check_results],
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            check_results=check_results,
            issues=all_issues,
            warnings=warnings,
            errors=errors,
            summary=summary,
            suggestion=suggestion,
            validation_time_ms=validation_time
        )

    # -------------------------------------------------------------------------
    # Quick Validation
    # -------------------------------------------------------------------------

    def quick_validate(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        plan: TechnicalPlan
    ) -> tuple[bool, str]:
        """
        Quick validation returning just pass/fail and summary.

        Useful for inline checks during chat interaction.

        Args:
            before_df: DataFrame before transformation
            after_df: DataFrame after transformation
            plan: The TechnicalPlan that was executed

        Returns:
            Tuple of (passed, summary_message)
        """
        result = self.validate(before_df, after_df, plan)
        return result.passed, result.summary

    # -------------------------------------------------------------------------
    # Batch Validation
    # -------------------------------------------------------------------------

    def validate_batch(
        self,
        transformations: list[tuple[pd.DataFrame, pd.DataFrame, TechnicalPlan]]
    ) -> list[TestResult]:
        """
        Validate multiple transformations.

        Args:
            transformations: List of (before_df, after_df, plan) tuples

        Returns:
            List of TestResult for each transformation
        """
        return [
            self.validate(before_df, after_df, plan)
            for before_df, after_df, plan in transformations
        ]

    # -------------------------------------------------------------------------
    # Acceptance Criteria Validation
    # -------------------------------------------------------------------------

    def _validate_acceptance_criteria(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        plan: TechnicalPlan,
    ) -> list[QualityIssue]:
        """
        Validate acceptance criteria defined by the Strategist.

        This is the key integration between Strategist intent and Tester validation.
        Each criterion describes what the transformation should achieve.
        """
        import re
        issues = []

        for criterion in plan.acceptance_criteria:
            try:
                criterion_issues = self._validate_single_criterion(
                    before_df, after_df, criterion
                )
                issues.extend(criterion_issues)
            except Exception as e:
                logger.error(f"Failed to validate criterion {criterion.type}: {e}")
                issues.append(QualityIssue(
                    check_name="acceptance_criteria",
                    severity=Severity.WARNING,
                    message=f"Could not validate criterion: {criterion.description}",
                    details={"error": str(e), "criterion_type": criterion.type}
                ))

        return issues

    def _validate_single_criterion(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        criterion: AcceptanceCriterion,
    ) -> list[QualityIssue]:
        """Validate a single acceptance criterion."""
        import re
        issues = []

        if criterion.type == "column_format":
            issues.extend(self._check_column_format(after_df, criterion))

        elif criterion.type == "value_changed":
            issues.extend(self._check_value_changed(before_df, after_df, criterion))

        elif criterion.type == "row_count_change":
            issues.extend(self._check_row_count_change(before_df, after_df, criterion))

        elif criterion.type == "column_exists":
            issues.extend(self._check_column_exists(after_df, criterion))

        elif criterion.type == "no_nulls":
            issues.extend(self._check_no_nulls(after_df, criterion))

        elif criterion.type == "unique_values":
            issues.extend(self._check_unique_values(after_df, criterion))

        return issues

    def _check_column_format(
        self,
        after_df: pd.DataFrame,
        criterion: AcceptanceCriterion,
    ) -> list[QualityIssue]:
        """Check if column values match expected regex pattern."""
        import re
        issues = []
        col = criterion.column

        if not col or col not in after_df.columns:
            return issues

        pattern = criterion.pattern
        if not pattern:
            return issues

        col_data = after_df[col].dropna().astype(str)
        if len(col_data) == 0:
            return issues

        matches = col_data.str.match(pattern, na=False)
        match_rate = matches.sum() / len(col_data)

        if match_rate < criterion.min_match_rate:
            sample_non_matching = col_data[~matches].head(3).tolist()
            issues.append(QualityIssue(
                check_name="acceptance_criteria",
                severity=Severity.ERROR,
                column=col,
                message=f"ACCEPTANCE CRITERION FAILED: {criterion.description}. Only {match_rate:.0%} of values match expected format (need {criterion.min_match_rate:.0%})",
                details={
                    "criterion_type": "column_format",
                    "expected_pattern": pattern,
                    "match_rate": float(match_rate),
                    "required_rate": criterion.min_match_rate,
                    "sample_non_matching": sample_non_matching
                },
                suggestion="The transformation may not have achieved the intended result. Consider retrying with different parameters."
            ))

        return issues

    def _check_value_changed(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        criterion: AcceptanceCriterion,
    ) -> list[QualityIssue]:
        """Check if column values actually changed."""
        issues = []
        col = criterion.column

        if not col:
            return issues

        if col not in before_df.columns or col not in after_df.columns:
            return issues

        # Compare values (handling potential length differences)
        try:
            before_vals = before_df[col].astype(str).tolist()
            after_vals = after_df[col].astype(str).tolist()

            # Count changes
            min_len = min(len(before_vals), len(after_vals))
            changes = sum(1 for i in range(min_len) if before_vals[i] != after_vals[i])

            if changes == 0:
                issues.append(QualityIssue(
                    check_name="acceptance_criteria",
                    severity=Severity.ERROR,
                    column=col,
                    message=f"ACCEPTANCE CRITERION FAILED: {criterion.description}. No values in '{col}' were changed by the transformation.",
                    details={
                        "criterion_type": "value_changed",
                        "changes_made": 0
                    },
                    suggestion="The transformation did not modify any values. Check if the transformation type is correct."
                ))
        except Exception:
            pass

        return issues

    def _check_row_count_change(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        criterion: AcceptanceCriterion,
    ) -> list[QualityIssue]:
        """Check if row count changed as expected."""
        issues = []
        expected = criterion.expected_change

        if not expected:
            return issues

        before_count = len(before_df)
        after_count = len(after_df)
        change = after_count - before_count
        change_pct = (change / before_count * 100) if before_count > 0 else 0

        failed = False
        if expected == "increase" and change <= 0:
            failed = True
        elif expected == "decrease" and change >= 0:
            failed = True
        elif expected == "same" and change != 0:
            failed = True

        if failed:
            issues.append(QualityIssue(
                check_name="acceptance_criteria",
                severity=Severity.ERROR,
                message=f"ACCEPTANCE CRITERION FAILED: {criterion.description}. Expected row count to {expected}, but changed by {change:+d} ({change_pct:+.1f}%)",
                details={
                    "criterion_type": "row_count_change",
                    "expected_change": expected,
                    "actual_change": change,
                    "before_count": before_count,
                    "after_count": after_count
                }
            ))

        return issues

    def _check_column_exists(
        self,
        after_df: pd.DataFrame,
        criterion: AcceptanceCriterion,
    ) -> list[QualityIssue]:
        """Check if column exists (or doesn't exist)."""
        issues = []
        col = criterion.column

        if not col:
            return issues

        exists = col in after_df.columns

        if criterion.should_exist and not exists:
            issues.append(QualityIssue(
                check_name="acceptance_criteria",
                severity=Severity.ERROR,
                column=col,
                message=f"ACCEPTANCE CRITERION FAILED: {criterion.description}. Column '{col}' should exist but was not found.",
                details={"criterion_type": "column_exists", "should_exist": True}
            ))
        elif not criterion.should_exist and exists:
            issues.append(QualityIssue(
                check_name="acceptance_criteria",
                severity=Severity.ERROR,
                column=col,
                message=f"ACCEPTANCE CRITERION FAILED: {criterion.description}. Column '{col}' should not exist but was found.",
                details={"criterion_type": "column_exists", "should_exist": False}
            ))

        return issues

    def _check_no_nulls(
        self,
        after_df: pd.DataFrame,
        criterion: AcceptanceCriterion,
    ) -> list[QualityIssue]:
        """Check if column has no null values."""
        issues = []
        col = criterion.column

        if not col or col not in after_df.columns:
            return issues

        null_count = after_df[col].isna().sum()

        if null_count > 0:
            issues.append(QualityIssue(
                check_name="acceptance_criteria",
                severity=Severity.ERROR,
                column=col,
                message=f"ACCEPTANCE CRITERION FAILED: {criterion.description}. Column '{col}' still has {null_count} null values.",
                details={
                    "criterion_type": "no_nulls",
                    "null_count": int(null_count)
                }
            ))

        return issues

    def _check_unique_values(
        self,
        after_df: pd.DataFrame,
        criterion: AcceptanceCriterion,
    ) -> list[QualityIssue]:
        """Check if column has unique values (no duplicates)."""
        issues = []
        col = criterion.column

        if not col or col not in after_df.columns:
            return issues

        total = len(after_df[col].dropna())
        unique = after_df[col].dropna().nunique()
        duplicates = total - unique

        if duplicates > 0:
            issues.append(QualityIssue(
                check_name="acceptance_criteria",
                severity=Severity.ERROR,
                column=col,
                message=f"ACCEPTANCE CRITERION FAILED: {criterion.description}. Column '{col}' has {duplicates} duplicate values.",
                details={
                    "criterion_type": "unique_values",
                    "duplicate_count": duplicates
                }
            ))

        return issues

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _generate_summary(
        self,
        passed: bool,
        trans_type: str,
        rows_changed: int,
        row_change_pct: float,
        issues: list[QualityIssue]
    ) -> str:
        """Generate a human-readable summary."""
        if passed and not issues:
            if rows_changed == 0:
                return "All checks passed. No unexpected changes detected."
            elif rows_changed < 0:
                return f"All checks passed. Removed {abs(rows_changed)} rows ({row_change_pct:.1f}%)."
            else:
                return f"All checks passed. Added {rows_changed} rows."

        if passed and issues:
            warning_count = sum(1 for i in issues if i.severity == Severity.WARNING)
            return f"Passed with {warning_count} warning(s). Review recommended."

        error_count = sum(1 for i in issues if i.severity == Severity.ERROR)
        return f"Validation failed with {error_count} error(s)."

    def _generate_suggestion(self, issues: list[QualityIssue]) -> str | None:
        """Generate the most relevant suggestion from issues."""
        # Prioritize error suggestions over warnings
        for severity in [Severity.ERROR, Severity.WARNING]:
            for issue in issues:
                if issue.severity == severity and issue.suggestion:
                    return issue.suggestion
        return None


# =============================================================================
# Convenience Functions
# =============================================================================

def validate_transformation(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    plan: TechnicalPlan
) -> TestResult:
    """
    Convenience function to validate a transformation.

    Args:
        before_df: DataFrame before transformation
        after_df: DataFrame after transformation
        plan: The TechnicalPlan that was executed

    Returns:
        TestResult
    """
    tester = TesterAgent()
    return tester.validate(before_df, after_df, plan)
