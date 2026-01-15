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

from agents.models.technical_plan import TechnicalPlan, TransformationType
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
