# =============================================================================
# agents/models/test_result.py - Test Result Schema
# =============================================================================
# This module defines the TestResult schema - the output from the Tester agent
# after validating a transformation.
#
# The result contains:
# - Pass/fail status with severity level
# - Before/after statistics
# - List of quality checks run
# - Detailed issues found
# - Human-readable summary and suggestions
#
# Example:
#   result = tester.validate(before_df, after_df, plan)
#   if not result.passed:
#       for issue in result.issues:
#           print(f"⚠️ {issue.message}")
# =============================================================================

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class Severity(str, Enum):
    """Severity level for issues and overall result."""
    SUCCESS = "success"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class QualityIssue(BaseModel):
    """
    A specific data quality issue found during validation.

    Represents a single problem or concern detected by a quality check.
    Issues can range from informational to errors that should block the operation.
    """

    check_name: str = Field(
        ...,
        description="Name of the check that found this issue"
    )

    severity: Severity = Field(
        ...,
        description="How serious is this issue"
    )

    column: str | None = Field(
        default=None,
        description="Column affected, if applicable"
    )

    message: str = Field(
        ...,
        description="Human-readable description of the issue"
    )

    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional context for debugging"
    )

    suggestion: str | None = Field(
        default=None,
        description="Suggested action to fix this issue"
    )


class CheckResult(BaseModel):
    """Result from a single quality check."""

    check_name: str = Field(
        ...,
        description="Name of the check"
    )

    passed: bool = Field(
        ...,
        description="Whether this check passed"
    )

    issues: list[QualityIssue] = Field(
        default_factory=list,
        description="Issues found by this check"
    )

    execution_time_ms: float = Field(
        default=0.0,
        description="Time taken to run this check"
    )


class TestResult(BaseModel):
    """
    Result from Tester Agent validation.

    Returned by TesterAgent.validate() after checking a transformation result.
    Contains overall status, statistics, and detailed quality issues.
    """

    # -------------------------------------------------------------------------
    # Overall Status
    # -------------------------------------------------------------------------

    passed: bool = Field(
        ...,
        description="Whether all critical checks passed"
    )

    severity: Severity = Field(
        ...,
        description="Overall severity: success, warning, or error"
    )

    # -------------------------------------------------------------------------
    # Transformation Info
    # -------------------------------------------------------------------------

    transformation_type: str = Field(
        default="",
        description="Type of transformation that was validated"
    )

    # -------------------------------------------------------------------------
    # Before/After Statistics
    # -------------------------------------------------------------------------

    rows_before: int = Field(
        default=0,
        ge=0,
        description="Row count before transformation"
    )

    rows_after: int = Field(
        default=0,
        ge=0,
        description="Row count after transformation"
    )

    cols_before: int = Field(
        default=0,
        ge=0,
        description="Column count before transformation"
    )

    cols_after: int = Field(
        default=0,
        ge=0,
        description="Column count after transformation"
    )

    rows_changed: int = Field(
        default=0,
        description="Number of rows added or removed (can be negative)"
    )

    row_change_percent: float = Field(
        default=0.0,
        description="Percentage of rows changed"
    )

    # -------------------------------------------------------------------------
    # Check Results
    # -------------------------------------------------------------------------

    checks_run: list[str] = Field(
        default_factory=list,
        description="Names of all checks that were run"
    )

    checks_passed: list[str] = Field(
        default_factory=list,
        description="Names of checks that passed"
    )

    checks_failed: list[str] = Field(
        default_factory=list,
        description="Names of checks that failed"
    )

    check_results: list[CheckResult] = Field(
        default_factory=list,
        description="Detailed results from each check"
    )

    # -------------------------------------------------------------------------
    # Issues
    # -------------------------------------------------------------------------

    issues: list[QualityIssue] = Field(
        default_factory=list,
        description="All quality issues found"
    )

    warnings: list[str] = Field(
        default_factory=list,
        description="Warning messages (convenience accessor)"
    )

    errors: list[str] = Field(
        default_factory=list,
        description="Error messages (convenience accessor)"
    )

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------

    summary: str = Field(
        default="",
        description="Human-readable summary of validation result"
    )

    suggestion: str | None = Field(
        default=None,
        description="Suggested next action if issues found"
    )

    # -------------------------------------------------------------------------
    # Timing
    # -------------------------------------------------------------------------

    validated_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When validation was performed"
    )

    validation_time_ms: float = Field(
        default=0.0,
        ge=0.0,
        description="Total validation time in milliseconds"
    )

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def has_errors(self) -> bool:
        """Check if any error-level issues exist."""
        return any(issue.severity == Severity.ERROR for issue in self.issues)

    def has_warnings(self) -> bool:
        """Check if any warning-level issues exist."""
        return any(issue.severity == Severity.WARNING for issue in self.issues)

    def get_issues_by_severity(self, severity: Severity) -> list[QualityIssue]:
        """Get all issues of a specific severity."""
        return [i for i in self.issues if i.severity == severity]

    def get_issues_by_column(self, column: str) -> list[QualityIssue]:
        """Get all issues for a specific column."""
        return [i for i in self.issues if i.column == column]

    def format_for_display(self) -> str:
        """Format the result for terminal display."""
        if self.passed and not self.has_warnings():
            icon = "✅"
            status = "Passed"
        elif self.passed:
            icon = "⚠️"
            status = "Passed with warnings"
        else:
            icon = "❌"
            status = "Failed"

        lines = [f"Quality Check: {icon} {status}"]

        # Row change info
        if self.rows_changed != 0:
            direction = "Removed" if self.rows_changed < 0 else "Added"
            lines.append(f"  - {direction} {abs(self.rows_changed)} rows ({abs(self.row_change_percent):.1f}%)")

        # Issues
        for issue in self.issues:
            icon = "⚠️" if issue.severity == Severity.WARNING else "❌" if issue.severity == Severity.ERROR else "ℹ️"
            lines.append(f"  - {icon} {issue.message}")

        # Suggestion
        if self.suggestion:
            lines.append(f"  → {self.suggestion}")

        return "\n".join(lines)

    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
