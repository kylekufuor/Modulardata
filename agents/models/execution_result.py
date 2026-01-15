# =============================================================================
# agents/models/execution_result.py - Execution Result Schema
# =============================================================================
# This module defines the ExecutionResult schema - the output from the
# Engineer agent after executing a transformation.
#
# The result contains:
# - Success/failure status
# - New node information (ID, parent, etc.)
# - Data statistics (row/column counts, affected rows)
# - Generated pandas code for transparency
# - Error details if execution failed
#
# Example:
#   result = engineer.execute_plan(session_id, plan)
#   if result.success:
#       print(f"Created node {result.new_node_id}")
#       print(f"Affected {result.rows_affected} rows")
#       print(f"Code: {result.transformation_code}")
# =============================================================================

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class ExecutionResult(BaseModel):
    """
    Result of executing a TechnicalPlan.

    Returned by EngineerAgent.execute_plan() and execute_batch().
    Contains all information needed to update the UI and track history.
    """

    # -------------------------------------------------------------------------
    # Status
    # -------------------------------------------------------------------------

    success: bool = Field(
        ...,
        description="Whether the transformation executed successfully"
    )

    error_message: str | None = Field(
        default=None,
        description="Error message if success=False"
    )

    error_type: str | None = Field(
        default=None,
        description="Error type/category (e.g., 'ValidationError', 'ColumnNotFound')"
    )

    # -------------------------------------------------------------------------
    # Session & Node Info
    # -------------------------------------------------------------------------

    session_id: str = Field(
        ...,
        description="Session ID for this execution"
    )

    new_node_id: str | None = Field(
        default=None,
        description="ID of the newly created node (if successful)"
    )

    parent_node_id: str | None = Field(
        default=None,
        description="ID of the parent node this was derived from"
    )

    # -------------------------------------------------------------------------
    # Data Statistics
    # -------------------------------------------------------------------------

    row_count: int = Field(
        default=0,
        ge=0,
        description="Number of rows in the resulting DataFrame"
    )

    column_count: int = Field(
        default=0,
        ge=0,
        description="Number of columns in the resulting DataFrame"
    )

    rows_affected: int = Field(
        default=0,
        ge=0,
        description="Number of rows changed/removed/added"
    )

    columns_affected: list[str] = Field(
        default_factory=list,
        description="List of columns that were modified"
    )

    # -------------------------------------------------------------------------
    # Transformation Details
    # -------------------------------------------------------------------------

    transformation_type: str = Field(
        default="",
        description="Type of transformation executed"
    )

    transformation: str = Field(
        default="",
        description="Human-readable description of the transformation"
    )

    transformation_code: str = Field(
        default="",
        description="Generated pandas code for transparency"
    )

    # -------------------------------------------------------------------------
    # Timing
    # -------------------------------------------------------------------------

    executed_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="Timestamp when execution completed"
    )

    execution_time_ms: float = Field(
        default=0.0,
        ge=0.0,
        description="Execution time in milliseconds"
    )

    # -------------------------------------------------------------------------
    # Preview Data
    # -------------------------------------------------------------------------

    preview_data: list[dict[str, Any]] | None = Field(
        default=None,
        description="Optional preview of the transformed data (first N rows as dicts)"
    )

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def is_successful(self) -> bool:
        """Check if execution was successful."""
        return self.success and self.new_node_id is not None

    def get_summary(self) -> str:
        """Get a human-readable summary of the result."""
        if not self.success:
            return f"Failed: {self.error_message}"

        return (
            f"Created node {self.new_node_id}: "
            f"{self.row_count} rows, {self.column_count} columns "
            f"({self.rows_affected} rows affected)"
        )

    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class BatchExecutionResult(BaseModel):
    """
    Result of executing multiple TechnicalPlans as a batch.

    Used when applying a queue of transformations to create a single module.
    """

    success: bool = Field(
        ...,
        description="Whether all transformations executed successfully"
    )

    results: list[ExecutionResult] = Field(
        default_factory=list,
        description="Individual results for each transformation"
    )

    final_node_id: str | None = Field(
        default=None,
        description="ID of the final node after all transformations"
    )

    total_transformations: int = Field(
        default=0,
        ge=0,
        description="Total number of transformations in the batch"
    )

    successful_transformations: int = Field(
        default=0,
        ge=0,
        description="Number of transformations that succeeded"
    )

    combined_code: str = Field(
        default="",
        description="Combined pandas code for all transformations"
    )

    def get_summary(self) -> str:
        """Get a human-readable summary of the batch result."""
        if not self.success:
            failed = [r for r in self.results if not r.success]
            if failed:
                return f"Batch failed at step {len(self.results)}: {failed[-1].error_message}"
            return "Batch failed"

        return (
            f"Batch complete: {self.successful_transformations}/{self.total_transformations} "
            f"transformations applied. Final node: {self.final_node_id}"
        )
