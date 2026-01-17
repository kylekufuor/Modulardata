# =============================================================================
# transforms_v2/engine.py - Deterministic Execution Engine
# =============================================================================
# Executes plans (sequences of primitives) on DataFrames.
# Provides rollback, logging, and detailed execution results.
# =============================================================================

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from transforms_v2.registry import get_primitive
from transforms_v2.types import PrimitiveResult


@dataclass
class StepResult:
    """Result of executing a single step in a plan."""
    step_index: int
    operation: str
    params: dict[str, Any]
    result: PrimitiveResult
    duration_ms: float = 0.0


@dataclass
class ExecutionResult:
    """
    Result of executing an entire plan.

    Contains the final DataFrame and detailed results for each step.
    """
    success: bool
    df: pd.DataFrame | None = None
    steps: list[StepResult] = field(default_factory=list)
    error: str | None = None
    error_step: int | None = None
    total_duration_ms: float = 0.0

    @property
    def rows_before(self) -> int:
        if not self.steps:
            return 0
        return self.steps[0].result.rows_before

    @property
    def rows_after(self) -> int:
        if not self.steps or self.df is None:
            return 0
        return len(self.df)

    @property
    def total_rows_changed(self) -> int:
        return self.rows_after - self.rows_before


class Engine:
    """
    Deterministic execution engine for transformation plans.

    A plan is a list of operations:
        [
            {"op": "remove_duplicates", "params": {"subset": ["email"]}},
            {"op": "change_text_casing", "params": {"column": "name", "case": "title"}}
        ]

    The engine executes each operation in sequence, tracking results
    and providing rollback capability on failure.

    Usage:
        engine = Engine()
        result = engine.execute(df, plan)

        if result.success:
            cleaned_df = result.df
        else:
            print(f"Failed at step {result.error_step}: {result.error}")
    """

    def __init__(self, stop_on_error: bool = True, copy_input: bool = True):
        """
        Initialize the engine.

        Args:
            stop_on_error: If True, stop execution on first error.
                          If False, skip failed steps and continue.
            copy_input: If True, make a copy of input DataFrame before processing.
                       Set to False for large DataFrames if you don't need rollback.
        """
        self.stop_on_error = stop_on_error
        self.copy_input = copy_input

    def execute(
        self,
        df: pd.DataFrame,
        plan: list[dict[str, Any]],
    ) -> ExecutionResult:
        """
        Execute a plan on a DataFrame.

        Args:
            df: Input DataFrame
            plan: List of operations, each with "op" and "params" keys

        Returns:
            ExecutionResult with success/failure, final DataFrame, and step details
        """
        import time

        start_time = time.time()

        # Optionally copy input to preserve original
        current_df = df.copy() if self.copy_input else df
        original_df = df.copy() if self.copy_input else None

        steps: list[StepResult] = []

        for i, step in enumerate(plan):
            step_start = time.time()

            op_name = step.get("op")
            params = step.get("params", {})

            if not op_name:
                error = f"Step {i}: Missing 'op' key"
                if self.stop_on_error:
                    return ExecutionResult(
                        success=False,
                        df=original_df,
                        steps=steps,
                        error=error,
                        error_step=i,
                        total_duration_ms=(time.time() - start_time) * 1000,
                    )
                continue

            # Get the primitive class
            primitive_cls = get_primitive(op_name)
            if primitive_cls is None:
                error = f"Step {i}: Unknown primitive '{op_name}'"
                if self.stop_on_error:
                    return ExecutionResult(
                        success=False,
                        df=original_df,
                        steps=steps,
                        error=error,
                        error_step=i,
                        total_duration_ms=(time.time() - start_time) * 1000,
                    )
                continue

            # Instantiate and execute
            primitive = primitive_cls()

            # Validate parameters
            valid, errors = primitive.validate_params(params)
            if not valid:
                error = f"Step {i}: Invalid params - {', '.join(errors)}"
                if self.stop_on_error:
                    return ExecutionResult(
                        success=False,
                        df=original_df,
                        steps=steps,
                        error=error,
                        error_step=i,
                        total_duration_ms=(time.time() - start_time) * 1000,
                    )
                continue

            # Execute the primitive
            try:
                result = primitive.execute(current_df, params)
            except Exception as e:
                result = PrimitiveResult(
                    success=False,
                    df=None,
                    rows_before=len(current_df),
                    rows_after=len(current_df),
                    cols_before=len(current_df.columns),
                    cols_after=len(current_df.columns),
                    error=str(e),
                )

            step_duration = (time.time() - step_start) * 1000

            step_result = StepResult(
                step_index=i,
                operation=op_name,
                params=params,
                result=result,
                duration_ms=step_duration,
            )
            steps.append(step_result)

            if not result.success:
                if self.stop_on_error:
                    return ExecutionResult(
                        success=False,
                        df=original_df,
                        steps=steps,
                        error=result.error or f"Step {i} failed",
                        error_step=i,
                        total_duration_ms=(time.time() - start_time) * 1000,
                    )
            else:
                # Update current DataFrame for next step
                current_df = result.df

        total_duration = (time.time() - start_time) * 1000

        return ExecutionResult(
            success=True,
            df=current_df,
            steps=steps,
            total_duration_ms=total_duration,
        )

    def validate_plan(self, plan: list[dict[str, Any]]) -> tuple[bool, list[str]]:
        """
        Validate a plan without executing it.

        Checks that all operations exist and parameters are valid.

        Args:
            plan: List of operations

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        for i, step in enumerate(plan):
            op_name = step.get("op")
            params = step.get("params", {})

            if not op_name:
                errors.append(f"Step {i}: Missing 'op' key")
                continue

            primitive_cls = get_primitive(op_name)
            if primitive_cls is None:
                errors.append(f"Step {i}: Unknown primitive '{op_name}'")
                continue

            primitive = primitive_cls()
            valid, param_errors = primitive.validate_params(params)
            if not valid:
                for err in param_errors:
                    errors.append(f"Step {i}: {err}")

        return len(errors) == 0, errors

    def dry_run(
        self,
        df: pd.DataFrame,
        plan: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Perform a dry run to show what would happen without executing.

        Args:
            df: Input DataFrame
            plan: List of operations

        Returns:
            Dict with plan summary and expected changes
        """
        valid, errors = self.validate_plan(plan)

        summary = {
            "valid": valid,
            "errors": errors,
            "steps": [],
            "input_rows": len(df),
            "input_cols": len(df.columns),
        }

        for i, step in enumerate(plan):
            op_name = step.get("op", "unknown")
            params = step.get("params", {})

            primitive_cls = get_primitive(op_name)
            if primitive_cls:
                info = primitive_cls.info()
                summary["steps"].append({
                    "index": i,
                    "operation": op_name,
                    "description": info.description,
                    "params": params,
                    "may_change_rows": info.may_change_row_count,
                    "may_change_cols": info.may_change_col_count,
                })
            else:
                summary["steps"].append({
                    "index": i,
                    "operation": op_name,
                    "error": f"Unknown primitive '{op_name}'",
                })

        return summary
