# =============================================================================
# agents/engineer.py - Engineer Agent
# =============================================================================
# The Engineer Agent executes TechnicalPlans from the Strategist.
#
# Key responsibilities:
# - Execute transformation functions using transforms_v2 primitives
# - Validate plans and data before execution
# - Generate new node with transformed data
# - Track code for transparency ("show your work")
#
# Architecture:
# - PRIMARY: Uses transforms_v2 Engine for deterministic primitives (38 ops)
# - FALLBACK: Uses old registry for operations not yet in transforms_v2
# - Each transformation returns (DataFrame, code_string)
# - Supports batch execution for multiple plans -> single node
#
# Example:
#   engineer = EngineerAgent()
#   result = engineer.execute_plan(session_id, plan)
#   if result.success:
#       print(f"Created node {result.new_node_id}")
#       print(f"Code: {result.transformation_code}")
# =============================================================================

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime
from typing import Any

import pandas as pd

from agents.models.technical_plan import TechnicalPlan, TransformationType
from agents.models.execution_result import ExecutionResult, BatchExecutionResult
from agents.transformations import get_transformer, REGISTRY
from agents.plan_translator import PlanTranslator, TranslationError
from lib.profiler import generate_profile
from lib.supabase_client import SupabaseClient, SupabaseClientError

# Import transforms_v2 Engine
try:
    from transforms_v2 import Engine as V2Engine
    TRANSFORMS_V2_AVAILABLE = True
except ImportError:
    TRANSFORMS_V2_AVAILABLE = False
    V2Engine = None

# Set up logging
logger = logging.getLogger(__name__)


class EngineerError(Exception):
    """
    Error during transformation execution.

    Provides actionable error messages for debugging.
    """

    def __init__(
        self,
        message: str,
        code: str = "ENGINEER_ERROR",
        suggestion: str | None = None,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.suggestion = suggestion
        self.details = details or {}

    def __str__(self) -> str:
        result = f"[{self.code}] {self.message}"
        if self.suggestion:
            result += f" Suggestion: {self.suggestion}"
        return result


class EngineerAgent:
    """
    Agent that executes TechnicalPlans to transform data.

    The Engineer is deterministic - it uses transforms_v2 primitives
    (primary) or hardcoded transformation functions (fallback). This ensures:
    - Predictable, testable results
    - No code injection risks
    - Fast execution (no API calls)
    - Transparent code generation

    Usage:
        engineer = EngineerAgent()

        # Execute single plan
        result = engineer.execute_plan(session_id, plan)

        # Execute batch (multiple plans -> single node)
        batch_result = engineer.execute_batch(session_id, [plan1, plan2, plan3])

        # Execute on DataFrame directly (for testing/preview)
        df_result, code = engineer.execute_on_dataframe(df, plan)
    """

    def __init__(self):
        """Initialize the Engineer agent."""
        self._validate_registry()
        self._translator = PlanTranslator()
        self._v2_engine = V2Engine() if TRANSFORMS_V2_AVAILABLE else None

        if TRANSFORMS_V2_AVAILABLE:
            logger.info("Engineer initialized with transforms_v2 Engine (primary)")
        else:
            logger.warning("transforms_v2 not available, using legacy registry only")

    def _validate_registry(self) -> None:
        """Ensure transformation registry is populated."""
        if not REGISTRY:
            logger.warning("Transformation registry is empty - no transformations available")

    # -------------------------------------------------------------------------
    # Transformation Execution (v2 primary, legacy fallback)
    # -------------------------------------------------------------------------

    def _execute_transformation(
        self,
        df: pd.DataFrame,
        plan: TechnicalPlan,
    ) -> tuple[pd.DataFrame, str]:
        """
        Execute a transformation using transforms_v2 (primary) or legacy (fallback).

        Args:
            df: Input DataFrame
            plan: TechnicalPlan to execute

        Returns:
            Tuple of (transformed DataFrame, generated code string)

        Raises:
            EngineerError: If both v2 and legacy execution fail
        """
        # Try transforms_v2 first if available
        if self._v2_engine and self._translator.can_translate(plan):
            try:
                return self._execute_with_v2(df, plan)
            except TranslationError as e:
                logger.warning(f"transforms_v2 translation failed: {e}, falling back to legacy")
            except Exception as e:
                logger.warning(f"transforms_v2 execution failed: {e}, falling back to legacy")

        # Fallback to legacy registry
        return self._execute_with_legacy(df, plan)

    def _execute_with_v2(
        self,
        df: pd.DataFrame,
        plan: TechnicalPlan,
    ) -> tuple[pd.DataFrame, str]:
        """
        Execute using transforms_v2 Engine.

        Args:
            df: Input DataFrame
            plan: TechnicalPlan to execute

        Returns:
            Tuple of (transformed DataFrame, generated code string)

        Raises:
            EngineerError: If execution fails
        """
        # Translate TechnicalPlan to v2 format
        v2_plan = self._translator.translate(plan)

        logger.info(f"Executing via transforms_v2: {v2_plan}")

        # Execute using v2 Engine
        result = self._v2_engine.execute(df, v2_plan)

        if not result.success:
            raise EngineerError(
                message=f"transforms_v2 execution failed: {result.error}",
                code="V2_EXECUTION_FAILED",
            )

        # Generate code string for transparency
        code = self._generate_v2_code(v2_plan)

        return result.df, code

    def _execute_with_legacy(
        self,
        df: pd.DataFrame,
        plan: TechnicalPlan,
    ) -> tuple[pd.DataFrame, str]:
        """
        Execute using legacy transformation registry.

        Args:
            df: Input DataFrame
            plan: TechnicalPlan to execute

        Returns:
            Tuple of (transformed DataFrame, generated code string)

        Raises:
            EngineerError: If execution fails
        """
        transformer = get_transformer(plan.transformation_type)
        if not transformer:
            raise EngineerError(
                message=f"Unknown transformation type: {plan.transformation_type}",
                code="UNKNOWN_TRANSFORMATION",
                suggestion=f"Available types: {list(REGISTRY.keys())[:10]}...",
            )

        logger.info(f"Executing via legacy registry: {plan.transformation_type}")

        return transformer(df, plan)

    def _generate_v2_code(self, v2_plan: list[dict]) -> str:
        """Generate a code string from a v2 plan for transparency."""
        lines = ["# Generated by transforms_v2 Engine"]
        for step in v2_plan:
            op = step.get("op", "unknown")
            params = step.get("params", {})
            # Format params nicely
            param_strs = [f"{k}={repr(v)}" for k, v in params.items()
                         if not isinstance(v, pd.DataFrame)]  # Skip DataFrame params
            lines.append(f"df = engine.execute_single(df, '{op}', {{{', '.join(param_strs)}}})")
        return "\n".join(lines)

    # -------------------------------------------------------------------------
    # Main Execution Methods
    # -------------------------------------------------------------------------

    def execute_plan(
        self,
        session_id: str,
        plan: TechnicalPlan,
        df: pd.DataFrame | None = None,
        save_node: bool = True,
    ) -> ExecutionResult:
        """
        Execute a single TechnicalPlan.

        Args:
            session_id: The session UUID
            plan: TechnicalPlan from the Strategist
            df: Optional DataFrame (if not provided, loads from current node)
            save_node: If True, saves result to database as new node

        Returns:
            ExecutionResult with success status, new node ID, statistics

        Raises:
            EngineerError: If execution fails
        """
        start_time = time.time()

        try:
            # Handle UNDO operation specially
            if plan.is_undo():
                return self._execute_undo(session_id, plan)

            # 1. Load data if not provided
            if df is None:
                df, current_node = self._load_current_data(session_id)
            else:
                current_node = self._get_current_node(session_id)

            # 2. Validate plan
            self._validate_plan(plan)

            # 3. Validate columns exist
            self._validate_columns(df, plan)

            # 4. Execute transformation (try transforms_v2 first, then legacy)
            transformed_df, code = self._execute_transformation(df, plan)

            # 5. Validate result
            self._validate_result(df, transformed_df, plan)

            # 6. Calculate statistics
            rows_affected = abs(len(df) - len(transformed_df))
            columns_affected = plan.get_affected_columns()

            # 7. Generate profile for new data
            profile = generate_profile(transformed_df)

            # 8. Save node if requested
            new_node_id = None
            parent_node_id = current_node.get("id") if current_node else None

            if save_node:
                new_node = self._save_node(
                    session_id=session_id,
                    parent_id=parent_node_id,
                    df=transformed_df,
                    profile=profile,
                    transformation=plan.explanation,
                    code=code,
                )
                new_node_id = new_node.get("id")

                # Update session's current node
                self._update_session_current_node(session_id, new_node_id)

            execution_time_ms = (time.time() - start_time) * 1000

            return ExecutionResult(
                success=True,
                session_id=session_id,
                new_node_id=new_node_id,
                parent_node_id=parent_node_id,
                row_count=len(transformed_df),
                column_count=len(transformed_df.columns),
                rows_affected=rows_affected,
                columns_affected=columns_affected,
                transformation_type=str(plan.transformation_type),
                transformation=plan.explanation,
                transformation_code=code,
                execution_time_ms=execution_time_ms,
                preview_data=transformed_df.head(5).to_dict('records'),
            )

        except EngineerError:
            raise
        except Exception as e:
            logger.error(f"Execution failed: {e}", exc_info=True)
            execution_time_ms = (time.time() - start_time) * 1000

            return ExecutionResult(
                success=False,
                error_message=str(e),
                error_type=type(e).__name__,
                session_id=session_id,
                execution_time_ms=execution_time_ms,
            )

    def execute_batch(
        self,
        session_id: str,
        plans: list[TechnicalPlan],
        df: pd.DataFrame | None = None,
    ) -> BatchExecutionResult:
        """
        Execute multiple TechnicalPlans, creating a single node.

        All transformations are applied sequentially, and only one
        node is created with the combined result. This is used when
        the user applies a queue of transformations.

        Args:
            session_id: The session UUID
            plans: List of TechnicalPlans to execute in order
            df: Optional DataFrame (if not provided, loads from current node)

        Returns:
            BatchExecutionResult with combined results
        """
        if not plans:
            return BatchExecutionResult(
                success=False,
                results=[],
                total_transformations=0,
            )

        results: list[ExecutionResult] = []
        all_code: list[str] = []
        all_explanations: list[str] = []

        try:
            # Load initial data
            if df is None:
                current_df, current_node = self._load_current_data(session_id)
            else:
                current_df = df.copy()
                current_node = self._get_current_node(session_id)

            parent_node_id = current_node.get("id") if current_node else None

            # Execute each plan sequentially (without saving intermediate nodes)
            for i, plan in enumerate(plans):
                result = self.execute_plan(
                    session_id=session_id,
                    plan=plan,
                    df=current_df,
                    save_node=False,  # Don't save intermediate nodes
                )

                results.append(result)

                if not result.success:
                    # Stop on first failure
                    return BatchExecutionResult(
                        success=False,
                        results=results,
                        total_transformations=len(plans),
                        successful_transformations=i,
                    )

                # Execute transformation to update current_df
                current_df, code = self._execute_transformation(current_df, plan)
                all_code.append(code)
                all_explanations.append(plan.explanation)

            # Create single node with combined transformation
            combined_explanation = "; ".join(all_explanations)
            combined_code = "\n".join(all_code)

            profile = generate_profile(current_df)
            new_node = self._save_node(
                session_id=session_id,
                parent_id=parent_node_id,
                df=current_df,
                profile=profile,
                transformation=combined_explanation,
                code=combined_code,
            )
            new_node_id = new_node.get("id")

            # Update session
            self._update_session_current_node(session_id, new_node_id)

            return BatchExecutionResult(
                success=True,
                results=results,
                final_node_id=new_node_id,
                total_transformations=len(plans),
                successful_transformations=len(plans),
                combined_code=combined_code,
            )

        except Exception as e:
            logger.error(f"Batch execution failed: {e}", exc_info=True)
            return BatchExecutionResult(
                success=False,
                results=results,
                total_transformations=len(plans),
                successful_transformations=len(results),
            )

    def execute_on_dataframe(
        self,
        df: pd.DataFrame,
        plan: TechnicalPlan,
    ) -> tuple[pd.DataFrame, str]:
        """
        Execute a plan directly on a DataFrame (for testing/preview).

        This does not interact with the database at all.

        Args:
            df: Input DataFrame
            plan: TechnicalPlan to execute

        Returns:
            Tuple of (transformed DataFrame, generated code)

        Raises:
            EngineerError: If transformation fails
        """
        # Validate
        self._validate_plan(plan)
        self._validate_columns(df, plan)

        # Execute using transforms_v2 (primary) or legacy (fallback)
        return self._execute_transformation(df, plan)

    # -------------------------------------------------------------------------
    # Undo Operation
    # -------------------------------------------------------------------------

    def _execute_undo(self, session_id: str, plan: TechnicalPlan) -> ExecutionResult:
        """
        Execute an UNDO operation by reverting to a previous node.

        Args:
            session_id: The session UUID
            plan: TechnicalPlan with rollback_to_node_id

        Returns:
            ExecutionResult pointing to the rollback node
        """
        start_time = time.time()

        try:
            # Get current node
            current_node = self._get_current_node(session_id)
            if not current_node:
                raise EngineerError(
                    message="No current node to undo from",
                    code="NO_CURRENT_NODE",
                )

            # Determine target node
            target_node_id = plan.rollback_to_node_id

            if not target_node_id:
                # Default: go to parent
                target_node_id = current_node.get("parent_id")
                if not target_node_id:
                    raise EngineerError(
                        message="Cannot undo: this is the root node",
                        code="NO_PARENT_NODE",
                        suggestion="This is the original data, there's nothing to undo",
                    )

            # Verify target node exists
            target_node = SupabaseClient.fetch_node(target_node_id)
            if not target_node:
                raise EngineerError(
                    message=f"Target node not found: {target_node_id}",
                    code="NODE_NOT_FOUND",
                )

            # Update session to point to target node
            self._update_session_current_node(session_id, target_node_id)

            execution_time_ms = (time.time() - start_time) * 1000

            return ExecutionResult(
                success=True,
                session_id=session_id,
                new_node_id=target_node_id,
                parent_node_id=current_node.get("id"),
                row_count=target_node.get("row_count", 0),
                column_count=target_node.get("column_count", 0),
                rows_affected=0,
                transformation_type="undo",
                transformation=f"Rolled back to node {target_node_id[:8]}...",
                transformation_code="# Undo: reverted to previous state",
                execution_time_ms=execution_time_ms,
            )

        except EngineerError:
            raise
        except Exception as e:
            logger.error(f"Undo failed: {e}", exc_info=True)
            return ExecutionResult(
                success=False,
                error_message=str(e),
                error_type=type(e).__name__,
                session_id=session_id,
            )

    # -------------------------------------------------------------------------
    # Validation Methods
    # -------------------------------------------------------------------------

    def _validate_plan(self, plan: TechnicalPlan) -> None:
        """Validate the TechnicalPlan before execution."""
        if not plan.transformation_type:
            raise EngineerError(
                message="Plan missing transformation_type",
                code="INVALID_PLAN",
            )

    def _validate_columns(self, df: pd.DataFrame, plan: TechnicalPlan) -> None:
        """Validate that required columns exist in the DataFrame."""
        required_columns = plan.get_affected_columns()

        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise EngineerError(
                message=f"Columns not found in data: {missing}",
                code="COLUMN_NOT_FOUND",
                suggestion=f"Available columns: {list(df.columns)}",
                details={"missing": missing, "available": list(df.columns)},
            )

    def _validate_result(
        self,
        original_df: pd.DataFrame,
        result_df: pd.DataFrame,
        plan: TechnicalPlan,
    ) -> None:
        """Validate the transformation result."""
        if result_df is None:
            raise EngineerError(
                message="Transformation returned None",
                code="NULL_RESULT",
            )

        if not isinstance(result_df, pd.DataFrame):
            raise EngineerError(
                message=f"Expected DataFrame, got {type(result_df)}",
                code="INVALID_RESULT_TYPE",
            )

    # -------------------------------------------------------------------------
    # Database Operations
    # -------------------------------------------------------------------------

    def _get_current_node(self, session_id: str) -> dict[str, Any] | None:
        """Get the current node for a session."""
        try:
            return SupabaseClient.fetch_current_node(session_id)
        except SupabaseClientError as e:
            logger.warning(f"Could not fetch current node: {e}")
            return None

    def _load_current_data(
        self,
        session_id: str,
    ) -> tuple[pd.DataFrame, dict[str, Any] | None]:
        """
        Load the current DataFrame for a session from cloud storage.

        Args:
            session_id: Session UUID

        Returns:
            Tuple of (DataFrame, current_node dict) or (empty DataFrame, None)
        """
        from core.services.storage_service import StorageService

        current_node = self._get_current_node(session_id)

        if not current_node:
            logger.warning(f"No current node for session {session_id}")
            return pd.DataFrame(), None

        # Load from storage using the storage_path stored in the node
        storage_path = current_node.get("storage_path")
        if not storage_path:
            logger.warning(f"No storage_path in node for session {session_id}")
            return pd.DataFrame(), current_node

        try:
            df = StorageService.download_csv(storage_path)
            return df, current_node
        except Exception as e:
            logger.error(f"Failed to load data from storage: {e}")
            return pd.DataFrame(), current_node

    def _save_node(
        self,
        session_id: str,
        parent_id: str | None,
        df: pd.DataFrame,
        profile: Any,
        transformation: str,
        code: str,
    ) -> dict[str, Any]:
        """
        Save a new node to the database.

        Note: In production, this would also save the DataFrame
        to cloud storage and store the URL.
        """
        try:
            client = SupabaseClient.get_client()

            # Generate new node ID
            node_id = str(uuid.uuid4())

            # Prepare profile JSON
            profile_json = profile.model_dump() if hasattr(profile, 'model_dump') else profile

            node_data = {
                "id": node_id,
                "session_id": session_id,
                "parent_id": parent_id,
                "transformation": transformation,
                "transformation_code": code,
                "row_count": len(df),
                "column_count": len(df.columns),
                "profile_json": profile_json,
                # In production: "data_url": storage_url
            }

            response = client.table("nodes").insert(node_data).execute()

            if response.data:
                logger.info(f"Created node {node_id}")
                return response.data[0]

            raise EngineerError(
                message="Failed to save node - no data returned",
                code="SAVE_NODE_FAILED",
            )

        except EngineerError:
            raise
        except Exception as e:
            raise EngineerError(
                message=f"Failed to save node: {e}",
                code="SAVE_NODE_FAILED",
                details={"session_id": session_id},
            )

    def _update_session_current_node(self, session_id: str, node_id: str) -> None:
        """Update the session's current_node_id."""
        try:
            client = SupabaseClient.get_client()

            response = (
                client.table("sessions")
                .update({"current_node_id": node_id})
                .eq("id", session_id)
                .execute()
            )

            if response.data:
                logger.info(f"Updated session {session_id} current_node to {node_id}")
            else:
                logger.warning(f"No session updated for {session_id}")

        except Exception as e:
            logger.error(f"Failed to update session: {e}")
            # Don't raise - node was already created

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    def list_available_transformations(self) -> list[str]:
        """List all available transformation types."""
        return [
            k.value if hasattr(k, 'value') else str(k)
            for k in REGISTRY.keys()
            if not isinstance(k, str)  # Avoid duplicates
        ]

    def get_transformation_info(self, trans_type: TransformationType) -> dict[str, Any]:
        """Get information about a specific transformation type."""
        transformer = get_transformer(trans_type)

        if not transformer:
            return {"error": f"Unknown transformation: {trans_type}"}

        return {
            "type": str(trans_type),
            "function": transformer.__name__,
            "docstring": transformer.__doc__,
        }
