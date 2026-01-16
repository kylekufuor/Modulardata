# =============================================================================
# core/services/module_run_service.py - Module Run Business Logic
# =============================================================================
# Handles running a module on new data:
# - Schema matching
# - Transformation replay
# - Run logging
# =============================================================================

import io
import logging
import time
from typing import Any
from uuid import UUID
from datetime import datetime

import pandas as pd

from lib.supabase_client import SupabaseClient
from lib.profiler import generate_profile, generate_contract, match_schema
from core.services.node_service import NodeService
from core.services.storage_service import StorageService
from core.models.profile import MatchConfidence

logger = logging.getLogger(__name__)


class ModuleRunService:
    """
    Service for running modules on new data.

    Handles the full flow:
    1. Profile incoming file
    2. Match against module's schema
    3. Replay transformations (if compatible)
    4. Log run results
    """

    @staticmethod
    def create_run(
        session_id: str,
        user_id: str,
        input_filename: str,
        input_row_count: int,
        input_column_count: int,
        confidence_score: float,
        confidence_level: str,
        column_mappings: list[dict] | None = None,
        discrepancies: list[dict] | None = None,
        input_storage_path: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a new run record.

        Args:
            session_id: Module (session) being run
            user_id: User who initiated the run
            input_filename: Name of uploaded file
            input_row_count: Number of rows in input
            input_column_count: Number of columns in input
            confidence_score: Schema match confidence (0-100)
            confidence_level: HIGH, MEDIUM, LOW, or NO_MATCH
            column_mappings: How columns were mapped
            discrepancies: Schema issues found
            input_storage_path: Where input file was stored

        Returns:
            Created run record
        """
        client = SupabaseClient.get_client()

        data = {
            "session_id": session_id,
            "user_id": user_id,
            "input_filename": input_filename,
            "input_row_count": input_row_count,
            "input_column_count": input_column_count,
            "confidence_score": confidence_score,
            "confidence_level": confidence_level,
            "column_mappings": column_mappings,
            "discrepancies": discrepancies,
            "input_storage_path": input_storage_path,
            "status": "pending",
        }

        try:
            response = client.table("module_runs").insert(data).execute()

            if response.data:
                run = response.data[0]
                logger.info(f"Created module run: {run['id']} for session {session_id}")
                return run

            raise Exception("Insert returned no data")

        except Exception as e:
            logger.error(f"Failed to create module run: {e}")
            raise

    @staticmethod
    def update_run(
        run_id: str,
        status: str | None = None,
        error_message: str | None = None,
        error_details: dict | None = None,
        output_row_count: int | None = None,
        output_column_count: int | None = None,
        output_storage_path: str | None = None,
        duration_ms: int | None = None,
        timing_breakdown: dict | None = None,
    ) -> dict[str, Any]:
        """Update a run record with results."""
        client = SupabaseClient.get_client()

        update_data = {}
        if status is not None:
            update_data["status"] = status
        if error_message is not None:
            update_data["error_message"] = error_message
        if error_details is not None:
            update_data["error_details"] = error_details
        if output_row_count is not None:
            update_data["output_row_count"] = output_row_count
        if output_column_count is not None:
            update_data["output_column_count"] = output_column_count
        if output_storage_path is not None:
            update_data["output_storage_path"] = output_storage_path
        if duration_ms is not None:
            update_data["duration_ms"] = duration_ms
        if timing_breakdown is not None:
            update_data["timing_breakdown"] = timing_breakdown

        if not update_data:
            return ModuleRunService.get_run(run_id)

        try:
            response = (
                client.table("module_runs")
                .update(update_data)
                .eq("id", run_id)
                .execute()
            )

            if response.data:
                return response.data[0]

            return ModuleRunService.get_run(run_id)

        except Exception as e:
            logger.error(f"Failed to update module run: {e}")
            raise

    @staticmethod
    def get_run(run_id: str) -> dict[str, Any] | None:
        """Get a run by ID."""
        client = SupabaseClient.get_client()

        try:
            response = (
                client.table("module_runs")
                .select("*")
                .eq("id", run_id)
                .single()
                .execute()
            )
            return response.data
        except Exception as e:
            logger.error(f"Failed to get module run: {e}")
            return None

    @staticmethod
    def list_runs(
        session_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List runs for a module."""
        client = SupabaseClient.get_client()

        try:
            response = (
                client.table("module_runs")
                .select("*")
                .eq("session_id", session_id)
                .order("created_at", desc=True)
                .range(offset, offset + limit - 1)
                .execute()
            )
            return response.data or []
        except Exception as e:
            logger.error(f"Failed to list module runs: {e}")
            return []

    @staticmethod
    def get_transformation_chain(session_id: str, use_deployed: bool = True) -> list[dict[str, Any]]:
        """
        Get the full transformation chain for a module.

        Args:
            session_id: Module session ID
            use_deployed: If True, use deployed_node_id; if False, use current_node_id

        Returns nodes from root (original) to target node, in order.
        """
        from core.services.session_service import SessionService

        # Get session to find target node
        session = SessionService.get_session(session_id)

        # Use deployed_node_id for runs, current_node_id otherwise
        if use_deployed:
            target_node_id = session.get("deployed_node_id")
        else:
            target_node_id = session.get("current_node_id")

        if not target_node_id:
            return []

        # Get lineage from target back to root
        lineage = NodeService.get_node_lineage(target_node_id, depth=100)

        # Lineage is returned root -> target, which is what we want
        return lineage

    @staticmethod
    def match_incoming_file(
        df: pd.DataFrame,
        session_id: str,
    ) -> dict[str, Any]:
        """
        Match an incoming DataFrame against a module's expected schema.

        Args:
            df: Incoming data as DataFrame
            session_id: Module to match against

        Returns:
            Dict with match results:
            - confidence_score: 0-100
            - confidence_level: HIGH/MEDIUM/LOW/NO_MATCH
            - auto_processable: bool
            - column_mappings: list
            - discrepancies: list
            - error_message: str (if NO_MATCH)
        """
        # Profile the incoming data
        incoming_profile = generate_profile(df)

        # Get the original node's profile to create contract
        root_node = NodeService.get_root_node(session_id)

        if not root_node:
            return {
                "confidence_score": 0,
                "confidence_level": "NO_MATCH",
                "auto_processable": False,
                "column_mappings": [],
                "discrepancies": [],
                "error_message": "Module has no data. Please upload data to the module first.",
            }

        # Get the original profile
        original_profile_json = root_node.get("profile_json")

        if not original_profile_json:
            return {
                "confidence_score": 0,
                "confidence_level": "NO_MATCH",
                "auto_processable": False,
                "column_mappings": [],
                "discrepancies": [],
                "error_message": "Module has no profile data. Please re-upload data to the module.",
            }

        # Reconstruct DataProfile from JSON
        from core.models.profile import DataProfile
        original_profile = DataProfile.model_validate(original_profile_json)

        # Generate contract from original profile
        contract = generate_contract(original_profile, module_id=session_id)

        # Match schemas
        match_result = match_schema(incoming_profile, contract)

        # Convert to serializable dict
        column_mappings = [
            {
                "incoming_name": m.incoming_name,
                "contract_name": m.contract_name,
                "match_type": m.match_type,
                "confidence": m.confidence,
                "type_compatible": m.type_compatible,
                "notes": m.notes,
            }
            for m in match_result.column_mappings
        ]

        discrepancies = [
            {
                "type": d.discrepancy_type,
                "severity": d.severity.value if hasattr(d.severity, 'value') else str(d.severity),
                "column": d.column,
                "description": d.description,
                "suggestion": d.suggestion,
            }
            for d in match_result.discrepancies
        ]

        # Build user-friendly error message for NO_MATCH
        error_message = None
        if match_result.confidence_level == MatchConfidence.NO_MATCH:
            error_message = ModuleRunService._build_error_message(
                match_result, incoming_profile, contract
            )

        return {
            "confidence_score": match_result.confidence_score,
            "confidence_level": match_result.confidence_level.value,
            "auto_processable": match_result.auto_processable,
            "is_compatible": match_result.is_compatible,
            "column_mappings": column_mappings,
            "discrepancies": discrepancies,
            "unmapped_required": match_result.unmapped_required,
            "unmapped_incoming": match_result.unmapped_incoming,
            "error_message": error_message,
        }

    @staticmethod
    def _build_error_message(match_result, incoming_profile, contract) -> str:
        """Build a user-friendly error message for schema mismatch."""
        lines = [
            f"Schema Mismatch (Confidence: {match_result.confidence_score:.0f}%)",
            "",
        ]

        if match_result.unmapped_required:
            lines.append("Missing required columns:")
            for col in match_result.unmapped_required:
                lines.append(f"  - {col}")
            lines.append("")

        incoming_cols = [c.name for c in incoming_profile.columns]
        lines.append("Your file has these columns:")
        for col in incoming_cols[:10]:  # Show first 10
            lines.append(f"  - {col}")
        if len(incoming_cols) > 10:
            lines.append(f"  ... and {len(incoming_cols) - 10} more")
        lines.append("")

        lines.append("Expected columns:")
        for col in contract.columns[:10]:
            lines.append(f"  - {col.name}")
        if len(contract.columns) > 10:
            lines.append(f"  ... and {len(contract.columns) - 10} more")

        return "\n".join(lines)

    @staticmethod
    def execute_transformations(
        df: pd.DataFrame,
        session_id: str,
        column_mappings: list[dict] | None = None,
    ) -> pd.DataFrame:
        """
        Execute all transformations from a module on new data.

        Args:
            df: Input DataFrame
            session_id: Module to replay
            column_mappings: Optional column name mappings

        Returns:
            Transformed DataFrame
        """
        # Get transformation chain
        chain = ModuleRunService.get_transformation_chain(session_id)

        if not chain:
            logger.warning(f"No transformation chain found for session {session_id}")
            return df

        # Apply column mappings if provided
        if column_mappings:
            rename_map = {}
            for mapping in column_mappings:
                if mapping["incoming_name"] != mapping["contract_name"]:
                    rename_map[mapping["incoming_name"]] = mapping["contract_name"]

            if rename_map:
                df = df.rename(columns=rename_map)
                logger.info(f"Renamed columns: {rename_map}")

        # Execute each transformation (skip the root node which has no code)
        for node in chain:
            code = node.get("transformation_code")

            if not code:
                # Root node or no transformation
                continue

            logger.info(f"Executing transformation: {node.get('transformation', 'unknown')}")

            try:
                # Execute the transformation code
                # The code expects 'df' as input and modifies it in place or returns new df
                local_vars = {"df": df, "pd": pd}
                exec(code, {"pd": pd, "__builtins__": {}}, local_vars)
                df = local_vars.get("df", df)

            except Exception as e:
                logger.error(f"Transformation failed: {e}")
                raise RuntimeError(
                    f"Transformation failed at step '{node.get('transformation', 'unknown')}': {str(e)}"
                )

        return df

    @staticmethod
    def run_module(
        session_id: str,
        user_id: str,
        df: pd.DataFrame,
        filename: str,
        force: bool = False,
    ) -> dict[str, Any]:
        """
        Run a module on new data.

        Full flow:
        1. Profile incoming data
        2. Match against module schema
        3. If compatible (or forced), execute transformations
        4. Store output and log run

        Args:
            session_id: Module to run
            user_id: User running the module
            df: Input DataFrame
            filename: Original filename
            force: If True, run even with MEDIUM confidence

        Returns:
            Run result dict with status and output info
        """
        start_time = time.time()
        timing = {}

        # Step 1: Match schema
        match_start = time.time()
        match_result = ModuleRunService.match_incoming_file(df, session_id)
        timing["schema_match_ms"] = int((time.time() - match_start) * 1000)

        confidence_level = match_result["confidence_level"]

        # Step 2: Create run record
        run = ModuleRunService.create_run(
            session_id=session_id,
            user_id=user_id,
            input_filename=filename,
            input_row_count=len(df),
            input_column_count=len(df.columns),
            confidence_score=match_result["confidence_score"],
            confidence_level=confidence_level,
            column_mappings=match_result["column_mappings"],
            discrepancies=match_result["discrepancies"],
        )
        run_id = run["id"]

        # Step 3: Check if we should proceed
        if confidence_level == "NO_MATCH":
            # Cannot proceed - schema too different
            ModuleRunService.update_run(
                run_id=run_id,
                status="failed",
                error_message=match_result.get("error_message", "Schema mismatch - cannot process this file"),
                duration_ms=int((time.time() - start_time) * 1000),
                timing_breakdown=timing,
            )
            return {
                "run_id": run_id,
                "status": "failed",
                "confidence_score": match_result["confidence_score"],
                "confidence_level": confidence_level,
                "error_message": match_result.get("error_message"),
                "column_mappings": match_result["column_mappings"],
                "discrepancies": match_result["discrepancies"],
            }

        if confidence_level == "LOW":
            # Too risky without explicit user action
            ModuleRunService.update_run(
                run_id=run_id,
                status="failed",
                error_message="Schema confidence too low. Please review column mappings.",
                duration_ms=int((time.time() - start_time) * 1000),
                timing_breakdown=timing,
            )
            return {
                "run_id": run_id,
                "status": "failed",
                "confidence_score": match_result["confidence_score"],
                "confidence_level": confidence_level,
                "error_message": "Schema confidence too low. Please review column mappings.",
                "column_mappings": match_result["column_mappings"],
                "discrepancies": match_result["discrepancies"],
            }

        if confidence_level == "MEDIUM" and not force:
            # Need user confirmation
            return {
                "run_id": run_id,
                "status": "pending",
                "confidence_score": match_result["confidence_score"],
                "confidence_level": confidence_level,
                "requires_confirmation": True,
                "column_mappings": match_result["column_mappings"],
                "discrepancies": match_result["discrepancies"],
                "message": "Schema has some differences. Please confirm to proceed.",
            }

        # Step 4: Execute transformations
        transform_start = time.time()
        try:
            output_df = ModuleRunService.execute_transformations(
                df=df,
                session_id=session_id,
                column_mappings=match_result["column_mappings"],
            )
            timing["transform_ms"] = int((time.time() - transform_start) * 1000)

        except Exception as e:
            timing["transform_ms"] = int((time.time() - transform_start) * 1000)
            ModuleRunService.update_run(
                run_id=run_id,
                status="failed",
                error_message=f"Transformation failed: {str(e)}",
                error_details={"exception": str(e)},
                duration_ms=int((time.time() - start_time) * 1000),
                timing_breakdown=timing,
            )
            return {
                "run_id": run_id,
                "status": "failed",
                "confidence_score": match_result["confidence_score"],
                "confidence_level": confidence_level,
                "error_message": f"Transformation failed: {str(e)}",
            }

        # Step 5: Store output
        upload_start = time.time()
        output_path = f"runs/{run_id}/output.csv"

        try:
            StorageService.upload_csv(
                session_id=session_id,
                df=output_df,
                filename=f"run_{run_id[:8]}_output.csv",
                path_override=output_path,
            )
            timing["upload_ms"] = int((time.time() - upload_start) * 1000)
        except Exception as e:
            logger.error(f"Failed to upload output: {e}")
            # Continue anyway - transformation succeeded
            output_path = None
            timing["upload_ms"] = int((time.time() - upload_start) * 1000)

        # Step 6: Update run record
        total_duration = int((time.time() - start_time) * 1000)
        status = "warning_confirmed" if confidence_level == "MEDIUM" else "success"

        ModuleRunService.update_run(
            run_id=run_id,
            status=status,
            output_row_count=len(output_df),
            output_column_count=len(output_df.columns),
            output_storage_path=output_path,
            duration_ms=total_duration,
            timing_breakdown=timing,
        )

        return {
            "run_id": run_id,
            "status": status,
            "confidence_score": match_result["confidence_score"],
            "confidence_level": confidence_level,
            "input_rows": len(df),
            "input_columns": len(df.columns),
            "output_rows": len(output_df),
            "output_columns": len(output_df.columns),
            "output_storage_path": output_path,
            "duration_ms": total_duration,
            "column_mappings": match_result["column_mappings"],
        }

    @staticmethod
    def confirm_and_run(
        run_id: str,
        user_id: str,
    ) -> dict[str, Any]:
        """
        Confirm a pending run and execute it.

        Used when user confirms a MEDIUM confidence run.
        """
        run = ModuleRunService.get_run(run_id)

        if not run:
            raise ValueError(f"Run not found: {run_id}")

        if run["status"] != "pending":
            raise ValueError(f"Run is not pending: {run['status']}")

        if run["user_id"] != user_id:
            raise ValueError("Not authorized to confirm this run")

        # Re-fetch the input data and run with force=True
        # For now, we'll need the input file to be re-uploaded
        # In production, you'd store the input temporarily
        raise NotImplementedError(
            "Confirm and run requires input file re-upload. "
            "Use run_module with force=True instead."
        )
