# =============================================================================
# workers/tasks.py - Celery Task Definitions
# =============================================================================
# Defines background tasks for AI processing and data transformation.
#
# Tasks:
# - process_chat_message: Full pipeline (Strategist → Engineer → Tester)
# - apply_transformation: Execute a single transformation
# - generate_profile: Profile a dataset (for large files)
# =============================================================================

import logging
from typing import Any
from celery import shared_task, current_task
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def _safe_serialize_rows(df: pd.DataFrame, max_rows: int = 10) -> list[dict]:
    """
    Safely convert DataFrame rows to JSON-serializable dicts.

    Handles NaT, NaN, numpy types, and other non-serializable values.
    """
    rows = []
    for _, row in df.head(max_rows).iterrows():
        safe_row = {}
        for col, val in row.items():
            # Handle various non-serializable types
            if pd.isna(val):
                safe_row[col] = None
            elif isinstance(val, (pd.Timestamp, np.datetime64)):
                safe_row[col] = str(val) if not pd.isna(val) else None
            elif isinstance(val, (np.integer, np.floating)):
                safe_row[col] = val.item()  # Convert numpy scalar to Python type
            elif isinstance(val, np.ndarray):
                safe_row[col] = val.tolist()
            else:
                safe_row[col] = val
        rows.append(safe_row)
    return rows


# =============================================================================
# Task State Updates
# =============================================================================

def update_progress(current: int, total: int, message: str = "Processing..."):
    """
    Update task progress for polling.

    Args:
        current: Current step number
        total: Total steps
        message: Status message
    """
    if current_task:
        current_task.update_state(
            state="PROGRESS",
            meta={
                "current": current,
                "total": total,
                "percent": int((current / total) * 100),
                "message": message,
            }
        )


def create_short_node_label(steps: list[dict], max_length: int = 60) -> str:
    """
    Create a short, readable label for a node based on transformation steps.

    Args:
        steps: List of transformation step dicts
        max_length: Maximum label length

    Returns:
        Short label like "Fill email & age nulls" or "3 transformations"
    """
    if not steps:
        return "Transformation"

    if len(steps) == 1:
        # Single step - create short version
        step = steps[0]
        trans_type = step.get("transformation_type", "transform")
        columns = step.get("target_columns", [])

        # Create short labels based on type
        type_labels = {
            "fill_nulls": "Fill nulls",
            "drop_rows": "Remove rows",
            "drop_columns": "Remove columns",
            "deduplicate": "Deduplicate",
            "trim_whitespace": "Trim whitespace",
            "standardize": "Standardize",
            "rename_column": "Rename column",
            "replace_values": "Replace values",
            "change_case": "Change case",
            "parse_date": "Parse dates",
            "format_date": "Format dates",
            "convert_type": "Convert type",
            "filter_rows": "Filter rows",
        }

        base = type_labels.get(trans_type, "Transform")

        if columns:
            if len(columns) == 1:
                label = f"{base} in {columns[0]}"
            elif len(columns) <= 3:
                label = f"{base} in {', '.join(columns)}"
            else:
                label = f"{base} in {len(columns)} columns"
        else:
            label = base

        return label[:max_length]

    else:
        # Multiple steps - summarize
        trans_types = [s.get("transformation_type", "") for s in steps]
        unique_types = list(set(trans_types))

        # Get all affected columns
        all_columns = []
        for s in steps:
            all_columns.extend(s.get("target_columns", []))
        unique_columns = list(set(all_columns))

        if len(unique_types) == 1:
            # Same operation on multiple columns
            type_label = {
                "fill_nulls": "Fill nulls",
                "drop_rows": "Remove rows",
                "trim_whitespace": "Trim",
            }.get(unique_types[0], "Transform")

            if len(unique_columns) <= 3:
                label = f"{type_label} in {', '.join(unique_columns)}"
            else:
                label = f"{type_label} in {len(unique_columns)} columns"
        else:
            # Different operations
            label = f"{len(steps)} transformations"
            if unique_columns and len(unique_columns) <= 2:
                label += f" on {', '.join(unique_columns)}"

        return label[:max_length]


# =============================================================================
# Chat Processing Task
# =============================================================================

@shared_task(bind=True, name="workers.tasks.process_chat_message")
def process_chat_message(
    self,
    session_id: str,
    message: str,
    user_id: str | None = None,
) -> dict[str, Any]:
    """
    Process a user chat message through the 3-agent pipeline.

    This is the main task that handles conversational data transformation:
    1. Load session context (current data, history)
    2. Run Strategist to create transformation plan
    3. Run Engineer to execute transformation
    4. Run Tester to validate results
    5. Save new node and update session

    Args:
        session_id: The session UUID
        message: User's natural language request
        user_id: Optional user ID for logging

    Returns:
        Dict with:
        - success: bool
        - node_id: New node UUID (if successful)
        - transformation: Description of what was done
        - rows_before: Row count before
        - rows_after: Row count after
        - code: Generated pandas code

    Raises:
        Exception: If processing fails (will be caught by Celery)
    """
    logger.info(f"Processing chat for session {session_id}: {message[:50]}...")

    try:
        # Step 1: Load context
        update_progress(1, 5, "Loading session data...")

        from lib.supabase_client import SupabaseClient
        from lib.memory import build_conversation_context

        # Get session
        session = SupabaseClient.fetch_session(session_id)
        if not session:
            return {
                "success": False,
                "error": f"Session not found: {session_id}",
            }

        # Build context
        context = build_conversation_context(session_id)

        # Step 2: Run Strategist
        update_progress(2, 5, "Analyzing your request...")

        from agents.strategist import StrategistAgent

        strategist = StrategistAgent()
        plan = strategist.create_plan(session_id, message)

        if not plan:
            return {
                "success": False,
                "error": "Could not understand the request",
            }

        logger.info(f"Strategist created plan: {plan.transformation_type}")

        # Step 3: Run Engineer
        update_progress(3, 5, "Applying transformation...")

        from agents.engineer import EngineerAgent
        from core.services.storage_service import StorageService

        # Load current data
        current_node = SupabaseClient.fetch_current_node(session_id)
        if not current_node:
            return {
                "success": False,
                "error": "No data uploaded for this session",
            }

        df = StorageService.download_csv(current_node["storage_path"])
        rows_before = len(df)

        # Execute transformation
        engineer = EngineerAgent()
        result_df, code = engineer.execute_on_dataframe(df, plan)
        rows_after = len(result_df)

        logger.info(f"Engineer executed: {rows_before} → {rows_after} rows")

        # Step 4: Run Tester
        update_progress(4, 5, "Validating results...")

        from agents.tester import TesterAgent

        tester = TesterAgent()
        test_result = tester.validate(df, result_df, plan)

        if not test_result.passed:
            logger.warning(f"Tester found issues: {test_result.summary}")

        # Step 5: Save results
        update_progress(5, 5, "Saving changes...")

        from core.services.node_service import NodeService
        from core.services.session_service import SessionService
        from lib.profiler import generate_profile

        # Upload new CSV
        storage_path = StorageService.upload_csv(
            session_id=session_id,
            df=result_df,
            node_id=None,  # Will be set after node creation
        )

        # Generate profile
        profile = generate_profile(result_df)

        # Create new node
        node = NodeService.create_node(
            session_id=session_id,
            parent_id=current_node["id"],
            storage_path=storage_path,
            row_count=len(result_df),
            column_count=len(result_df.columns),
            profile_json=profile.model_dump(),
            transformation=plan.explanation,
            transformation_code=code,
            preview_rows=_safe_serialize_rows(result_df, 10),
            step_descriptions=[plan.explanation],
        )

        # Update session
        SessionService.update_session(
            session_id=session_id,
            current_node_id=node["id"],
        )

        # Revert to draft if module was deployed (editing a deployed module)
        SessionService.revert_to_draft(session_id)

        # Save chat messages
        SupabaseClient.insert_chat_message(
            session_id=session_id,
            role="user",
            content=message,
            node_id=current_node["id"],
        )

        SupabaseClient.insert_chat_message(
            session_id=session_id,
            role="assistant",
            content=f"Done! {plan.explanation}",
            node_id=node["id"],
            metadata={
                "transformation_type": str(plan.transformation_type),
                "rows_affected": abs(rows_before - rows_after),
                "code": code,
            }
        )

        logger.info(f"Task completed: Created node {node['id']}")

        # Broadcast node_created event to WebSocket clients
        try:
            from app.websocket.broadcast import publish_node_created, publish_task_complete

            publish_node_created(
                session_id=session_id,
                node_id=node["id"],
                transformation=plan.explanation,
                row_count=rows_after,
                column_count=len(result_df.columns),
            )

            publish_task_complete(
                session_id=session_id,
                task_id=self.request.id,
                result={
                    "node_id": node["id"],
                    "transformation": plan.explanation,
                    "rows_before": rows_before,
                    "rows_after": rows_after,
                }
            )
        except Exception as broadcast_error:
            logger.warning(f"Failed to broadcast event: {broadcast_error}")

        return {
            "success": True,
            "node_id": node["id"],
            "transformation": plan.explanation,
            "transformation_type": str(plan.transformation_type),
            "rows_before": rows_before,
            "rows_after": rows_after,
            "rows_affected": abs(rows_before - rows_after),
            "code": code,
            "validation": {
                "passed": test_result.passed,
                "summary": test_result.summary,
            }
        }

    except Exception as e:
        logger.exception(f"Task failed: {e}")

        # Broadcast task_failed event
        try:
            from app.websocket.broadcast import publish_task_failed
            publish_task_failed(
                session_id=session_id,
                task_id=self.request.id,
                error=str(e),
            )
        except Exception:
            pass

        return {
            "success": False,
            "error": str(e),
        }


# =============================================================================
# Single Transformation Task
# =============================================================================

@shared_task(bind=True, name="workers.tasks.apply_transformation")
def apply_transformation(
    self,
    session_id: str,
    plan_dict: dict[str, Any],
) -> dict[str, Any]:
    """
    Apply a single transformation (already planned).

    Used when the Strategist has already run and we just need to execute.

    Args:
        session_id: The session UUID
        plan_dict: Serialized TechnicalPlan

    Returns:
        Dict with transformation results
    """
    logger.info(f"Applying transformation for session {session_id}")

    try:
        from agents.models.technical_plan import TechnicalPlan
        from agents.engineer import EngineerAgent
        from core.services.storage_service import StorageService
        from lib.supabase_client import SupabaseClient

        # Reconstruct plan
        plan = TechnicalPlan.model_validate(plan_dict)

        # Load data
        current_node = SupabaseClient.fetch_current_node(session_id)
        if not current_node:
            return {"success": False, "error": "No data uploaded"}

        df = StorageService.download_csv(current_node["storage_path"])

        # Execute
        engineer = EngineerAgent()
        result_df, code = engineer.execute_on_dataframe(df, plan)

        return {
            "success": True,
            "rows_before": len(df),
            "rows_after": len(result_df),
            "code": code,
        }

    except Exception as e:
        logger.exception(f"Transformation failed: {e}")
        return {"success": False, "error": str(e)}


# =============================================================================
# Profile Generation Task (for large files)
# =============================================================================

@shared_task(bind=True, name="workers.tasks.generate_profile_async")
def generate_profile_async(
    self,
    session_id: str,
    storage_path: str,
) -> dict[str, Any]:
    """
    Generate profile for a large file in the background.

    Args:
        session_id: The session UUID
        storage_path: Path to CSV in storage

    Returns:
        Dict with profile data
    """
    logger.info(f"Generating profile for {storage_path}")

    try:
        from core.services.storage_service import StorageService
        from lib.profiler import generate_profile

        update_progress(1, 3, "Downloading file...")
        df = StorageService.download_csv(storage_path)

        update_progress(2, 3, "Analyzing data...")
        profile = generate_profile(df)

        update_progress(3, 3, "Complete!")

        return {
            "success": True,
            "profile": profile.model_dump(),
        }

    except Exception as e:
        logger.exception(f"Profile generation failed: {e}")
        return {"success": False, "error": str(e)}


# =============================================================================
# Test Task
# =============================================================================

@shared_task(bind=True, name="workers.tasks.test_task")
def test_task(self, message: str = "Hello") -> dict[str, Any]:
    """
    Simple test task to verify Celery is working.

    Args:
        message: Test message

    Returns:
        Dict with echo and status
    """
    import time

    update_progress(1, 3, "Starting...")
    time.sleep(1)

    update_progress(2, 3, "Processing...")
    time.sleep(1)

    update_progress(3, 3, "Done!")

    return {
        "success": True,
        "echo": message,
        "message": "Celery is working!",
    }


# =============================================================================
# Plan Apply Task
# =============================================================================

@shared_task(bind=True, name="workers.tasks.process_plan_apply")
def process_plan_apply(
    self,
    session_id: str,
    plan_id: str,
    steps: list[dict[str, Any]],
    mode: str = "all",
) -> dict[str, Any]:
    """
    Apply a plan's transformations to create a new data node.

    This executes all transformation steps from a plan and creates
    a new node with the transformed data.

    Args:
        session_id: Session UUID
        plan_id: Plan UUID
        steps: List of transformation step dicts
        mode: "all" (single node) or "one_by_one" (multiple nodes)

    Returns:
        Dict with:
        - success: bool
        - node_id: New node UUID (if successful)
        - transformations_applied: Count
        - rows_before: Row count before
        - rows_after: Row count after
    """
    logger.info(f"Applying plan {plan_id} for session {session_id}: {len(steps)} steps")

    try:
        from lib.supabase_client import SupabaseClient
        from core.services.storage_service import StorageService
        from core.services.node_service import NodeService
        from core.services.session_service import SessionService
        from core.services.plan_service import PlanService
        from lib.profiler import generate_profile
        from agents.engineer import EngineerAgent
        from agents.models.technical_plan import TechnicalPlan, TransformationType, ColumnTarget

        # Step 1: Load current data
        update_progress(1, 4, "Loading data...")

        current_node = SupabaseClient.fetch_current_node(session_id)
        if not current_node:
            return {
                "success": False,
                "error": "No data uploaded for this session",
            }

        df = StorageService.download_csv(current_node["storage_path"])
        rows_before = len(df)

        # Step 2: Execute transformations
        update_progress(2, 4, "Applying transformations...")

        engineer = EngineerAgent()
        all_code = []
        all_explanations = []

        for step in steps:
            # Convert step dict to TechnicalPlan
            try:
                trans_type = TransformationType(step["transformation_type"])
            except ValueError:
                trans_type = TransformationType.CUSTOM

            plan = TechnicalPlan(
                transformation_type=trans_type,
                target_columns=[
                    ColumnTarget(column_name=col)
                    for col in step.get("target_columns", [])
                ],
                parameters=step.get("parameters", {}),
                explanation=step.get("explanation", "Transformation"),
            )

            # Execute transformation
            df, code = engineer.execute_on_dataframe(df, plan)
            all_code.append(code)
            all_explanations.append(step.get("explanation", ""))

        rows_after = len(df)

        # Step 3: Save new node
        update_progress(3, 4, "Saving results...")

        # Generate node_id first for unique storage path
        import uuid
        new_node_id = str(uuid.uuid4())

        # Upload transformed CSV with unique path
        storage_path = StorageService.upload_csv(
            session_id=session_id,
            df=df,
            node_id=new_node_id,
        )

        # Generate profile
        profile = generate_profile(df)

        # Create new node with short label for display
        short_label = create_short_node_label(steps)
        combined_code = "\n".join(all_code)

        node = NodeService.create_node(
            session_id=session_id,
            parent_id=current_node["id"],
            storage_path=storage_path,
            row_count=len(df),
            column_count=len(df.columns),
            profile_json=profile.model_dump(),
            transformation=short_label,
            transformation_code=combined_code,
            preview_rows=_safe_serialize_rows(df, 10),
            node_id=new_node_id,
            step_descriptions=all_explanations,
        )

        # Update session
        SessionService.update_session(
            session_id=session_id,
            current_node_id=node["id"],
        )

        # Revert to draft if module was deployed (editing a deployed module)
        SessionService.revert_to_draft(session_id)

        # Mark plan as applied (skip if plan doesn't exist - e.g., Transform mode)
        try:
            PlanService.mark_applied(plan_id, node["id"])
        except Exception as mark_error:
            # Plan may not exist if this is a direct Transform mode execution
            logger.debug(f"Could not mark plan as applied (may be Transform mode): {mark_error}")

        # Step 4: Done
        update_progress(4, 4, "Complete!")

        logger.info(f"Plan applied: {len(steps)} transformations, created node {node['id']}")

        # Save completion message to chat
        row_diff = rows_after - rows_before
        if row_diff == 0:
            row_summary = "Row count unchanged."
        elif row_diff > 0:
            row_summary = f"Added {row_diff} rows."
        else:
            row_summary = f"Removed {abs(row_diff)} rows."

        completion_message = f"Done! Applied {len(steps)} transformation{'s' if len(steps) > 1 else ''}. {row_summary}\n\nWhat else would you like to do with your data?"

        SupabaseClient.insert_chat_message(
            session_id=session_id,
            role="assistant",
            content=completion_message,
            node_id=node["id"],
            metadata={
                "transformations_applied": len(steps),
                "rows_before": rows_before,
                "rows_after": rows_after,
            },
        )

        # Broadcast events to WebSocket clients
        try:
            from app.websocket.broadcast import publish_node_created, publish_task_complete

            publish_node_created(
                session_id=session_id,
                node_id=node["id"],
                transformation=short_label,
                row_count=rows_after,
                column_count=len(df.columns),
            )

            publish_task_complete(
                session_id=session_id,
                task_id=self.request.id,
                result={
                    "node_id": node["id"],
                    "transformations_applied": len(steps),
                    "rows_before": rows_before,
                    "rows_after": rows_after,
                }
            )
        except Exception as broadcast_error:
            logger.warning(f"Failed to broadcast event: {broadcast_error}")

        return {
            "success": True,
            "node_id": node["id"],
            "transformations_applied": len(steps),
            "rows_before": rows_before,
            "rows_after": rows_after,
            "rows_affected": abs(rows_before - rows_after),
        }

    except Exception as e:
        logger.exception(f"Plan apply failed: {e}")

        # Broadcast task_failed event
        try:
            from app.websocket.broadcast import publish_task_failed
            publish_task_failed(
                session_id=session_id,
                task_id=self.request.id,
                error=str(e),
            )
        except Exception:
            pass

        return {
            "success": False,
            "error": str(e),
        }
