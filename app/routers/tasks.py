# =============================================================================
# app/routers/tasks.py - Task Status Endpoints
# =============================================================================
# Provides endpoints for checking background task status and results.
# =============================================================================

import logging
from typing import Annotated

from fastapi import APIRouter, Path, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()


# =============================================================================
# Response Models
# =============================================================================

class TaskStatusResponse(BaseModel):
    """Response model for task status."""
    task_id: str
    status: str
    progress: int | None = None
    message: str | None = None
    result: dict | None = None
    error: str | None = None


class TaskSubmitResponse(BaseModel):
    """Response model for task submission."""
    task_id: str
    status: str
    message: str


# =============================================================================
# Endpoints
# =============================================================================

@router.get("/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(
    task_id: Annotated[str, Path(description="Celery task ID")]
):
    """
    Get the status of a background task.

    Returns the current state of the task:
    - PENDING: Task is waiting in queue
    - STARTED: Task has been picked up by a worker
    - PROGRESS: Task is running (includes progress percentage)
    - SUCCESS: Task completed successfully
    - FAILURE: Task failed

    For PROGRESS state, includes:
    - progress: Percentage complete (0-100)
    - message: Current step description

    For SUCCESS state, includes:
    - result: Task result data

    For FAILURE state, includes:
    - error: Error message
    """
    try:
        from workers.celery_app import celery_app

        # Get task result
        result = celery_app.AsyncResult(task_id)

        response = TaskStatusResponse(
            task_id=task_id,
            status=result.status,
        )

        # Add details based on state
        if result.status == "PROGRESS":
            info = result.info or {}
            response.progress = info.get("percent", 0)
            response.message = info.get("message", "Processing...")

        elif result.status == "SUCCESS":
            response.result = result.result
            response.progress = 100
            response.message = "Complete"

        elif result.status == "FAILURE":
            response.error = str(result.result) if result.result else "Unknown error"
            response.message = "Failed"

        elif result.status == "PENDING":
            response.progress = 0
            response.message = "Waiting in queue..."

        elif result.status == "STARTED":
            response.progress = 0
            response.message = "Starting..."

        return response

    except Exception as e:
        logger.error(f"Error getting task status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get task status: {e}")


@router.get("/{task_id}/result")
async def get_task_result(
    task_id: Annotated[str, Path(description="Celery task ID")]
):
    """
    Get the result of a completed task.

    Only returns data if task status is SUCCESS.
    For other states, returns status info.
    """
    try:
        from workers.celery_app import celery_app

        result = celery_app.AsyncResult(task_id)

        if result.status == "SUCCESS":
            return {
                "task_id": task_id,
                "status": "SUCCESS",
                "result": result.result,
            }

        elif result.status == "FAILURE":
            return {
                "task_id": task_id,
                "status": "FAILURE",
                "error": str(result.result) if result.result else "Unknown error",
            }

        else:
            return {
                "task_id": task_id,
                "status": result.status,
                "message": "Task not yet complete",
            }

    except Exception as e:
        logger.error(f"Error getting task result: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get task result: {e}")


@router.delete("/{task_id}")
async def cancel_task(
    task_id: Annotated[str, Path(description="Celery task ID")]
):
    """
    Cancel a pending or running task.

    Only works for tasks that haven't completed yet.
    """
    try:
        from workers.celery_app import celery_app

        result = celery_app.AsyncResult(task_id)

        if result.status in ["SUCCESS", "FAILURE"]:
            return {
                "task_id": task_id,
                "message": f"Task already {result.status.lower()}, cannot cancel",
                "cancelled": False,
            }

        # Revoke the task
        result.revoke(terminate=True)

        return {
            "task_id": task_id,
            "message": "Task cancelled",
            "cancelled": True,
        }

    except Exception as e:
        logger.error(f"Error cancelling task: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to cancel task: {e}")


@router.post("/test", response_model=TaskSubmitResponse)
async def submit_test_task(message: str = "Hello from API"):
    """
    Submit a test task to verify Celery is working.

    Returns a task_id that can be used to check status.
    """
    try:
        from workers.tasks import test_task

        result = test_task.delay(message)

        return TaskSubmitResponse(
            task_id=result.id,
            status="PENDING",
            message="Test task submitted. Use GET /api/v1/tasks/{task_id} to check status.",
        )

    except Exception as e:
        logger.error(f"Error submitting test task: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Failed to submit task. Is Redis running? Error: {e}"
        )
