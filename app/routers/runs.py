# =============================================================================
# app/routers/runs.py - Module Run Endpoints
# =============================================================================
# Handles running modules on new data:
# - POST /sessions/{id}/run - Upload and run
# - GET /sessions/{id}/runs - List run history
# - GET /sessions/{id}/runs/{run_id} - Get run details
# - GET /sessions/{id}/runs/{run_id}/download - Download output
# =============================================================================

import io
import logging
from typing import Annotated
from uuid import UUID

import pandas as pd
from fastapi import APIRouter, Path, Query, Depends, File, UploadFile, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.auth import get_current_user, AuthUser
from core.services.module_run_service import ModuleRunService
from core.services.session_service import SessionService
from core.services.storage_service import StorageService

logger = logging.getLogger(__name__)

router = APIRouter()


# =============================================================================
# Response Models
# =============================================================================

class RunResponse(BaseModel):
    """Response for a module run."""
    run_id: str = Field(..., description="Unique run identifier")
    status: str = Field(..., description="Run status: success, failed, pending, warning_confirmed")
    confidence_score: float = Field(..., description="Schema match confidence (0-100)")
    confidence_level: str = Field(..., description="HIGH, MEDIUM, LOW, or NO_MATCH")
    input_rows: int | None = Field(None, description="Number of input rows")
    input_columns: int | None = Field(None, description="Number of input columns")
    output_rows: int | None = Field(None, description="Number of output rows")
    output_columns: int | None = Field(None, description="Number of output columns")
    error_message: str | None = Field(None, description="Error message if failed")
    requires_confirmation: bool = Field(False, description="True if user must confirm")
    column_mappings: list[dict] | None = Field(None, description="How columns were mapped")
    discrepancies: list[dict] | None = Field(None, description="Schema issues found")
    output_storage_path: str | None = Field(None, description="Path to output file")
    duration_ms: int | None = Field(None, description="Total run duration in ms")
    message: str | None = Field(None, description="User-facing message")


class RunListItem(BaseModel):
    """Item in run list response."""
    run_id: str
    status: str
    confidence_score: float
    confidence_level: str
    input_filename: str
    input_row_count: int
    output_row_count: int | None
    created_at: str
    duration_ms: int | None


class RunListResponse(BaseModel):
    """Response for listing runs."""
    runs: list[RunListItem]
    total: int


class RunDetailResponse(BaseModel):
    """Detailed run information."""
    run_id: str
    session_id: str
    status: str
    created_at: str

    # Input info
    input_filename: str
    input_row_count: int
    input_column_count: int
    input_storage_path: str | None

    # Schema matching
    confidence_score: float
    confidence_level: str
    column_mappings: list[dict] | None
    discrepancies: list[dict] | None

    # Output info
    output_row_count: int | None
    output_column_count: int | None
    output_storage_path: str | None

    # Error info
    error_message: str | None
    error_details: dict | None

    # Timing
    duration_ms: int | None
    timing_breakdown: dict | None


# =============================================================================
# Endpoints
# =============================================================================

@router.post("/{session_id}/run", response_model=RunResponse)
async def run_module(
    session_id: Annotated[UUID, Path(description="Module (session) UUID")],
    file: UploadFile = File(..., description="CSV file to process"),
    force: Annotated[bool, Query(description="Force run even with MEDIUM confidence")] = False,
    user: AuthUser = Depends(get_current_user),
):
    """
    Run a module on new data.

    Upload a CSV file and run the module's transformations on it.

    The module will:
    1. Profile the incoming file
    2. Match columns against the module's expected schema
    3. If compatible, replay all transformations
    4. Return the transformed output

    **Confidence Levels:**
    - **HIGH (≥85%)**: Automatically processes
    - **MEDIUM (60-84%)**: Requires confirmation or force=true
    - **LOW (40-59%)**: Rejected - schema too different
    - **NO_MATCH (<40%)**: Rejected - file doesn't match at all
    """
    session_id_str = str(session_id)

    # Verify session exists and user owns it
    session = SessionService.get_session(session_id_str, user_id=str(user.id))

    # Check session has data and is deployed-ready
    if not session.get("current_node_id"):
        raise HTTPException(
            status_code=400,
            detail="Module has no data. Upload and transform data first."
        )

    # Read uploaded file
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        logger.error(f"Failed to read uploaded file: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Failed to read CSV file: {str(e)}"
        )

    if df.empty:
        raise HTTPException(
            status_code=400,
            detail="Uploaded file is empty"
        )

    # Run the module
    try:
        result = ModuleRunService.run_module(
            session_id=session_id_str,
            user_id=str(user.id),
            df=df,
            filename=file.filename or "uploaded.csv",
            force=force,
        )
    except Exception as e:
        logger.error(f"Module run failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Module run failed: {str(e)}"
        )

    return RunResponse(
        run_id=result["run_id"],
        status=result["status"],
        confidence_score=result["confidence_score"],
        confidence_level=result["confidence_level"],
        input_rows=result.get("input_rows") or len(df),
        input_columns=result.get("input_columns") or len(df.columns),
        output_rows=result.get("output_rows"),
        output_columns=result.get("output_columns"),
        error_message=result.get("error_message"),
        requires_confirmation=result.get("requires_confirmation", False),
        column_mappings=result.get("column_mappings"),
        discrepancies=result.get("discrepancies"),
        output_storage_path=result.get("output_storage_path"),
        duration_ms=result.get("duration_ms"),
        message=result.get("message"),
    )


@router.get("/{session_id}/runs", response_model=RunListResponse)
async def list_runs(
    session_id: Annotated[UUID, Path(description="Module (session) UUID")],
    limit: Annotated[int, Query(ge=1, le=100, description="Max runs to return")] = 50,
    offset: Annotated[int, Query(ge=0, description="Offset for pagination")] = 0,
    user: AuthUser = Depends(get_current_user),
):
    """
    List run history for a module.

    Returns runs sorted by creation time (newest first).
    """
    session_id_str = str(session_id)

    # Verify ownership
    SessionService.get_session(session_id_str, user_id=str(user.id))

    runs = ModuleRunService.list_runs(
        session_id=session_id_str,
        limit=limit,
        offset=offset,
    )

    return RunListResponse(
        runs=[
            RunListItem(
                run_id=r["id"],
                status=r["status"],
                confidence_score=r["confidence_score"],
                confidence_level=r["confidence_level"],
                input_filename=r["input_filename"],
                input_row_count=r["input_row_count"],
                output_row_count=r.get("output_row_count"),
                created_at=r["created_at"],
                duration_ms=r.get("duration_ms"),
            )
            for r in runs
        ],
        total=len(runs),  # TODO: Add proper count query
    )


@router.get("/{session_id}/runs/{run_id}", response_model=RunDetailResponse)
async def get_run_detail(
    session_id: Annotated[UUID, Path(description="Module (session) UUID")],
    run_id: Annotated[UUID, Path(description="Run UUID")],
    user: AuthUser = Depends(get_current_user),
):
    """
    Get detailed information about a specific run.

    Includes full schema matching results, timing breakdown, and error details.
    """
    session_id_str = str(session_id)
    run_id_str = str(run_id)

    # Verify ownership
    SessionService.get_session(session_id_str, user_id=str(user.id))

    run = ModuleRunService.get_run(run_id_str)

    if not run:
        raise HTTPException(
            status_code=404,
            detail="Run not found"
        )

    # Verify run belongs to this session
    if run["session_id"] != session_id_str:
        raise HTTPException(
            status_code=404,
            detail="Run not found for this module"
        )

    return RunDetailResponse(
        run_id=run["id"],
        session_id=run["session_id"],
        status=run["status"],
        created_at=run["created_at"],
        input_filename=run["input_filename"],
        input_row_count=run["input_row_count"],
        input_column_count=run["input_column_count"],
        input_storage_path=run.get("input_storage_path"),
        confidence_score=run["confidence_score"],
        confidence_level=run["confidence_level"],
        column_mappings=run.get("column_mappings"),
        discrepancies=run.get("discrepancies"),
        output_row_count=run.get("output_row_count"),
        output_column_count=run.get("output_column_count"),
        output_storage_path=run.get("output_storage_path"),
        error_message=run.get("error_message"),
        error_details=run.get("error_details"),
        duration_ms=run.get("duration_ms"),
        timing_breakdown=run.get("timing_breakdown"),
    )


@router.get("/{session_id}/runs/{run_id}/download")
async def download_run_output(
    session_id: Annotated[UUID, Path(description="Module (session) UUID")],
    run_id: Annotated[UUID, Path(description="Run UUID")],
    user: AuthUser = Depends(get_current_user),
):
    """
    Download the output file from a successful run.

    Returns the transformed CSV file.
    """
    session_id_str = str(session_id)
    run_id_str = str(run_id)

    # Verify ownership
    SessionService.get_session(session_id_str, user_id=str(user.id))

    run = ModuleRunService.get_run(run_id_str)

    if not run:
        raise HTTPException(
            status_code=404,
            detail="Run not found"
        )

    # Verify run belongs to this session
    if run["session_id"] != session_id_str:
        raise HTTPException(
            status_code=404,
            detail="Run not found for this module"
        )

    # Check run was successful
    if run["status"] not in ("success", "warning_confirmed"):
        raise HTTPException(
            status_code=400,
            detail=f"Run did not complete successfully. Status: {run['status']}"
        )

    # Get output storage path
    output_path = run.get("output_storage_path")
    if not output_path:
        raise HTTPException(
            status_code=404,
            detail="Output file not found"
        )

    # Download from storage
    try:
        content = StorageService.download_raw(output_path)
    except Exception as e:
        logger.error(f"Failed to download run output: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to download output file"
        )

    # Generate filename
    input_filename = run.get("input_filename", "data")
    if input_filename.endswith(".csv"):
        input_filename = input_filename[:-4]
    output_filename = f"{input_filename}_transformed.csv"

    return StreamingResponse(
        io.BytesIO(content),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{output_filename}"'
        }
    )


@router.post("/{session_id}/runs/{run_id}/confirm", response_model=RunResponse)
async def confirm_run(
    session_id: Annotated[UUID, Path(description="Module (session) UUID")],
    run_id: Annotated[UUID, Path(description="Run UUID")],
    file: UploadFile = File(..., description="CSV file to process (must match original upload)"),
    user: AuthUser = Depends(get_current_user),
):
    """
    Confirm a pending run with MEDIUM confidence and execute it.

    When a run has MEDIUM confidence, the user must explicitly confirm.
    Re-upload the same file to proceed with the transformation.
    """
    session_id_str = str(session_id)
    run_id_str = str(run_id)

    # Verify ownership
    SessionService.get_session(session_id_str, user_id=str(user.id))

    # Get existing run
    run = ModuleRunService.get_run(run_id_str)

    if not run:
        raise HTTPException(
            status_code=404,
            detail="Run not found"
        )

    if run["session_id"] != session_id_str:
        raise HTTPException(
            status_code=404,
            detail="Run not found for this module"
        )

    if run["status"] != "pending":
        raise HTTPException(
            status_code=400,
            detail=f"Run is not pending confirmation. Status: {run['status']}"
        )

    # Read uploaded file
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to read CSV file: {str(e)}"
        )

    # Run with force=True
    try:
        result = ModuleRunService.run_module(
            session_id=session_id_str,
            user_id=str(user.id),
            df=df,
            filename=file.filename or run["input_filename"],
            force=True,
        )
    except Exception as e:
        logger.error(f"Confirmed module run failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Module run failed: {str(e)}"
        )

    return RunResponse(
        run_id=result["run_id"],
        status=result["status"],
        confidence_score=result["confidence_score"],
        confidence_level=result["confidence_level"],
        input_rows=result.get("input_rows"),
        input_columns=result.get("input_columns"),
        output_rows=result.get("output_rows"),
        output_columns=result.get("output_columns"),
        error_message=result.get("error_message"),
        requires_confirmation=False,
        column_mappings=result.get("column_mappings"),
        discrepancies=result.get("discrepancies"),
        output_storage_path=result.get("output_storage_path"),
        duration_ms=result.get("duration_ms"),
        message="Run confirmed and executed successfully.",
    )
