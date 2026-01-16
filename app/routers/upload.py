# =============================================================================
# app/routers/upload.py - File Upload Pipeline
# =============================================================================
# Handles CSV file uploads with validation, profiling, and storage.
# =============================================================================

import io
import logging
from typing import Annotated, Any
from uuid import UUID

import numpy as np
import pandas as pd
from fastapi import APIRouter, File, UploadFile, Path, Depends

from app.auth import get_current_user, AuthUser
from app.config import settings
from app.exceptions import (
    InvalidFileTypeError,
    FileTooLargeError,
    FileReadError,
    NoDataError,
)
from core.services.session_service import SessionService
from core.services.storage_service import StorageService
from core.services.node_service import NodeService
from lib.profiler import generate_profile, read_csv_safe

logger = logging.getLogger(__name__)

router = APIRouter()


# =============================================================================
# Helper Functions
# =============================================================================

def _sanitize_value(value: Any) -> Any:
    """Convert numpy/pandas types to JSON-serializable Python types."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if pd.isna(value):
        return None
    if isinstance(value, (np.integer, np.int64, np.int32)):
        return int(value)
    if isinstance(value, (np.floating, np.float64, np.float32)):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, (np.ndarray, pd.Series)):
        return value.tolist()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, 'item'):  # Generic numpy scalar
        return value.item()
    return value


def _sanitize_preview_rows(rows: list[dict]) -> list[dict]:
    """Sanitize all values in preview rows for JSON serialization."""
    return [
        {k: _sanitize_value(v) for k, v in row.items()}
        for row in rows
    ]


def _build_welcome_message(filename: str, profile: Any, row_count: int) -> str:
    """Build a card-style welcome message for the chat."""
    # Count total missing values
    total_missing = sum(col.null_count for col in profile.columns if col.null_count > 0)
    columns_with_issues = [col for col in profile.columns if col.null_count > 0]
    columns_with_issues.sort(key=lambda c: c.null_count, reverse=True)

    msg = f"📊 {filename} loaded!\n\n"
    msg += f"   {row_count:,} rows  •  {profile.column_count} columns\n\n"

    if columns_with_issues:
        top_issue = columns_with_issues[0]
        msg += f"⚠️ Found {total_missing} missing values across {len(columns_with_issues)} columns\n"
        msg += f"   → {top_issue.name} has the most ({top_issue.null_count} missing)\n\n"
        msg += "How can I help clean this data?"
    else:
        msg += "✅ Data looks clean!\n\n"
        msg += "What would you like to do with it?"

    return msg


# =============================================================================
# Endpoints
# =============================================================================

@router.post("/{session_id}/upload")
async def upload_file(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    file: Annotated[UploadFile, File(description="CSV file to upload")],
    user: AuthUser = Depends(get_current_user),
):
    """
    Upload a CSV file to a session.

    This endpoint:
    1. Validates the file (extension, size)
    2. Reads and parses the CSV
    3. Generates a data profile
    4. Uploads to Supabase Storage
    5. Creates Node 0 (the root version)
    6. Updates the session with the current node

    Returns the session info with the data profile.
    User must own the session.
    """
    session_id_str = str(session_id)

    # Verify session exists and user owns it
    SessionService.get_session(session_id_str, user_id=user.id)

    # =============================================================================
    # 1. Validate File
    # =============================================================================

    # Check file extension
    filename = file.filename or "data.csv"
    file_ext = "." + filename.split(".")[-1].lower() if "." in filename else ""

    if file_ext not in settings.allowed_extensions_list:
        raise InvalidFileTypeError(filename, settings.allowed_extensions_list)

    # Read file content
    content = await file.read()
    file_size_bytes = len(content)
    file_size_mb = file_size_bytes / (1024 * 1024)

    # Check file size
    if file_size_bytes > settings.max_upload_size_bytes:
        raise FileTooLargeError(file_size_mb, settings.MAX_UPLOAD_SIZE_MB)

    logger.info(f"Processing upload: {filename} ({file_size_mb:.2f}MB)")

    # =============================================================================
    # 2. Read and Parse CSV
    # =============================================================================

    try:
        # Use the robust CSV reader from profiler
        csv_buffer = io.BytesIO(content)
        # read_csv_safe returns (DataFrame, encoding, delimiter)
        df, detected_encoding, detected_delimiter = read_csv_safe(csv_buffer)

        if df.empty:
            raise FileReadError(filename, "File is empty or contains no data")

        logger.info(f"Parsed CSV: {len(df)} rows × {len(df.columns)} columns")

    except FileReadError:
        raise
    except Exception as e:
        raise FileReadError(filename, str(e))

    # =============================================================================
    # 3. Generate Profile
    # =============================================================================

    try:
        profile = generate_profile(df)
        profile_dict = profile.model_dump()

        # Add file metadata
        profile_dict["file_size_bytes"] = file_size_bytes

        logger.info(f"Generated profile with {len(profile.issues)} issues")

    except Exception as e:
        logger.error(f"Profiling failed: {e}")
        raise FileReadError(filename, f"Failed to analyze file: {e}")

    # =============================================================================
    # 4. Upload to Storage
    # =============================================================================

    storage_path = StorageService.upload_csv(
        session_id=session_id_str,
        df=df,
        filename="original.csv",
    )

    logger.info(f"Uploaded to storage: {storage_path}")

    # =============================================================================
    # 5. Create Node 0
    # =============================================================================

    # Get preview rows (first 10) - sanitize for JSON serialization
    preview_rows = _sanitize_preview_rows(df.head(10).to_dict(orient="records"))

    node = NodeService.create_node(
        session_id=session_id_str,
        parent_id=None,  # Root node has no parent
        storage_path=storage_path,
        row_count=len(df),
        column_count=len(df.columns),
        profile_json=profile_dict,
        transformation=None,  # Original upload has no transformation
        transformation_code=None,
        preview_rows=preview_rows,
    )

    logger.info(f"Created Node 0: {node['id']}")

    # =============================================================================
    # 6. Update Session
    # =============================================================================

    SessionService.update_session(
        session_id=session_id_str,
        current_node_id=node["id"],
        original_filename=filename,
    )

    # =============================================================================
    # 6b. Save Welcome Message to Chat Logs
    # =============================================================================

    try:
        from lib.supabase_client import SupabaseClient

        # Build a simple welcome message that will be displayed in chat
        welcome_message = _build_welcome_message(filename, profile, len(df))
        SupabaseClient.insert_chat_message(
            session_id=session_id_str,
            role="assistant",
            content=welcome_message,
            node_id=node["id"],
            metadata={"type": "welcome", "filename": filename},
        )
        logger.debug(f"Saved welcome message for session {session_id_str}")
    except Exception as e:
        logger.warning(f"Failed to save welcome message: {e}")

    # =============================================================================
    # 7. Return Response
    # =============================================================================

    # Build simplified profile for response - sanitize all values for JSON
    column_summaries = []
    for col in profile.columns:
        column_summaries.append({
            "name": col.name,
            "dtype": col.dtype,
            "semantic_type": col.semantic_type.value if hasattr(col.semantic_type, 'value') else str(col.semantic_type),
            "null_count": int(col.null_count) if col.null_count is not None else 0,
            "null_percent": float(col.null_percent) if col.null_percent is not None else 0.0,
            "unique_count": int(col.unique_count) if col.unique_count is not None else 0,
        })

    # Extract issues as strings
    issues_list = []
    for issue in profile.issues:
        issues_list.append(issue.description if hasattr(issue, 'description') else str(issue))

    # Add column-level issues
    for col in profile.columns:
        for issue in col.issues:
            issues_list.append(f"[{col.name}] {issue.description if hasattr(issue, 'description') else str(issue)}")

    return {
        "session_id": session_id_str,
        "node_id": node["id"],
        "filename": filename,
        "storage_path": storage_path,
        "profile": {
            "row_count": int(profile.row_count) if profile.row_count is not None else 0,
            "column_count": int(profile.column_count) if profile.column_count is not None else 0,
            "columns": column_summaries,
            "issues": issues_list[:20],  # Limit to 20 issues in response
            "duplicate_row_count": int(profile.duplicate_row_count) if profile.duplicate_row_count is not None else 0,
        },
        "preview": preview_rows[:5],  # First 5 rows for quick view
    }
