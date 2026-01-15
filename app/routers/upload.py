# =============================================================================
# app/routers/upload.py - File Upload Pipeline
# =============================================================================
# Handles CSV file uploads with validation, profiling, and storage.
# =============================================================================

import io
import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, File, UploadFile, Path

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
# Endpoints
# =============================================================================

@router.post("/{session_id}/upload")
async def upload_file(
    session_id: Annotated[UUID, Path(description="Session UUID")],
    file: Annotated[UploadFile, File(description="CSV file to upload")],
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
    """
    session_id_str = str(session_id)

    # Verify session exists
    SessionService.get_session(session_id_str)

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

    # Get preview rows (first 10)
    preview_rows = df.head(10).to_dict(orient="records")

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
    # 7. Return Response
    # =============================================================================

    # Build simplified profile for response
    column_summaries = []
    for col in profile.columns:
        column_summaries.append({
            "name": col.name,
            "dtype": col.dtype,
            "semantic_type": col.semantic_type.value if hasattr(col.semantic_type, 'value') else col.semantic_type,
            "null_count": col.null_count,
            "null_percent": col.null_percent,
            "unique_count": col.unique_count,
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
            "row_count": profile.row_count,
            "column_count": profile.column_count,
            "columns": column_summaries,
            "issues": issues_list[:20],  # Limit to 20 issues in response
            "duplicate_row_count": profile.duplicate_row_count,
        },
        "preview": preview_rows[:5],  # First 5 rows for quick view
    }
