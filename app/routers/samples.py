# =============================================================================
# app/routers/samples.py - Sample Data Endpoints
# =============================================================================
# Provides sample datasets for users to test the platform without their own data.
# =============================================================================

import json
import logging
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, HTTPException, Path as PathParam
from fastapi.responses import FileResponse

logger = logging.getLogger(__name__)

router = APIRouter()

# Path to sample data directory
SAMPLE_DATA_DIR = Path(__file__).parent.parent.parent / "sample_data"


@router.get("/samples")
async def list_samples():
    """
    List all available sample datasets.

    Returns metadata about each sample including description and issues.
    """
    index_path = SAMPLE_DATA_DIR / "index.json"

    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Sample data index not found")

    try:
        with open(index_path) as f:
            data = json.load(f)
        return data
    except Exception as e:
        logger.error(f"Failed to load sample index: {e}")
        raise HTTPException(status_code=500, detail="Failed to load sample data")


@router.get("/samples/{sample_id}")
async def get_sample(
    sample_id: Annotated[str, PathParam(description="Sample dataset ID")]
):
    """
    Get a specific sample dataset file.

    Returns the CSV file for the requested sample.
    """
    index_path = SAMPLE_DATA_DIR / "index.json"

    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Sample data index not found")

    try:
        with open(index_path) as f:
            data = json.load(f)

        # Find the sample
        sample = next(
            (s for s in data["samples"] if s["id"] == sample_id),
            None
        )

        if not sample:
            raise HTTPException(status_code=404, detail=f"Sample not found: {sample_id}")

        # Get the file path
        file_path = SAMPLE_DATA_DIR / sample["filename"]

        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"Sample file not found: {sample['filename']}")

        return FileResponse(
            path=file_path,
            media_type="text/csv",
            filename=sample["filename"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get sample: {e}")
        raise HTTPException(status_code=500, detail="Failed to get sample data")


@router.get("/samples/{sample_id}/content")
async def get_sample_content(
    sample_id: Annotated[str, PathParam(description="Sample dataset ID")]
):
    """
    Get sample dataset content as text.

    Returns the raw CSV content for preview or direct upload.
    """
    index_path = SAMPLE_DATA_DIR / "index.json"

    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Sample data index not found")

    try:
        with open(index_path) as f:
            data = json.load(f)

        # Find the sample
        sample = next(
            (s for s in data["samples"] if s["id"] == sample_id),
            None
        )

        if not sample:
            raise HTTPException(status_code=404, detail=f"Sample not found: {sample_id}")

        # Get the file content
        file_path = SAMPLE_DATA_DIR / sample["filename"]

        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"Sample file not found: {sample['filename']}")

        with open(file_path) as f:
            content = f.read()

        return {
            "id": sample["id"],
            "name": sample["name"],
            "filename": sample["filename"],
            "content": content,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get sample content: {e}")
        raise HTTPException(status_code=500, detail="Failed to get sample data")
