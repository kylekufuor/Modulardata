# =============================================================================
# app/routers/feedback.py - User Feedback API
# =============================================================================
# Handles user feedback (thumbs up/down) for AI transformations.
# This data helps identify common issues and improve the AI.
# =============================================================================

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import logging

from lib.supabase_client import SupabaseClient

router = APIRouter(prefix="/api/v1/feedback", tags=["feedback"])
logger = logging.getLogger(__name__)


class FeedbackCreate(BaseModel):
    """Request body for submitting feedback."""
    session_id: str
    message_id: str
    rating: str  # "positive" or "negative"
    comment: Optional[str] = None
    node_id: Optional[str] = None
    transformation_type: Optional[str] = None


class FeedbackResponse(BaseModel):
    """Response after submitting feedback."""
    id: str
    message: str


@router.post("", response_model=FeedbackResponse)
async def submit_feedback(feedback: FeedbackCreate):
    """
    Submit user feedback for an AI response.

    - **rating**: "positive" (thumbs up) or "negative" (thumbs down)
    - **comment**: Optional explanation (typically for negative feedback)
    - **transformation_type**: Type of transformation that was performed
    """
    # Validate rating
    if feedback.rating not in ("positive", "negative"):
        raise HTTPException(
            status_code=400,
            detail="Rating must be 'positive' or 'negative'"
        )

    try:
        client = SupabaseClient.get_client()

        data = {
            "session_id": feedback.session_id,
            "message_id": feedback.message_id,
            "rating": feedback.rating,
            "comment": feedback.comment,
            "node_id": feedback.node_id,
            "transformation_type": feedback.transformation_type,
        }

        # Remove None values
        data = {k: v for k, v in data.items() if v is not None}

        result = client.table("feedback").insert(data).execute()

        if not result.data:
            raise HTTPException(status_code=500, detail="Failed to save feedback")

        feedback_id = result.data[0]["id"]

        logger.info(
            f"Feedback submitted: {feedback.rating} for session {feedback.session_id}"
            + (f" - {feedback.comment}" if feedback.comment else "")
        )

        return FeedbackResponse(
            id=feedback_id,
            message="Thank you for your feedback!"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to submit feedback: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_feedback_stats():
    """
    Get aggregated feedback statistics.

    Returns counts of positive/negative feedback by transformation type.
    Useful for identifying common issues.
    """
    try:
        client = SupabaseClient.get_client()

        # Get all feedback
        result = client.table("feedback").select("*").execute()

        if not result.data:
            return {
                "total": 0,
                "positive": 0,
                "negative": 0,
                "by_type": {},
                "recent_issues": []
            }

        feedback_list = result.data

        # Aggregate stats
        total = len(feedback_list)
        positive = sum(1 for f in feedback_list if f["rating"] == "positive")
        negative = sum(1 for f in feedback_list if f["rating"] == "negative")

        # Group by transformation type
        by_type = {}
        for f in feedback_list:
            t_type = f.get("transformation_type") or "unknown"
            if t_type not in by_type:
                by_type[t_type] = {"positive": 0, "negative": 0}
            by_type[t_type][f["rating"]] += 1

        # Get recent negative feedback with comments
        recent_issues = [
            {
                "transformation_type": f.get("transformation_type"),
                "comment": f.get("comment"),
                "created_at": f.get("created_at")
            }
            for f in sorted(feedback_list, key=lambda x: x.get("created_at", ""), reverse=True)
            if f["rating"] == "negative" and f.get("comment")
        ][:10]  # Last 10 issues

        return {
            "total": total,
            "positive": positive,
            "negative": negative,
            "satisfaction_rate": round(positive / total * 100, 1) if total > 0 else 0,
            "by_type": by_type,
            "recent_issues": recent_issues
        }

    except Exception as e:
        logger.exception(f"Failed to get feedback stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
