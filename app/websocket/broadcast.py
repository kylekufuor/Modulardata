# =============================================================================
# app/websocket/broadcast.py - Cross-Process Broadcasting
# =============================================================================
# Provides utilities for Celery workers to publish events that get broadcast
# to WebSocket clients.
#
# Uses Redis pub/sub for cross-process communication:
# - Workers call publish_event() to send events
# - FastAPI subscribes and broadcasts to WebSocket clients
#
# Events:
#   - node_created: A new data node was created
#   - task_complete: A background task completed successfully
#   - task_failed: A background task failed
# =============================================================================

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Redis channel for WebSocket events
WEBSOCKET_CHANNEL = "modulardata:websocket:events"


def get_redis_client():
    """Get a Redis client for pub/sub operations."""
    import redis
    from app.config import settings
    return redis.from_url(settings.REDIS_URL)


def publish_event(session_id: str, event_type: str, data: dict[str, Any]) -> bool:
    """
    Publish an event that will be broadcast to WebSocket clients.

    This is called from Celery workers to notify the WebSocket server
    of events that should be broadcast to connected clients.

    Args:
        session_id: The session to broadcast to
        event_type: Event type (node_created, task_complete, task_failed)
        data: Event data to include

    Returns:
        bool: True if published successfully
    """
    try:
        client = get_redis_client()

        message = json.dumps({
            "session_id": session_id,
            "type": event_type,
            **data
        })

        # Publish to Redis channel
        client.publish(WEBSOCKET_CHANNEL, message)

        logger.debug(f"Published {event_type} event for session {session_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to publish event: {e}")
        return False


def publish_node_created(
    session_id: str,
    node_id: str,
    transformation: str,
    row_count: int,
    column_count: int,
) -> bool:
    """
    Publish a node_created event.

    Called when a new transformation node is created.
    """
    return publish_event(
        session_id=session_id,
        event_type="node_created",
        data={
            "node_id": node_id,
            "transformation": transformation,
            "row_count": row_count,
            "column_count": column_count,
        }
    )


def publish_task_complete(
    session_id: str,
    task_id: str,
    result: dict[str, Any],
) -> bool:
    """
    Publish a task_complete event.

    Called when a background task completes successfully.
    """
    return publish_event(
        session_id=session_id,
        event_type="task_complete",
        data={
            "task_id": task_id,
            "status": "SUCCESS",
            "result": result,
        }
    )


def publish_task_failed(
    session_id: str,
    task_id: str,
    error: str,
) -> bool:
    """
    Publish a task_failed event.

    Called when a background task fails.
    """
    return publish_event(
        session_id=session_id,
        event_type="task_failed",
        data={
            "task_id": task_id,
            "status": "FAILURE",
            "error": error,
        }
    )
