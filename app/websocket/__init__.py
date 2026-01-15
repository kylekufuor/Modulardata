# =============================================================================
# app/websocket/__init__.py - WebSocket Module
# =============================================================================
# Provides real-time updates for session events.
#
# Usage:
#   # Broadcast an event to all connections for a session (from FastAPI)
#   from app.websocket import websocket_manager
#
#   await websocket_manager.broadcast(session_id, {
#       "type": "node_created",
#       "node_id": "..."
#   })
#
#   # Publish events from Celery workers
#   from app.websocket.broadcast import publish_node_created
#
#   publish_node_created(session_id, node_id, transformation, row_count, column_count)
# =============================================================================

from app.websocket.manager import websocket_manager
from app.websocket.broadcast import (
    publish_event,
    publish_node_created,
    publish_task_complete,
    publish_task_failed,
    WEBSOCKET_CHANNEL,
)

__all__ = [
    "websocket_manager",
    "publish_event",
    "publish_node_created",
    "publish_task_complete",
    "publish_task_failed",
    "WEBSOCKET_CHANNEL",
]
