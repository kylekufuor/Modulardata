# =============================================================================
# app/websocket/routes.py - WebSocket Routes
# =============================================================================
# WebSocket endpoint for real-time session updates.
#
# Connect: ws://host/ws/sessions/{session_id}?token={jwt}
#
# Events:
#   - {"type": "node_created", "node_id": "...", ...}
#   - {"type": "task_complete", "task_id": "...", "status": "SUCCESS", ...}
#   - {"type": "task_failed", "task_id": "...", "error": "..."}
# =============================================================================

import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from jose import jwt, JWTError

from app.config import settings
from app.websocket.manager import websocket_manager
from lib.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/ws/sessions/{session_id}")
async def session_websocket(
    websocket: WebSocket,
    session_id: str,
    token: str = Query(..., description="JWT token for authentication")
):
    """
    WebSocket endpoint for real-time session updates.

    Authentication is required via the `token` query parameter.
    User must own the session to connect.

    Connection URL:
        ws://localhost:8000/ws/sessions/{session_id}?token={jwt}

    Events received:
        - node_created: New transformation node was created
        - task_complete: Background task completed successfully
        - task_failed: Background task failed

    Example event:
        {
            "type": "node_created",
            "node_id": "550e8400-...",
            "transformation": "Remove nulls from email column",
            "row_count": 95,
            "column_count": 6
        }
    """
    # 1. Verify JWT token
    try:
        payload = jwt.decode(
            token,
            settings.SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            audience="authenticated"
        )
        user_id = payload.get("sub")

        if not user_id:
            logger.warning("WebSocket auth failed: missing user ID in token")
            await websocket.close(code=4001, reason="Invalid token: missing user ID")
            return

    except JWTError as e:
        logger.warning(f"WebSocket auth failed: {e}")
        await websocket.close(code=4001, reason="Invalid token")
        return

    # 2. Verify user owns this session
    try:
        session = SupabaseClient.fetch_session(session_id)

        if not session:
            logger.warning(f"WebSocket: session {session_id} not found")
            await websocket.close(code=4004, reason="Session not found")
            return

        if str(session.get("user_id")) != user_id:
            logger.warning(
                f"WebSocket access denied: user {user_id} "
                f"tried to access session owned by {session.get('user_id')}"
            )
            await websocket.close(code=4003, reason="Access denied")
            return

    except Exception as e:
        logger.error(f"WebSocket: error fetching session: {e}")
        await websocket.close(code=4000, reason="Server error")
        return

    # 3. Accept connection and add to manager
    await websocket_manager.connect(session_id, websocket)

    try:
        # Send welcome message
        await websocket.send_json({
            "type": "connected",
            "session_id": session_id,
            "message": "Connected to session updates"
        })

        # Keep connection alive and handle incoming messages
        while True:
            try:
                # Wait for messages from client
                data = await websocket.receive_text()

                # Handle ping/pong for keepalive
                if data == "ping":
                    await websocket.send_text("pong")
                else:
                    # Could handle other client messages here
                    logger.debug(f"WebSocket received: {data[:100]}")

            except WebSocketDisconnect:
                raise
            except Exception as e:
                logger.warning(f"WebSocket receive error: {e}")
                break

    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected from session {session_id}")
    finally:
        websocket_manager.disconnect(session_id, websocket)


@router.get("/ws/status")
async def websocket_status():
    """
    Get WebSocket connection statistics.

    Returns:
        dict: Connection counts and active sessions
    """
    return {
        "total_connections": websocket_manager.get_connection_count(),
        "active_sessions": websocket_manager.get_active_sessions(),
        "session_count": len(websocket_manager.get_active_sessions())
    }
