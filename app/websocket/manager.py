# =============================================================================
# app/websocket/manager.py - WebSocket Connection Manager
# =============================================================================
# Manages WebSocket connections per session and handles broadcasting.
#
# Usage:
#   from app.websocket import websocket_manager
#
#   # Connect a client
#   await websocket_manager.connect(session_id, websocket)
#
#   # Broadcast to all clients watching a session
#   await websocket_manager.broadcast(session_id, {"type": "node_created", ...})
#
#   # Disconnect a client
#   websocket_manager.disconnect(session_id, websocket)
# =============================================================================

import logging
from typing import Dict, Set
from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages WebSocket connections organized by session ID.

    Each session can have multiple connected clients (e.g., multiple browser tabs).
    When an event occurs for a session, it's broadcast to all connected clients.
    """

    def __init__(self):
        # session_id -> set of WebSocket connections
        self.connections: Dict[str, Set[WebSocket]] = {}
        # Track connection count for logging
        self._total_connections = 0

    async def connect(self, session_id: str, websocket: WebSocket) -> None:
        """
        Accept a new WebSocket connection and track it.

        Args:
            session_id: The session this connection is watching
            websocket: The WebSocket connection
        """
        await websocket.accept()

        if session_id not in self.connections:
            self.connections[session_id] = set()

        self.connections[session_id].add(websocket)
        self._total_connections += 1

        logger.info(
            f"WebSocket connected to session {session_id}. "
            f"Total connections: {self._total_connections}"
        )

    def disconnect(self, session_id: str, websocket: WebSocket) -> None:
        """
        Remove a WebSocket connection from tracking.

        Args:
            session_id: The session this connection was watching
            websocket: The WebSocket connection to remove
        """
        if session_id in self.connections:
            self.connections[session_id].discard(websocket)
            self._total_connections -= 1

            # Clean up empty session entries
            if not self.connections[session_id]:
                del self.connections[session_id]

        logger.info(
            f"WebSocket disconnected from session {session_id}. "
            f"Total connections: {self._total_connections}"
        )

    async def broadcast(self, session_id: str, message: dict) -> int:
        """
        Broadcast a message to all connections watching a session.

        Args:
            session_id: The session to broadcast to
            message: The message dict to send (will be JSON encoded)

        Returns:
            int: Number of clients the message was sent to
        """
        if session_id not in self.connections:
            logger.debug(f"No connections for session {session_id}, skipping broadcast")
            return 0

        dead_connections: Set[WebSocket] = set()
        sent_count = 0

        for websocket in self.connections[session_id]:
            try:
                await websocket.send_json(message)
                sent_count += 1
            except Exception as e:
                logger.warning(f"Failed to send to WebSocket: {e}")
                dead_connections.add(websocket)

        # Clean up any dead connections
        for ws in dead_connections:
            self.connections[session_id].discard(ws)
            self._total_connections -= 1

        if dead_connections:
            logger.info(f"Cleaned up {len(dead_connections)} dead connections")

        # Clean up empty session entries
        if session_id in self.connections and not self.connections[session_id]:
            del self.connections[session_id]

        logger.debug(
            f"Broadcast to session {session_id}: "
            f"type={message.get('type')}, sent to {sent_count} clients"
        )

        return sent_count

    def get_connection_count(self, session_id: str = None) -> int:
        """
        Get the number of active connections.

        Args:
            session_id: If provided, count for specific session. Otherwise total.

        Returns:
            int: Number of connections
        """
        if session_id:
            return len(self.connections.get(session_id, set()))
        return self._total_connections

    def get_active_sessions(self) -> list[str]:
        """
        Get list of session IDs with active connections.

        Returns:
            list[str]: Session IDs with at least one connection
        """
        return list(self.connections.keys())


# Global singleton instance
websocket_manager = ConnectionManager()
