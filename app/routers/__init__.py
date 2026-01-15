# =============================================================================
# app/routers/ - API Route Definitions
# =============================================================================
# This package contains FastAPI routers organized by feature:
# - health.py: Health check endpoints
# - sessions.py: Session creation and management endpoints
# - upload.py: File upload and processing endpoints
# - data.py: Data access and download endpoints
# - tasks.py: Background task status endpoints
# - chat.py: Conversational AI interaction endpoints (Plan Mode)
# - history.py: Version history and rollback endpoints (Milestone 8)
#
# Each router is mounted in main.py with a URL prefix.
# =============================================================================

from . import health
from . import sessions
from . import upload
from . import data
from . import tasks
from . import chat
from . import history

__all__ = [
    "health",
    "sessions",
    "upload",
    "data",
    "tasks",
    "chat",
    "history",
]
