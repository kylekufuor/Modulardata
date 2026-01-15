# =============================================================================
# workers/ - Celery Background Task Workers
# =============================================================================
# This package contains the Celery configuration and task definitions for
# background processing of AI transformations.
#
# Components:
# - celery_app.py: Celery application configuration
# - tasks.py: Task definitions (chat processing, transformations)
# - config.py: Worker-specific settings
#
# Usage:
#   # Start worker
#   celery -A workers.celery_app worker --loglevel=info
#
#   # Or use the poetry script
#   poetry run start-worker
#
#   # Submit task (from API)
#   from workers.tasks import process_chat_message
#   result = process_chat_message.delay(session_id, message)
# =============================================================================

from .celery_app import celery_app
from . import tasks

__all__ = [
    "celery_app",
    "tasks",
]
