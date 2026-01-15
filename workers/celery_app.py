# =============================================================================
# workers/celery_app.py - Celery Application Configuration
# =============================================================================
# This module creates and configures the Celery application instance.
#
# Usage:
#   # Start worker
#   celery -A workers.celery_app worker --loglevel=info
#
#   # Check status
#   celery -A workers.celery_app status
# =============================================================================

import logging
import os
import sys

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from celery import Celery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_celery_app() -> Celery:
    """
    Create and configure Celery application.

    Returns:
        Configured Celery app instance
    """
    # Get Redis URL from environment
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Create Celery app
    app = Celery(
        "modulardata_worker",
        broker=redis_url,
        backend=redis_url,
        include=["workers.tasks"],  # Auto-discover tasks
    )

    # Load configuration
    app.config_from_object("workers.config:CeleryConfig")

    # Log startup
    logger.info(f"Celery app created with broker: {redis_url.split('@')[-1] if '@' in redis_url else redis_url}")

    return app


# Create the Celery app instance
celery_app = create_celery_app()


# =============================================================================
# Celery Signals (Lifecycle Hooks)
# =============================================================================

@celery_app.task(bind=True, name="workers.healthcheck")
def healthcheck(self):
    """
    Simple healthcheck task to verify worker is running.

    Usage:
        from workers.celery_app import healthcheck
        result = healthcheck.delay()
        print(result.get(timeout=5))  # Should return "OK"
    """
    return "OK"


# Log when tasks are received
from celery.signals import task_prerun, task_postrun, task_failure


@task_prerun.connect
def task_prerun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, **extra):
    """Log when a task starts."""
    logger.info(f"Task started: {task.name} [{task_id}]")


@task_postrun.connect
def task_postrun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, **extra):
    """Log when a task completes."""
    logger.info(f"Task completed: {task.name} [{task_id}] - State: {state}")


@task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None, traceback=None, **extra):
    """Log when a task fails."""
    logger.error(f"Task failed: {sender.name} [{task_id}] - Error: {exception}")


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    celery_app.start()
