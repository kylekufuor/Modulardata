# =============================================================================
# workers/config.py - Celery Worker Configuration
# =============================================================================
# Settings specific to Celery workers.
# =============================================================================

from app.config import settings


class CeleryConfig:
    """
    Celery configuration settings.

    These are applied to the Celery app via app.config_from_object().
    """

    # -------------------------------------------------------------------------
    # Broker Settings (Redis)
    # -------------------------------------------------------------------------

    # Redis URL for message broker
    broker_url = settings.REDIS_URL

    # Redis URL for result backend (store task results)
    result_backend = settings.REDIS_URL

    # -------------------------------------------------------------------------
    # Task Settings
    # -------------------------------------------------------------------------

    # Acknowledge tasks after they complete (not before)
    # This prevents task loss if worker crashes mid-task
    task_acks_late = True

    # Only prefetch one task at a time
    # This ensures fair distribution across workers
    worker_prefetch_multiplier = 1

    # Task results expire after 1 hour
    result_expires = 3600

    # Default task timeout (5 minutes)
    task_time_limit = 300

    # Soft timeout (4 minutes) - gives task time to clean up
    task_soft_time_limit = 240

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    # Use JSON for task serialization (safer than pickle)
    task_serializer = "json"
    result_serializer = "json"
    accept_content = ["json"]

    # -------------------------------------------------------------------------
    # Task Routing
    # -------------------------------------------------------------------------

    # Define task queues
    task_queues = {
        "default": {
            "exchange": "default",
            "routing_key": "default",
        },
        "ai_tasks": {
            "exchange": "ai_tasks",
            "routing_key": "ai_tasks",
        },
    }

    # Route AI tasks to dedicated queue
    task_routes = {
        "workers.tasks.process_chat_message": {"queue": "ai_tasks"},
        "workers.tasks.apply_transformation": {"queue": "ai_tasks"},
    }

    # Default queue for unrouted tasks
    task_default_queue = "default"

    # -------------------------------------------------------------------------
    # Retry Settings
    # -------------------------------------------------------------------------

    # Retry failed tasks up to 3 times
    task_annotations = {
        "*": {
            "max_retries": 3,
            "default_retry_delay": 60,  # Wait 60 seconds between retries
        }
    }

    # -------------------------------------------------------------------------
    # Monitoring
    # -------------------------------------------------------------------------

    # Send task events for monitoring (Flower, etc.)
    worker_send_task_events = True
    task_send_sent_event = True

    # -------------------------------------------------------------------------
    # Timezone
    # -------------------------------------------------------------------------

    timezone = "UTC"
    enable_utc = True
