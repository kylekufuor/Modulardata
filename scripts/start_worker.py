#!/usr/bin/env python3
# =============================================================================
# scripts/start_worker.py - Celery Worker Entry Point
# =============================================================================
# Starts a Celery worker to process background tasks.
#
# Usage:
#   # Start worker (development)
#   poetry run python scripts/start_worker.py
#
#   # Or use Celery CLI directly
#   poetry run celery -A workers.celery_app worker --loglevel=info
#
#   # Start with concurrency limit
#   poetry run celery -A workers.celery_app worker --loglevel=info --concurrency=4
#
# Prerequisites:
#   - Redis must be running (brew services start redis)
#   - Environment variables must be set (.env file)
# =============================================================================

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.celery_app import celery_app


def main():
    """Start the Celery worker."""
    print("=" * 60)
    print("ModularData Celery Worker")
    print("=" * 60)
    print()
    print("Starting worker...")
    print("Press Ctrl+C to stop")
    print()

    # Start worker with info logging
    celery_app.worker_main([
        "worker",
        "--loglevel=info",
        "--concurrency=2",  # 2 worker processes
    ])


if __name__ == "__main__":
    main()
