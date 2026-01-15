# =============================================================================
# Procfile - Process definitions for deployment
# =============================================================================
# Used by Railway, Heroku, and other PaaS platforms.
#
# Railway Setup:
#   1. Deploy this repo as "API" service with: web process
#   2. Deploy again as "Worker" service with: worker process
#   3. Add Redis service from Railway's marketplace
#
# Environment Variables Required:
#   - SUPABASE_URL
#   - SUPABASE_KEY
#   - SUPABASE_SERVICE_KEY
#   - OPENAI_API_KEY
#   - REDIS_URL (auto-set by Railway when linking Redis)
# =============================================================================

# Web API Server
web: uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}

# Celery Worker
worker: celery -A workers.celery_app worker --loglevel=info --concurrency=${CELERY_CONCURRENCY:-4}
