# =============================================================================
# core/ - Business Logic Package
# =============================================================================
# This package contains framework-agnostic business logic:
# - models/: Pydantic schemas for data validation
# - database.py: Supabase client wrapper and queries
# - storage.py: File upload/download utilities
#
# Code in this package should NOT import from FastAPI or Celery.
# This keeps the logic testable and reusable.
# =============================================================================
