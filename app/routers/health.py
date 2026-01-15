# =============================================================================
# app/routers/health.py - Health Check Endpoints
# =============================================================================
# Provides health check endpoints for monitoring and load balancers.
# =============================================================================

from datetime import datetime

from fastapi import APIRouter
from pydantic import BaseModel

from app.config import settings

router = APIRouter()


# =============================================================================
# Response Models
# =============================================================================

class HealthResponse(BaseModel):
    """Basic health check response."""
    status: str
    timestamp: str
    environment: str
    version: str


class ChecksResponse(BaseModel):
    """Individual service checks."""
    database: str
    storage: str


class ReadinessResponse(BaseModel):
    """Readiness check response."""
    status: str
    checks: ChecksResponse
    timestamp: str


class LivenessResponse(BaseModel):
    """Liveness check response."""
    status: str
    timestamp: str


# =============================================================================
# Endpoints
# =============================================================================

@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.

    Returns basic health status for load balancers and monitoring.
    """
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat(),
        environment=settings.ENVIRONMENT,
        version="1.0.0",
    )


@router.get("/health/ready", response_model=ReadinessResponse)
async def readiness_check():
    """
    Readiness check endpoint.

    Returns whether the service is ready to accept requests.
    Checks database and storage connectivity.
    """
    from lib.supabase_client import SupabaseClient

    checks = ChecksResponse(database="unknown", storage="unknown")

    # Check database
    try:
        client = SupabaseClient.get_client()
        client.table("sessions").select("id").limit(1).execute()
        checks.database = "healthy"
    except Exception as e:
        checks.database = f"unhealthy: {str(e)[:50]}"

    # Check storage
    try:
        client = SupabaseClient.get_client()
        client.storage.list_buckets()
        checks.storage = "healthy"
    except Exception as e:
        checks.storage = f"unhealthy: {str(e)[:50]}"

    # Overall status
    all_healthy = checks.database == "healthy" and checks.storage == "healthy"

    return ReadinessResponse(
        status="ready" if all_healthy else "degraded",
        checks=checks,
        timestamp=datetime.utcnow().isoformat(),
    )


@router.get("/health/live", response_model=LivenessResponse)
async def liveness_check():
    """
    Liveness check endpoint.

    Returns whether the service process is alive.
    Used by Kubernetes/Docker for restart decisions.
    """
    return LivenessResponse(
        status="alive",
        timestamp=datetime.utcnow().isoformat(),
    )
