# =============================================================================
# Dockerfile - ModularData API
# =============================================================================
# Multi-stage build for the FastAPI application.
# Optimized for production with minimal image size.
#
# Build: docker build -t modulardata-api .
# Run:   docker run -p 8000:8000 --env-file .env modulardata-api
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Builder - Install dependencies
# -----------------------------------------------------------------------------
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install --no-cache-dir poetry==1.7.1

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Configure Poetry: no virtualenv, install deps to system
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --only main

# -----------------------------------------------------------------------------
# Stage 2: Runtime - Final image
# -----------------------------------------------------------------------------
FROM python:3.11-slim as runtime

WORKDIR /app

# Create non-root user for security
RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid 1000 --shell /bin/bash appuser

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY --chown=appuser:appuser . .

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health/live || exit 1

# Default command - run API server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
