# =============================================================================
# app/main.py - FastAPI Application Entry Point
# =============================================================================
# This is the main entry point for the ModularData API.
# It configures the FastAPI application with middleware, routers, and handlers.
#
# Usage:
#   poetry run uvicorn app.main:app --reload
# =============================================================================

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.exceptions import (
    ModularDataException,
    modulardata_exception_handler,
    validation_exception_handler,
)
from app.routers import health, sessions, upload, data, tasks, chat, history

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.

    Runs on startup and shutdown:
    - Startup: Initialize connections, validate config
    - Shutdown: Clean up resources
    """
    # Startup
    logger.info(f"Starting ModularData API in {settings.ENVIRONMENT} mode")
    logger.info(f"CORS origins: {settings.cors_origins_list}")

    yield

    # Shutdown
    logger.info("Shutting down ModularData API")


# Create FastAPI application
app = FastAPI(
    title="ModularData API",
    description="""
## AI-Powered Data Transformation API

ModularData uses a 3-agent pipeline to transform your CSV data through natural language instructions.

### How It Works

1. **Create a Session** - Start a new data transformation session
2. **Upload CSV** - Upload your data file (processed asynchronously)
3. **Chat to Transform** - Describe transformations in plain English
4. **Apply Changes** - Execute the planned transformations
5. **Export Results** - Download your cleaned data

### Agent Pipeline

| Agent | Role |
|-------|------|
| **Advisor** | Analyzes data and provides profile/recommendations |
| **Strategist** | Converts natural language to technical plans |
| **Engineer** | Executes pandas transformations |

### Key Features

- **Plan Mode**: Preview and batch transformations before applying
- **Version History**: Full undo/redo with node-based versioning
- **Async Processing**: Background task queue for large datasets
- **Data Profiling**: Automatic column statistics and quality analysis

### Quick Start

```bash
# 1. Create session
curl -X POST http://localhost:8000/api/v1/sessions

# 2. Upload CSV
curl -X POST http://localhost:8000/api/v1/sessions/{id}/upload \\
  -F "file=@data.csv"

# 3. Chat to transform
curl -X POST http://localhost:8000/api/v1/sessions/{id}/chat \\
  -H "Content-Type: application/json" \\
  -d '{"message": "remove rows where email is blank"}'

# 4. Apply plan
curl -X POST http://localhost:8000/api/v1/sessions/{id}/plan/apply
```
""",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
    openapi_tags=[
        {
            "name": "Sessions",
            "description": "Create and manage data transformation sessions",
        },
        {
            "name": "Upload",
            "description": "Upload CSV files for transformation",
        },
        {
            "name": "Data",
            "description": "Access data, profiles, and export results",
        },
        {
            "name": "Chat",
            "description": "Natural language data transformation interface",
        },
        {
            "name": "Tasks",
            "description": "Track async task progress",
        },
        {
            "name": "History",
            "description": "Version history, undo/redo, and rollback",
        },
        {
            "name": "Health",
            "description": "API health and readiness checks",
        },
    ],
)


# =============================================================================
# Middleware
# =============================================================================

# CORS middleware - allows cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list if settings.is_production else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Exception Handlers
# =============================================================================

@app.exception_handler(ModularDataException)
async def handle_modulardata_exception(request: Request, exc: ModularDataException):
    """Handle custom ModularData exceptions."""
    return await modulardata_exception_handler(request, exc)


@app.exception_handler(Exception)
async def handle_general_exception(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    logger.exception(f"Unexpected error: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "detail": "An unexpected error occurred",
            "code": "INTERNAL_ERROR",
        }
    )


# =============================================================================
# Routers
# =============================================================================

# Health check endpoints
app.include_router(
    health.router,
    prefix="/api/v1",
    tags=["Health"]
)

# Session management endpoints
app.include_router(
    sessions.router,
    prefix="/api/v1/sessions",
    tags=["Sessions"]
)

# File upload endpoints
app.include_router(
    upload.router,
    prefix="/api/v1/sessions",
    tags=["Upload"]
)

# Data access endpoints
app.include_router(
    data.router,
    prefix="/api/v1/sessions",
    tags=["Data"]
)

# Task status endpoints
app.include_router(
    tasks.router,
    prefix="/api/v1/tasks",
    tags=["Tasks"]
)

# Chat endpoints (Plan Mode)
app.include_router(
    chat.router,
    prefix="/api/v1/sessions",
    tags=["Chat"]
)

# History endpoints (Version Control)
app.include_router(
    history.router,
    prefix="/api/v1/sessions",
    tags=["History"]
)


# =============================================================================
# Root Endpoint
# =============================================================================

@app.get("/", tags=["Root"])
async def root():
    """
    Root endpoint - returns API info.
    """
    return {
        "name": "ModularData API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/v1/health",
    }
