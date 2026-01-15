# Milestone 6: Async Infrastructure - Redis & Celery

**Status:** ✅ Complete
**Roadmap Reference:** Section 6 - "Async Infrastructure"

---

## Overview

Set up the background job processing system so heavy AI transformations don't block the API. Users get immediate responses with a task ID, then poll for results.

**Goal:** A working task queue where the API can offload work to background workers.

---

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   FastAPI   │────▶│    Redis    │────▶│   Celery    │
│   (API)     │     │   (Queue)   │     │  (Worker)   │
└─────────────┘     └─────────────┘     └─────────────┘
      │                                        │
      │         ┌─────────────┐               │
      └────────▶│  Supabase   │◀──────────────┘
                │ (Database)  │
                └─────────────┘
```

**Flow:**
1. API receives request → Creates task in Redis → Returns task_id immediately
2. Celery Worker picks up task from Redis
3. Worker processes (runs AI agents, transforms data)
4. Worker saves result to Supabase
5. User polls `GET /task/{id}` to check status

---

## Components

### 1. Redis
- **Purpose:** Message broker (task queue)
- **Location:** Local for development, Railway for production
- **Config:** `REDIS_URL` in `.env`

### 2. Celery
- **Purpose:** Distributed task queue
- **Workers:** Run separately from API
- **Tasks:** AI transformations, data processing

### 3. Task Status
- **Storage:** Celery result backend (Redis)
- **States:** PENDING → STARTED → SUCCESS/FAILURE
- **Endpoint:** `GET /api/v1/tasks/{task_id}`

---

## Files to Create

| File | Description |
|------|-------------|
| `workers/__init__.py` | Worker package |
| `workers/celery_app.py` | Celery application configuration |
| `workers/tasks.py` | Task definitions |
| `workers/config.py` | Worker-specific settings |
| `app/routers/tasks.py` | Task status endpoints |
| `scripts/run_worker.py` | Worker entry point |

---

## Implementation Steps

### Step 1: Celery Configuration
- Create Celery app with Redis broker
- Configure result backend
- Set up task serialization

### Step 2: Task Definitions
- Create base task class with error handling
- Define `process_chat_message` task
- Define `apply_transformation` task

### Step 3: Task Status Endpoints
- `GET /api/v1/tasks/{task_id}` - Get task status
- `GET /api/v1/tasks/{task_id}/result` - Get task result

### Step 4: Worker Entry Point
- Create script to run Celery worker
- Configure logging and concurrency

### Step 5: Testing
- Test task submission
- Test task status polling
- Test worker processing

---

## Task States

| State | Description |
|-------|-------------|
| `PENDING` | Task waiting in queue |
| `STARTED` | Worker picked up task |
| `PROGRESS` | Task running (with progress %) |
| `SUCCESS` | Task completed successfully |
| `FAILURE` | Task failed with error |

---

## API Contract

### Submit Task (Future - Milestone 7)
```
POST /api/v1/sessions/{id}/chat
{
    "message": "remove rows where email is null"
}

Response:
{
    "task_id": "abc-123",
    "status": "PENDING",
    "message": "Task queued for processing"
}
```

### Check Task Status
```
GET /api/v1/tasks/{task_id}

Response:
{
    "task_id": "abc-123",
    "status": "PROGRESS",
    "progress": 50,
    "message": "Applying transformation..."
}
```

### Get Task Result
```
GET /api/v1/tasks/{task_id}/result

Response:
{
    "task_id": "abc-123",
    "status": "SUCCESS",
    "result": {
        "node_id": "new-node-uuid",
        "rows_affected": 150,
        "transformation": "Dropped 150 rows where email was null"
    }
}
```

---

## Verification

```bash
# Terminal 1: Start Redis (if local)
redis-server

# Terminal 2: Start Celery Worker
poetry run celery -A workers.celery_app worker --loglevel=info

# Terminal 3: Start API
poetry run uvicorn app.main:app --reload

# Terminal 4: Test
curl -X POST http://localhost:8000/api/v1/tasks/test
```

---

## Dependencies

```toml
# Already in pyproject.toml
celery = "^5.3.0"
redis = "^5.0.0"
```

---

## Environment Variables

```bash
# .env
REDIS_URL=redis://localhost:6379/0

# For Railway production
REDIS_URL=redis://default:password@redis.railway.internal:6379
```

---

## Quick Start Guide

### 1. Install Redis (macOS)
```bash
brew install redis

# Start Redis
brew services start redis

# Verify it's running
redis-cli ping  # Should return "PONG"
```

### 2. Add REDIS_URL to .env
```bash
echo "REDIS_URL=redis://localhost:6379/0" >> .env
```

### 3. Start the Worker
```bash
# Option 1: Using poetry script
poetry run start-worker

# Option 2: Using Celery CLI directly
poetry run celery -A workers.celery_app worker --loglevel=info
```

### 4. Test the Connection
```bash
# In another terminal, start the API
poetry run uvicorn app.main:app --reload

# Submit a test task
curl -X POST "http://localhost:8000/api/v1/tasks/test?message=Hello"

# Response:
# {"task_id":"abc-123","status":"PENDING","message":"Test task submitted..."}

# Check task status
curl http://localhost:8000/api/v1/tasks/abc-123
```

---

## Files Created

| File | Description |
|------|-------------|
| `workers/__init__.py` | Worker package exports |
| `workers/celery_app.py` | Celery application with Redis broker |
| `workers/config.py` | Celery configuration settings |
| `workers/tasks.py` | Task definitions (process_chat_message, test_task) |
| `app/routers/tasks.py` | Task status API endpoints |
| `scripts/start_worker.py` | Worker entry point script |
