# ModularData Architecture

> **Last Updated:** 2026-01-15
> **Version:** 1.0.0

This document describes the technical architecture of ModularData.

---

## System Overview

ModularData is an AI-powered data transformation API built on a modern async Python stack.

```
┌─────────────────────────────────────────────────────────────────────┐
│                           Clients                                    │
│                    (Postman, SDKs, Web UI)                          │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     FastAPI Web Service                              │
│                   (Railway: web service)                             │
│  ┌───────────┬───────────┬───────────┬───────────┬───────────┐     │
│  │  Sessions │  Upload   │   Chat    │   Data    │  History  │     │
│  │  Router   │  Router   │  Router   │  Router   │  Router   │     │
│  └───────────┴───────────┴───────────┴───────────┴───────────┘     │
└─────────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌────────────────┐  ┌────────────────┐  ┌────────────────────────────┐
│   Supabase     │  │     Redis      │  │    Celery Worker           │
│  (PostgreSQL   │  │  (Task Queue)  │  │   (Railway: worker)        │
│  + Storage)    │  │                │  │                            │
│                │  │                │  │  ┌────────────────────┐   │
│  - sessions    │  │  - broker      │  │  │  Strategist AI     │   │
│  - nodes       │  │  - backend     │  │  │  (Plan creation)   │   │
│  - chat_logs   │  │                │  │  └─────────┬──────────┘   │
│  - plans       │  │                │  │            │              │
│  - CSV files   │  │                │  │  ┌────────────────────┐   │
│                │  │                │  │  │  Engineer          │   │
└────────────────┘  └────────────────┘  │  │  (Pandas executor) │   │
                                        │  └────────────────────┘   │
                                        └────────────────────────────┘
```

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Web Framework | FastAPI 0.109 | Async REST API with auto OpenAPI docs |
| Task Queue | Celery 5.3 | Background job processing |
| Message Broker | Redis 5.0 | Celery broker + result backend |
| Database | Supabase PostgreSQL | Sessions, nodes, chat logs, plans |
| File Storage | Supabase Storage | CSV file storage |
| AI/LLM | OpenAI GPT-4 Turbo | Natural language understanding |
| Data Processing | Pandas 2.1 | DataFrame transformations |
| Deployment | Railway | Container hosting |

---

## Core Components

### 1. FastAPI Application (`app/`)

The web service layer handling HTTP requests.

```
app/
├── main.py           # FastAPI app initialization
├── config.py         # Environment configuration (pydantic-settings)
├── exceptions.py     # Custom exception classes
├── dependencies.py   # FastAPI dependency injection
└── routers/
    ├── health.py     # /api/v1/health/*
    ├── sessions.py   # /api/v1/sessions/*
    ├── upload.py     # CSV upload handling
    ├── data.py       # Data access/download
    ├── chat.py       # AI transformation interface
    ├── history.py    # Version control/rollback
    └── tasks.py      # Task status tracking
```

**Key design decisions:**
- All routers are async for non-blocking I/O
- Heavy work (transformations) offloaded to Celery workers
- CORS configured for frontend integration

### 2. Celery Workers (`workers/`)

Background task processing for AI operations.

```
workers/
├── celery_app.py     # Celery app configuration
├── config.py         # Worker-specific settings
└── tasks.py          # Task definitions
```

**Why Celery?**
- AI transformations can take 5-30 seconds
- Prevents HTTP timeout issues
- Enables horizontal scaling
- Provides task retry and monitoring

### 3. AI Agents (`agents/`)

The intelligence layer that interprets and executes transformations.

```
agents/
├── strategist.py     # Context Strategist - interprets user intent
├── engineer.py       # Engineer - executes pandas code
├── chat_handler.py   # Orchestrates agent communication
├── models/
│   ├── technical_plan.py    # Transformation plan schema
│   └── execution_result.py  # Result schema
├── prompts/
│   └── strategist_system.py # AI system prompts
├── transformations/         # 53 transformation implementations
│   ├── registry.py          # Decorator-based registry
│   ├── cleaning.py          # Data cleaning ops
│   ├── column_math.py       # Numeric operations
│   ├── filtering.py         # Row filtering
│   ├── restructuring.py     # Pivot, melt, etc.
│   ├── aggregation.py       # Group by, rank
│   ├── string_ops.py        # Text operations
│   ├── date_ops.py          # Date calculations
│   ├── validation.py        # Data quality checks
│   └── advanced_ops.py      # Complex operations
└── quality_checks/          # Data profiling rules
```

**Agent Pipeline:**

```
User Message → Strategist AI → Technical Plan → Engineer → Transformed Data
                    │                                           │
                    └── Uses: column names, data types,         │
                              sample values, prior context      │
                                                                ▼
                                                        New Node Created
```

### 4. Core Services (`core/`)

Domain logic and data persistence.

```
core/
├── models/
│   ├── session.py    # Session Pydantic models
│   ├── node.py       # Node version models
│   ├── plan.py       # Transformation plan models
│   └── profile.py    # Data profile models
└── services/
    ├── session_service.py   # Session CRUD
    ├── node_service.py      # Node/version management
    ├── plan_service.py      # Plan queue management
    └── storage_service.py   # File storage operations
```

### 5. Library (`lib/`)

Shared utilities.

```
lib/
├── supabase_client.py  # Typed Supabase wrapper
├── profiler.py         # CSV data profiling
├── memory.py           # Context memory management
└── utils.py            # General utilities
```

---

## Data Flow

### Upload Flow

```
1. Client → POST /sessions/{id}/upload (CSV file)
2. Upload Router → StorageService.upload_file()
3. StorageService → Supabase Storage (save file)
4. Profiler → Analyze CSV (types, nulls, issues)
5. NodeService → Create initial node with profile
6. SessionService → Update session.current_node_id
7. Response → Profile + preview to client
```

### Transformation Flow

```
1. Client → POST /sessions/{id}/chat (message)
2. Chat Router → ChatHandler.handle_message()
3. ChatHandler → Build context (messages, profile, node history)
4. Strategist AI → Generate technical plan
5. PlanService → Store plan in database
6. Response → Plan + explanation to client

[When client calls /plan/apply]

7. Client → POST /sessions/{id}/plan/apply
8. Apply Router → tasks.apply_plan.delay() (async)
9. Response → task_id to client

[In Celery Worker]

10. Worker → Fetch plan from database
11. Engineer → Execute each transformation
12. NodeService → Create new node with transformed data
13. StorageService → Save new CSV
14. Task Result → Success + statistics
```

### Rollback Flow

```
1. Client → POST /sessions/{id}/rollback (target_node_id)
2. History Router → NodeService.rollback()
3. NodeService → Verify target node exists
4. SessionService → Update current_node_id to target
5. Response → Confirmation + new state
```

---

## Database Schema

See [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md) for full schema.

**Key Tables:**
- `sessions` - Workspace containers
- `nodes` - Version snapshots (linked list via parent_id)
- `chat_logs` - Conversation history
- `plans` - Queued transformations
- `plan_steps` - Individual transformation steps

---

## Scaling Considerations

### Current Limits

| Resource | Limit | Notes |
|----------|-------|-------|
| File size | 50 MB | Configurable via MAX_UPLOAD_SIZE_MB |
| Row count | ~100K | Memory-bound, needs streaming for larger |
| Workers | 1 | Single Railway worker instance |

### Future Scaling

1. **Horizontal Worker Scaling**
   - Add more Celery workers on Railway
   - Redis handles task distribution automatically

2. **Large File Handling**
   - Implement chunked processing
   - Use Dask for out-of-memory DataFrames

3. **Caching**
   - Redis cache for repeated operations
   - Profile caching to avoid re-analysis

---

## Security Model

### Current State

- No authentication (public API)
- Service role key bypasses Supabase RLS
- CORS restricted in production

### Planned

- API key authentication
- Per-user session isolation
- Rate limiting
- Audit logging

---

## Error Handling

All errors follow a consistent format:

```json
{
    "detail": "Human-readable error message",
    "code": "ERROR_CODE"
}
```

Custom exceptions in `app/exceptions.py` map to HTTP status codes.

---

## Monitoring

### Health Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/api/v1/health` | Basic liveness |
| `/api/v1/health/ready` | DB + storage connectivity |
| `/api/v1/health/live` | Simple alive check |

### Logging

- Structured logging via Python logging
- Log levels: DEBUG (dev), INFO (prod)
- Task logs in Celery worker output

---

## Related Documentation

- [Development Setup](DEV_SETUP.md)
- [Deployment Guide](DEPLOYMENT.md)
- [Environment Variables](ENV_VARIABLES.md)
- [Database Schema](DATABASE_SCHEMA.md)
