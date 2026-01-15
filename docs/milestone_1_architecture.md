# Milestone 1: System Architecture & Database Schema

**Status:** ✅ Complete
**Date Completed:** January 2026

---

## Overview

This milestone established the foundation for the ModularData API:
- Database schema in Supabase
- Project structure with modular organization
- Pydantic models defining the API contract
- Development environment with Poetry

---

## What We Built

### 1. Database Schema (Supabase)

Three core tables were created to support "Time Travel" versioning:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  sessions   │────▶│    nodes    │────▶│  chat_logs  │
└─────────────┘     └─────────────┘     └─────────────┘
                          │
                          ▼
                    (self-reference)
                     parent_id
```

| Table | Purpose |
|-------|---------|
| `sessions` | Container for user interaction - one per uploaded CSV |
| `nodes` | Version tree - each transformation creates a new node linked to parent |
| `chat_logs` | Conversation history linked to specific data versions |

**Key design decisions:**
- UUIDs for all primary keys (standard for Supabase)
- `nodes.parent_id` creates a linked-list/tree for version branching
- `sessions.current_node_id` tracks the "active" version
- Soft delete via `status` field (archived vs active)
- JSONB for flexible metadata storage

### 2. Project Structure

```
Modulardata/
├── app/                      # FastAPI application
│   ├── __init__.py
│   ├── config.py            # Environment variable loading
│   └── routers/             # API endpoints (future)
│
├── core/                     # Business logic (framework-agnostic)
│   ├── __init__.py
│   └── models/              # Pydantic schemas
│       ├── __init__.py      # Re-exports all models
│       ├── profile.py       # DataProfile, ColumnProfile
│       ├── session.py       # Session CRUD schemas
│       ├── node.py          # Node/version schemas
│       └── chat.py          # Chat request/response schemas
│
├── agents/                   # CrewAI agents (future)
├── workers/                  # Celery tasks (future)
├── lib/                      # Standalone utilities (future)
├── tests/                    # Test suite
│   └── test_models.py       # 23 model tests
│
├── scripts/
│   └── init_db.sql          # Database schema
│
├── docs/                     # Documentation
├── pyproject.toml           # Poetry dependencies
├── poetry.lock              # Locked versions
├── .env.example             # Environment template
└── .gitignore               # Git ignore rules
```

### 3. Pydantic Models

All API request/response schemas defined with full validation:

| Model | File | Purpose |
|-------|------|---------|
| `DataProfile`, `ColumnProfile` | profile.py | CSV metadata for AI context |
| `SessionCreate`, `SessionResponse` | session.py | Session CRUD operations |
| `NodeCreate`, `NodeResponse`, `RollbackRequest` | node.py | Version tracking |
| `ChatRequest`, `ChatResponse`, `TaskStatus` | chat.py | Async chat operations |

### 4. Configuration

Environment variables loaded via `pydantic-settings`:
- Supabase credentials (URL, anon key, service key)
- Redis URL for Celery
- OpenAI API key for agents
- Application settings (debug, CORS, etc.)

---

## Files Created

| File | Lines | Description |
|------|-------|-------------|
| `pyproject.toml` | 55 | Poetry config with dependencies |
| `.gitignore` | 55 | Git ignore rules |
| `.env.example` | 55 | Environment variable template |
| `scripts/init_db.sql` | 135 | SQL schema with comments |
| `core/models/profile.py` | 145 | Data profile schemas |
| `core/models/session.py` | 130 | Session schemas |
| `core/models/node.py` | 185 | Node/version schemas |
| `core/models/chat.py` | 290 | Chat & task schemas |
| `core/models/__init__.py` | 90 | Model exports |
| `app/config.py` | 140 | Settings class |
| `tests/test_models.py` | 240 | Unit tests |

**Total:** ~1,520 lines of code

---

## Setup Steps Taken

### Step 1: Initialize Poetry Project
```bash
# Created pyproject.toml with dependencies:
# - fastapi, uvicorn, pydantic, pydantic-settings
# - supabase, python-multipart
# - pytest, httpx, pytest-asyncio (dev)
```

### Step 2: Create Folder Structure
```bash
mkdir -p app/routers core/models agents workers lib tests scripts docs
# Created __init__.py in each package with comments explaining purpose
```

### Step 3: Write Database Schema
- Created `scripts/init_db.sql` with:
  - UUID extension enablement
  - Three tables with appropriate columns
  - Foreign key constraints
  - Indexes on frequently queried columns
  - Comments for documentation

### Step 4: Write Pydantic Models
- `profile.py` - DataProfile with `to_text_summary()` method
- `session.py` - SessionCreate, SessionResponse, SessionStatus enum
- `node.py` - NodeCreate, NodeResponse, RollbackRequest/Response
- `chat.py` - ChatRequest, ChatResponse, TaskStatus, TaskState enum
- `__init__.py` - Re-exports all 20+ model classes

### Step 5: Write Configuration
- Created `app/config.py` with Settings class
- Uses `pydantic-settings` to load from `.env`
- Includes computed properties (cors_origins_list, etc.)

### Step 6: Write Tests
- Created `tests/test_models.py` with 23 unit tests
- Tests cover: valid data, defaults, validation errors, serialization

### Step 7: Install Dependencies
```bash
brew install poetry          # Installed Poetry
poetry install               # Installed all dependencies
```

### Step 8: Configure Supabase
1. Created `.env` from `.env.example`
2. Added Supabase URL and API keys
3. Ran `init_db.sql` in Supabase SQL Editor
4. Verified tables in Schema Visualizer

---

## Verification / Testing

### Tests Run
```bash
poetry run pytest tests/test_models.py -v
# Result: 23 passed in 0.07s
```

### Import Verification
```bash
poetry run python -c "from core.models import SessionCreate, NodeResponse, ChatRequest, DataProfile; print('OK')"
# Result: All model imports successful
```

### Database Verification
- Opened Supabase Schema Visualizer
- Confirmed all 3 tables exist with correct columns
- Confirmed foreign key relationships are correct

---

## Dependencies Installed

```toml
[tool.poetry.dependencies]
python = "^3.10"
fastapi = "^0.109.0"
uvicorn = {extras = ["standard"], version = "^0.27.0"}
pydantic = "^2.5.0"
pydantic-settings = "^2.1.0"
supabase = "^2.3.0"
python-multipart = "^0.0.6"

[tool.poetry.group.dev.dependencies]
pytest = "^7.4.0"
httpx = "^0.26.0"
pytest-asyncio = "^0.23.0"
```

---

## Notes & Decisions

1. **API Contract approach** - Pydantic models define the data contract. Full OpenAPI specification will be auto-generated by FastAPI when endpoints are built in Milestone 5 (Swagger UI at `/docs`)

2. **Soft delete for sessions** - Using `status` field instead of hard delete preserves audit trail

3. **JSONB for metadata** - Flexible storage for agent reasoning, generated code, etc.

4. **Node tree structure** - `parent_id` self-reference enables branching when user rolls back and makes new changes

5. **Separate profile snapshot per node** - Stored in `profile_json` so we don't need to re-profile for history views

6. **Poetry over pip** - Better dependency resolution and lock file support

7. **Pydantic v2** - Modern syntax, better performance, improved validation

---

## Next Milestone

**Milestone 2: Data Profiling Engine**
- Build `lib/profiler.py` to turn CSV data into AI-readable text summaries
- Handle edge cases: encoding errors, bad delimiters, large files
- Create `generate_profile(df)` function that outputs DataProfile
