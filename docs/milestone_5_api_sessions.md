# Milestone 5: API Layer 1 - Sessions & File Management

**Status:** ✅ Complete (Pending Supabase Storage Bucket Creation)
**Roadmap Reference:** Section 5 - "API Foundation"
**Date Completed:** January 2026

---

## Overview

Build the synchronous "Front Door" of the API. This milestone creates the foundational REST endpoints for session management and file uploads, making the system testable via Postman.

**Goal:** A working API where you can upload a CSV file and get a Session ID back.

---

## What We're Building

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/sessions` | Create a new session |
| `GET` | `/api/v1/sessions/{id}` | Get session details |
| `POST` | `/api/v1/sessions/{id}/upload` | Upload CSV file |
| `GET` | `/api/v1/sessions/{id}/profile` | Get data profile |
| `GET` | `/api/v1/sessions/{id}/data` | Download current data |
| `GET` | `/api/v1/health` | Health check endpoint |

### Upload Pipeline Flow

```
POST /upload
    │
    ├─→ 1. Validate file (CSV, size limits)
    │
    ├─→ 2. Stream to Supabase Storage
    │
    ├─→ 3. Run Data Profiling Engine
    │
    ├─→ 4. Create "Node 0" in database
    │
    └─→ 5. Return session_id + profile summary
```

---

## Technical Requirements

### 1. FastAPI Application Setup

**File:** `app/main.py`

- Initialize FastAPI with metadata (title, version, description)
- Configure CORS middleware (allow all origins for development)
- Add global exception handlers
- Include API routers
- Health check endpoint

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="ModularData API",
    version="1.0.0",
    description="AI-powered data transformation API"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### 2. Session Endpoints

**File:** `app/routers/sessions.py`

#### POST /api/v1/sessions
Create a new empty session.

**Request:** None (or optional metadata)

**Response:**
```json
{
    "session_id": "uuid",
    "status": "created",
    "created_at": "2024-01-15T10:30:00Z"
}
```

#### GET /api/v1/sessions/{id}
Get session details including current state.

**Response:**
```json
{
    "session_id": "uuid",
    "status": "active",
    "current_node_id": "uuid",
    "original_filename": "customers.csv",
    "row_count": 1000,
    "column_count": 5,
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:35:00Z"
}
```

### 3. Upload Pipeline

**File:** `app/routers/upload.py`

#### POST /api/v1/sessions/{id}/upload
Upload a CSV file to an existing session.

**Request:** `multipart/form-data` with file

**Process:**
1. Validate file extension (.csv)
2. Validate file size (< 50MB default)
3. Read file with `read_csv_safe()` (handles encoding/delimiters)
4. Upload to Supabase Storage: `uploads/{session_id}/original.csv`
5. Generate profile with `generate_profile(df)`
6. Create Node 0 in database (root node, no parent)
7. Update session with current_node_id
8. Return profile summary

**Response:**
```json
{
    "session_id": "uuid",
    "node_id": "uuid",
    "filename": "customers.csv",
    "storage_path": "uploads/uuid/original.csv",
    "profile": {
        "row_count": 1000,
        "column_count": 5,
        "columns": [
            {
                "name": "email",
                "dtype": "object",
                "null_count": 50,
                "null_percent": 5.0,
                "semantic_type": "email"
            }
        ],
        "issues": [
            "Column 'email' has 50 missing values (5%)"
        ]
    }
}
```

### 4. Data Access Endpoints

**File:** `app/routers/data.py`

#### GET /api/v1/sessions/{id}/profile
Get the current data profile.

#### GET /api/v1/sessions/{id}/data
Download the current CSV data.

**Query Params:**
- `format`: "csv" (default) or "json"
- `limit`: Max rows to return (default: all)

---

## Files to Create

| File | Description |
|------|-------------|
| `app/main.py` | FastAPI application entry point |
| `app/routers/sessions.py` | Session CRUD endpoints |
| `app/routers/upload.py` | File upload pipeline |
| `app/routers/data.py` | Data access endpoints |
| `app/routers/health.py` | Health check endpoint |
| `app/dependencies.py` | Shared dependencies (DB, storage) |
| `app/exceptions.py` | Custom exception handlers |
| `core/services/session_service.py` | Session business logic |
| `core/services/storage_service.py` | Supabase storage operations |
| `core/services/node_service.py` | Node CRUD operations |
| `tests/test_api_sessions.py` | API endpoint tests |

---

## Implementation Steps

### Step 1: FastAPI Application Setup
- Create `app/main.py` with FastAPI instance
- Configure CORS middleware
- Add global exception handlers
- Create health check endpoint

### Step 2: Service Layer
- Create `core/services/session_service.py` for session operations
- Create `core/services/storage_service.py` for Supabase Storage
- Create `core/services/node_service.py` for node operations

### Step 3: Session Endpoints
- Create `app/routers/sessions.py`
- Implement POST /sessions (create)
- Implement GET /sessions/{id} (read)

### Step 4: Upload Pipeline
- Create `app/routers/upload.py`
- Implement file validation
- Implement Supabase Storage upload
- Wire up profiler
- Create Node 0

### Step 5: Data Access Endpoints
- Create `app/routers/data.py`
- Implement GET /profile
- Implement GET /data (download)

### Step 6: Testing with Postman
- Create Postman collection
- Test all endpoints
- Document example requests/responses

---

## Dependencies on Previous Milestones

| Milestone | What We Use |
|-----------|-------------|
| M1: Architecture | `core/models/session.py`, `core/models/node.py` |
| M2: Profiler | `lib/profiler.py` - `generate_profile()`, `read_csv_safe()` |
| M1: Database | `lib/supabase_client.py` - Database operations |

---

## Request/Response Models

### SessionCreate (Request)
```python
class SessionCreate(BaseModel):
    name: str | None = None
    metadata: dict | None = None
```

### SessionResponse (Response)
```python
class SessionResponse(BaseModel):
    session_id: str
    status: str
    current_node_id: str | None
    original_filename: str | None
    row_count: int
    column_count: int
    created_at: datetime
    updated_at: datetime
```

### UploadResponse (Response)
```python
class UploadResponse(BaseModel):
    session_id: str
    node_id: str
    filename: str
    storage_path: str
    profile: ProfileSummary
```

### ProfileSummary (Response)
```python
class ProfileSummary(BaseModel):
    row_count: int
    column_count: int
    columns: list[ColumnSummary]
    issues: list[str]
```

---

## Error Handling

| Error | Status Code | Response |
|-------|-------------|----------|
| Session not found | 404 | `{"detail": "Session not found"}` |
| Invalid file type | 400 | `{"detail": "Only CSV files are supported"}` |
| File too large | 413 | `{"detail": "File exceeds 50MB limit"}` |
| Upload failed | 500 | `{"detail": "Failed to upload file"}` |
| Profiling failed | 500 | `{"detail": "Failed to analyze file"}` |

---

## Postman Collection Structure

```
ModularData API
├── Health
│   └── GET /health
├── Sessions
│   ├── POST /sessions (Create Session)
│   └── GET /sessions/{id} (Get Session)
├── Upload
│   └── POST /sessions/{id}/upload (Upload CSV)
└── Data
    ├── GET /sessions/{id}/profile (Get Profile)
    └── GET /sessions/{id}/data (Download Data)
```

---

## Verification

```bash
# Start the server
poetry run uvicorn app.main:app --reload

# Test health endpoint
curl http://localhost:8000/api/v1/health

# Create session
curl -X POST http://localhost:8000/api/v1/sessions

# Upload file
curl -X POST http://localhost:8000/api/v1/sessions/{id}/upload \
  -F "file=@test.csv"

# Get profile
curl http://localhost:8000/api/v1/sessions/{id}/profile
```

---

## Success Criteria

1. ✅ FastAPI server starts without errors
2. ✅ Health endpoint returns 200
3. ✅ Can create a session via POST
4. ⏳ Can upload CSV and receive profile (requires storage bucket)
5. ⏳ File is stored in Supabase Storage (requires storage bucket)
6. ⏳ Node 0 is created in database (requires storage bucket)
7. ✅ Can retrieve session details
8. ⏳ Can download current data (requires storage bucket)
9. ✅ All endpoints testable via Postman

---

## Implementation Complete

### Files Created

| File | Description |
|------|-------------|
| `app/main.py` | FastAPI application entry point with CORS, exception handlers |
| `app/exceptions.py` | Custom exception classes with actionable error messages |
| `app/dependencies.py` | Shared dependencies for dependency injection |
| `app/routers/__init__.py` | Router exports |
| `app/routers/health.py` | Health check endpoints (/health, /health/ready, /health/live) |
| `app/routers/sessions.py` | Session CRUD endpoints |
| `app/routers/upload.py` | File upload pipeline with validation |
| `app/routers/data.py` | Data access and download endpoints |
| `core/services/__init__.py` | Service layer exports |
| `core/services/session_service.py` | Session business logic |
| `core/services/storage_service.py` | Supabase Storage operations |
| `core/services/node_service.py` | Node CRUD operations |

### API Endpoints Implemented

| Method | Endpoint | Status |
|--------|----------|--------|
| `GET` | `/` | ✅ Working |
| `GET` | `/api/v1/health` | ✅ Working |
| `GET` | `/api/v1/health/ready` | ✅ Working |
| `GET` | `/api/v1/health/live` | ✅ Working |
| `POST` | `/api/v1/sessions` | ✅ Working |
| `GET` | `/api/v1/sessions/{id}` | ✅ Working |
| `GET` | `/api/v1/sessions` | ✅ Working |
| `DELETE` | `/api/v1/sessions/{id}` | ✅ Working |
| `POST` | `/api/v1/sessions/{id}/upload` | ⏳ Requires bucket |
| `GET` | `/api/v1/sessions/{id}/profile` | ⏳ Requires bucket |
| `GET` | `/api/v1/sessions/{id}/profile/summary` | ⏳ Requires bucket |
| `GET` | `/api/v1/sessions/{id}/data` | ⏳ Requires bucket |
| `GET` | `/api/v1/sessions/{id}/preview` | ⏳ Requires bucket |
| `GET` | `/api/v1/sessions/{id}/nodes/{node_id}/data` | ⏳ Requires bucket |
| `GET` | `/api/v1/sessions/{id}/nodes/{node_id}/profile` | ⏳ Requires bucket |

---

## Setup Required

### 1. Create Supabase Storage Bucket

Before file uploads will work, you need to create a storage bucket in Supabase:

1. Go to your Supabase project dashboard
2. Navigate to **Storage** in the left sidebar
3. Click **New Bucket**
4. Enter:
   - **Name:** `uploads`
   - **Public bucket:** No (keep private)
5. Click **Create bucket**

### 2. Set Bucket Policies (Optional)

For the service role key to work, add these policies:

```sql
-- Allow authenticated uploads
CREATE POLICY "Allow uploads" ON storage.objects
    FOR INSERT TO authenticated
    WITH CHECK (bucket_id = 'uploads');

-- Allow authenticated downloads
CREATE POLICY "Allow downloads" ON storage.objects
    FOR SELECT TO authenticated
    USING (bucket_id = 'uploads');
```

---

## Testing After Setup

Once the storage bucket is created:

```bash
# Start the server
poetry run uvicorn app.main:app --reload

# Test the full flow
SESSION_ID=$(curl -s -X POST http://localhost:8000/api/v1/sessions | jq -r '.session_id')
echo "Created session: $SESSION_ID"

# Upload a test CSV
curl -X POST "http://localhost:8000/api/v1/sessions/$SESSION_ID/upload" \
  -F "file=@test.csv"

# Get profile
curl "http://localhost:8000/api/v1/sessions/$SESSION_ID/profile"

# Download data
curl "http://localhost:8000/api/v1/sessions/$SESSION_ID/data"
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      FastAPI Application                    │
│                         (app/main.py)                       │
├─────────────────────────────────────────────────────────────┤
│  Routers                                                    │
│  ├── health.py    → /api/v1/health/*                       │
│  ├── sessions.py  → /api/v1/sessions/*                     │
│  ├── upload.py    → /api/v1/sessions/{id}/upload           │
│  └── data.py      → /api/v1/sessions/{id}/data, /profile   │
├─────────────────────────────────────────────────────────────┤
│  Service Layer (core/services/)                            │
│  ├── session_service.py  → Session CRUD                    │
│  ├── storage_service.py  → Supabase Storage                │
│  └── node_service.py     → Version tree operations         │
├─────────────────────────────────────────────────────────────┤
│  Data Layer                                                 │
│  ├── lib/supabase_client.py → Database operations          │
│  └── lib/profiler.py        → CSV profiling                │
└─────────────────────────────────────────────────────────────┘
```
