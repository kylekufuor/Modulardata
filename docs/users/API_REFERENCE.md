# ModularData API Reference

> **Base URL:** `https://web-production-2d224.up.railway.app`
> **Version:** v1
> **Last Updated:** 2026-01-15

---

## Overview

ModularData is an AI-powered data transformation API that lets you clean, transform, and analyze CSV data using natural language instructions.

### Key Concepts

- **Session**: A workspace containing your data and transformation history
- **Node**: A version of your data (each transformation creates a new node)
- **Plan**: A queue of transformations to apply (plan mode)
- **Task**: An async job for executing transformations

---

## Authentication

> **Note:** Authentication is not yet implemented. All endpoints are currently public.

---

## Endpoints

### Health

#### GET /api/v1/health
Basic health check.

**Response:**
```json
{
    "status": "healthy",
    "timestamp": "2026-01-15T15:00:00.000000",
    "environment": "production",
    "version": "1.0.0"
}
```

#### GET /api/v1/health/ready
Readiness check (verifies database and storage connections).

**Response:**
```json
{
    "status": "ready",
    "checks": {
        "database": "healthy",
        "storage": "healthy"
    }
}
```

#### GET /api/v1/health/live
Liveness check.

**Response:**
```json
{
    "status": "alive"
}
```

---

### Sessions

#### POST /api/v1/sessions
Create a new session.

**Request Body:** None required

**Response:**
```json
{
    "session_id": "9040d1ad-d698-40be-b4cb-279f91b95b71",
    "status": "active",
    "created_at": "2026-01-15T15:25:03.004587+00:00",
    "message": "Session created successfully"
}
```

#### GET /api/v1/sessions
List all sessions (paginated).

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| page | int | 1 | Page number |
| page_size | int | 10 | Results per page |

**Response:**
```json
{
    "sessions": [...],
    "total": 9,
    "page": 1,
    "page_size": 10
}
```

#### GET /api/v1/sessions/{session_id}
Get session details.

**Response:**
```json
{
    "session_id": "...",
    "status": "active",
    "created_at": "...",
    "original_filename": "customers.csv",
    "current_node_id": "...",
    "row_count": 15,
    "column_count": 6
}
```

#### DELETE /api/v1/sessions/{session_id}
Archive a session.

---

### Upload

#### POST /api/v1/sessions/{session_id}/upload
Upload a CSV file.

**Request:**
- Content-Type: `multipart/form-data`
- Body: `file` (CSV file)

**Response:**
```json
{
    "session_id": "...",
    "node_id": "...",
    "filename": "customers.csv",
    "storage_path": "sessions/.../original.csv",
    "profile": {
        "row_count": 15,
        "column_count": 6,
        "columns": [...],
        "issues": [...],
        "duplicate_row_count": 0
    },
    "preview": [...]
}
```

---

### Data Access

#### GET /api/v1/sessions/{session_id}/preview
Get preview of current data (first 10 rows).

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| limit | int | 10 | Number of rows |

**Response:**
```json
{
    "session_id": "...",
    "node_id": "...",
    "row_count": 15,
    "column_count": 6,
    "preview": [
        {"name": "John", "age": 32, ...},
        ...
    ]
}
```

#### GET /api/v1/sessions/{session_id}/profile
Get full data profile.

#### GET /api/v1/sessions/{session_id}/profile/summary
Get compact profile summary.

#### GET /api/v1/sessions/{session_id}/data
Download data as CSV or JSON.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| format | string | csv | `csv` or `json` |

#### GET /api/v1/sessions/{session_id}/nodes/{node_id}/data
Download specific version.

#### GET /api/v1/sessions/{session_id}/nodes/{node_id}/profile
Get profile for specific version.

---

### Chat (Plan Mode)

#### POST /api/v1/sessions/{session_id}/chat
Send a transformation instruction.

**Request Body:**
```json
{
    "message": "remove rows where email is blank"
}
```

**Response:**
```json
{
    "session_id": "...",
    "message": "remove rows where email is blank",
    "plan": {
        "plan_id": "...",
        "status": "planning",
        "steps": [
            {
                "step_number": 1,
                "transformation_type": "drop_rows",
                "target_columns": ["email"],
                "parameters": {},
                "explanation": "Remove all rows where email is null"
            }
        ],
        "step_count": 1
    },
    "assistant_response": "Added to plan: Remove all rows where email is null..."
}
```

#### GET /api/v1/sessions/{session_id}/plan
Get current plan.

#### POST /api/v1/sessions/{session_id}/plan/apply
Execute all planned transformations.

**Response:**
```json
{
    "success": true,
    "message": "Applying 2 transformation(s). Task ID: ...",
    "node_id": null
}
```

#### POST /api/v1/sessions/{session_id}/plan/clear
Clear the current plan.

---

### History & Rollback

#### GET /api/v1/sessions/{session_id}/history
Get full session history.

**Response:**
```json
{
    "session_id": "...",
    "current_node_id": "...",
    "total_nodes": 2,
    "nodes": [
        {
            "id": "...",
            "parent_id": null,
            "transformation": null,
            "row_count": 15,
            "is_current": false
        },
        {
            "id": "...",
            "parent_id": "...",
            "transformation": "Remove nulls; Trim whitespace",
            "row_count": 13,
            "is_current": true
        }
    ]
}
```

#### GET /api/v1/sessions/{session_id}/nodes
List all nodes (versions).

#### GET /api/v1/sessions/{session_id}/nodes/{node_id}
Get node details.

#### POST /api/v1/sessions/{session_id}/rollback
Rollback to a specific node.

**Request Body:**
```json
{
    "target_node_id": "3b815670-e3ac-4cd1-b084-5382bc779e36"
}
```

#### POST /api/v1/sessions/{session_id}/undo
Undo the last transformation.

#### GET /api/v1/sessions/{session_id}/lineage/{node_id}
Get ancestry chain for a node.

---

### Tasks

#### GET /api/v1/tasks/{task_id}
Get task status.

**Response:**
```json
{
    "task_id": "...",
    "status": "SUCCESS",
    "progress": 100,
    "message": "Complete",
    "result": {
        "success": true,
        "node_id": "...",
        "transformations_applied": 2,
        "rows_before": 15,
        "rows_after": 13
    }
}
```

**Status Values:**
- `PENDING` - Waiting in queue
- `PROGRESS` - Currently executing
- `SUCCESS` - Completed successfully
- `FAILURE` - Failed with error

#### GET /api/v1/tasks/{task_id}/result
Get task result only.

#### DELETE /api/v1/tasks/{task_id}
Cancel a task.

---

## Error Responses

All errors follow this format:

```json
{
    "detail": "Error message here",
    "code": "ERROR_CODE"
}
```

### Common Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `SESSION_NOT_FOUND` | 404 | Session does not exist |
| `NODE_NOT_FOUND` | 404 | Node does not exist |
| `INVALID_FILE_TYPE` | 400 | File is not a valid CSV |
| `FILE_TOO_LARGE` | 400 | File exceeds size limit |
| `NO_DATA` | 400 | Session has no uploaded data |
| `INTERNAL_ERROR` | 500 | Unexpected server error |

---

## Rate Limits

> **Note:** Rate limiting is not yet implemented.

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.
