# ModularData API Reference

> **Base URL:** `https://web-production-2d224.up.railway.app`
> **Version:** v1
> **Last Updated:** 2026-01-16

---

## Overview

ModularData is an AI-powered data transformation API that lets you clean, transform, and analyze CSV data using natural language instructions.

### Key Concepts

- **Session** (API) / **Module** (UI): A workspace containing your data and transformation history. The API uses "session" while the web interface displays these as "modules".
- **Node**: A version of your data (each transformation creates a new node)
- **Plan**: A queue of transformations to apply (plan mode)
- **Task**: An async job for executing transformations

> **Note:** The API uses "sessions" for endpoints and responses. The web UI displays sessions as "Modules" for user-friendliness. They refer to the same concept.

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

#### PATCH /api/v1/sessions/{session_id}
Update session details (rename module).

**Request Body:**
```json
{
    "name": "My Renamed Module"
}
```

**Response:**
```json
{
    "session_id": "...",
    "name": "My Renamed Module",
    "message": "Module updated successfully"
}
```

#### DELETE /api/v1/sessions/{session_id}
Archive a session.

#### POST /api/v1/sessions/{session_id}/deploy
Deploy a module, making it ready to run on new data.

Once deployed:
- The module can be run on new data via POST /sessions/{id}/run
- The current transformation chain becomes the "contract"
- Editing the module will revert it to draft status

**Response:**
```json
{
    "session_id": "...",
    "status": "active",
    "deployed_node_id": "3b815670-e3ac-4cd1-b084-5382bc779e36",
    "deployed_at": "2026-01-15T16:30:00.000000+00:00",
    "message": "Module deployed successfully"
}
```

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

### Module Runs

Run deployed modules on new data. Modules must be deployed via `POST /sessions/{id}/deploy` before they can be run.

#### POST /api/v1/sessions/{session_id}/run
Upload a CSV file and run the module's transformations on it.

**Request:**
- Content-Type: `multipart/form-data`
- Body: `file` (CSV file)

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| force | bool | false | Force run even with MEDIUM confidence |

The module will:
1. Profile the incoming file
2. Match columns against the module's expected schema
3. If compatible, replay all transformations
4. Return the transformed output

**Confidence Levels:**
- **HIGH (>=85%)**: Automatically processes
- **MEDIUM (60-84%)**: Requires confirmation or `force=true`
- **LOW (40-59%)**: Rejected - schema too different
- **NO_MATCH (<40%)**: Rejected - file doesn't match at all

**Response:**
```json
{
    "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "status": "success",
    "confidence_score": 92.5,
    "confidence_level": "HIGH",
    "input_rows": 100,
    "input_columns": 6,
    "output_rows": 95,
    "output_columns": 6,
    "error_message": null,
    "requires_confirmation": false,
    "column_mappings": [
        {"source": "customer_name", "target": "name", "confidence": 95.0},
        {"source": "email_address", "target": "email", "confidence": 98.0}
    ],
    "discrepancies": [],
    "output_storage_path": "sessions/.../runs/.../output.csv",
    "duration_ms": 1250,
    "message": "Module run completed successfully"
}
```

**Status Values:**
- `success` - Run completed successfully
- `failed` - Run failed with error
- `pending` - Awaiting confirmation (MEDIUM confidence)
- `warning_confirmed` - Completed after user confirmation

#### GET /api/v1/sessions/{session_id}/runs
List run history for a module.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| limit | int | 50 | Max runs to return (1-100) |
| offset | int | 0 | Offset for pagination |

**Response:**
```json
{
    "runs": [
        {
            "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "status": "success",
            "confidence_score": 92.5,
            "confidence_level": "HIGH",
            "input_filename": "january_data.csv",
            "input_row_count": 100,
            "output_row_count": 95,
            "created_at": "2026-01-15T16:45:00.000000+00:00",
            "duration_ms": 1250
        },
        {
            "run_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
            "status": "success",
            "confidence_score": 88.0,
            "confidence_level": "HIGH",
            "input_filename": "december_data.csv",
            "input_row_count": 85,
            "output_row_count": 82,
            "created_at": "2026-01-14T10:30:00.000000+00:00",
            "duration_ms": 980
        }
    ],
    "total": 2
}
```

#### GET /api/v1/sessions/{session_id}/runs/{run_id}
Get detailed information about a specific run.

Includes full schema matching results, timing breakdown, and error details.

**Response:**
```json
{
    "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "session_id": "9040d1ad-d698-40be-b4cb-279f91b95b71",
    "status": "success",
    "created_at": "2026-01-15T16:45:00.000000+00:00",
    "input_filename": "january_data.csv",
    "input_row_count": 100,
    "input_column_count": 6,
    "input_storage_path": "sessions/.../runs/.../input.csv",
    "confidence_score": 92.5,
    "confidence_level": "HIGH",
    "column_mappings": [
        {"source": "customer_name", "target": "name", "confidence": 95.0},
        {"source": "email_address", "target": "email", "confidence": 98.0}
    ],
    "discrepancies": [
        {"type": "missing_column", "column": "phone", "severity": "warning"}
    ],
    "output_row_count": 95,
    "output_column_count": 6,
    "output_storage_path": "sessions/.../runs/.../output.csv",
    "error_message": null,
    "error_details": null,
    "duration_ms": 1250,
    "timing_breakdown": {
        "schema_matching_ms": 150,
        "transformation_ms": 980,
        "storage_ms": 120
    }
}
```

#### GET /api/v1/sessions/{session_id}/runs/{run_id}/download
Download the output file from a successful run.

Returns the transformed CSV file as a download.

**Response:**
- Content-Type: `text/csv`
- Content-Disposition: `attachment; filename="original_transformed.csv"`

**Error Responses:**
- `400` - Run did not complete successfully
- `404` - Run not found or output file not found

#### POST /api/v1/sessions/{session_id}/runs/{run_id}/confirm
Confirm a pending run with MEDIUM confidence and execute it.

When a run has MEDIUM confidence, the user must explicitly confirm before the transformation proceeds. Re-upload the same file to proceed.

**Request:**
- Content-Type: `multipart/form-data`
- Body: `file` (CSV file - must match original upload)

**Response:**
```json
{
    "run_id": "c3d4e5f6-a7b8-9012-cdef-123456789012",
    "status": "warning_confirmed",
    "confidence_score": 72.0,
    "confidence_level": "MEDIUM",
    "input_rows": 100,
    "input_columns": 6,
    "output_rows": 95,
    "output_columns": 6,
    "error_message": null,
    "requires_confirmation": false,
    "column_mappings": [...],
    "discrepancies": [...],
    "output_storage_path": "sessions/.../runs/.../output.csv",
    "duration_ms": 1350,
    "message": "Run confirmed and executed successfully."
}
```

**Error Responses:**
- `400` - Run is not pending confirmation
- `404` - Run not found

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
