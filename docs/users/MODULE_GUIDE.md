# ModularData Module System Guide

This guide explains how to create, deploy, and run modules in ModularData. Modules are reusable data transformation pipelines that can be applied to new datasets with similar schemas.

---

## Table of Contents

1. [What is a Module?](#what-is-a-module)
2. [Module Lifecycle](#module-lifecycle)
3. [Creating a Module](#creating-a-module)
4. [Deploying a Module](#deploying-a-module)
5. [Running a Module](#running-a-module)
6. [Viewing Run History](#viewing-run-history)
7. [Schema Matching Explained](#schema-matching-explained)
8. [API Reference](#api-reference)

---

## What is a Module?

A **module** is a reusable transformation pipeline that captures a sequence of data transformations. Think of it as a recipe for cleaning and transforming data that can be applied to multiple datasets.

### Key Concepts

- **Session**: Every module starts as a session. A session becomes a module when you deploy it.
- **Transformation Chain**: The sequence of operations applied to your data (filter, rename, calculate, etc.)
- **Schema Contract**: When deployed, the module creates a contract based on your original data's schema. New data must match this contract.
- **Nodes**: Each transformation creates a node in the history. This enables undo/redo and version tracking.

### Why Use Modules?

- **Consistency**: Apply the same transformations to monthly reports, daily exports, or recurring data files
- **Efficiency**: Build once, run many times
- **Auditability**: Every run is logged with input/output details
- **Schema Safety**: Automatic validation ensures new data is compatible

---

## Module Lifecycle

A module progresses through these states:

```
CREATE SESSION --> UPLOAD DATA --> TRANSFORM --> DEPLOY --> RUN ON NEW DATA
      |                                            |              |
   DRAFT                                       DEPLOYED      (edit)
      ^                                            |              |
      |____________________________________________|______________|
                    (editing reverts to DRAFT)
```

### Status States

| Status | Description |
|--------|-------------|
| `draft` | Module is being built or edited. Cannot run on new data. |
| `deployed` | Module is locked and ready to run on new data. |
| `archived` | Module is soft-deleted (preserved for audit). |

### Status Transitions

- **DRAFT -> DEPLOYED**: Call `POST /sessions/{id}/deploy`
- **DEPLOYED -> DRAFT**: Automatically happens when you edit a deployed module
- **Any -> ARCHIVED**: Call `DELETE /sessions/{id}`

---

## Creating a Module

### Step 1: Create a Session

```bash
curl -X POST "https://api.modulardata.io/sessions" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json"
```

Response:
```json
{
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "draft",
  "created_at": "2024-01-15T10:30:00Z",
  "message": "Session created successfully"
}
```

### Step 2: Upload Initial Data

Upload a CSV file that represents the structure of data you will process:

```bash
curl -X POST "https://api.modulardata.io/sessions/550e8400-e29b-41d4-a716-446655440000/upload" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@sample_data.csv"
```

This creates the **root node** containing:
- Your original data
- A profile of your data's schema (column names, types, statistics)

### Step 3: Apply Transformations

Use the chat interface or API to apply transformations. Each transformation creates a new node:

```bash
curl -X POST "https://api.modulardata.io/sessions/550e8400-e29b-41d4-a716-446655440000/chat" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"message": "Remove rows where status is inactive"}'
```

Common transformations:
- Filter rows based on conditions
- Rename columns
- Calculate new columns
- Remove duplicates
- Convert data types
- Group and aggregate

Each transformation is recorded with its code, allowing it to be replayed on new data.

---

## Deploying a Module

Deployment locks the current transformation chain and creates a schema contract.

### Deploy Endpoint

```bash
curl -X POST "https://api.modulardata.io/sessions/550e8400-e29b-41d4-a716-446655440000/deploy" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

Response:
```json
{
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "deployed",
  "deployed_node_id": "7a8b9c0d-1e2f-3a4b-5c6d-7e8f9a0b1c2d",
  "deployed_at": "2024-01-15T12:00:00Z",
  "message": "Module deployed successfully"
}
```

### What Happens During Deployment

1. **Status Change**: Session status changes from `draft` to `deployed`
2. **Deployed Node ID**: The current node is saved as `deployed_node_id`
3. **Schema Contract**: A contract is created from the root node's profile
4. **Timestamp**: `deployed_at` is recorded

### Requirements

- Session must have data uploaded (must have a `current_node_id`)
- Session cannot be archived

### Editing After Deployment

If you edit a deployed module (apply new transformations), it automatically reverts to `draft` status. You must redeploy to run it again.

---

## Running a Module

Once deployed, you can run the module on new data files.

### Run Endpoint

```bash
curl -X POST "https://api.modulardata.io/sessions/550e8400-e29b-41d4-a716-446655440000/run" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@new_data.csv"
```

### Run Process

1. **Profile Incoming Data**: Analyze the uploaded file's schema
2. **Match Schema**: Compare against the module's contract
3. **Execute Transformations**: If compatible, replay all transformations
4. **Store Output**: Save the transformed data
5. **Log Run**: Record all details for audit

### Response (Success)

```json
{
  "run_id": "abc12345-def6-7890-ghij-klmnopqrstuv",
  "status": "success",
  "confidence_score": 95.5,
  "confidence_level": "HIGH",
  "input_rows": 1000,
  "input_columns": 8,
  "output_rows": 850,
  "output_columns": 10,
  "output_storage_path": "runs/abc12345/output.csv",
  "duration_ms": 1250,
  "column_mappings": [
    {
      "incoming_name": "customer_id",
      "contract_name": "customer_id",
      "match_type": "exact",
      "confidence": 100,
      "type_compatible": true
    }
  ]
}
```

### Response (Requires Confirmation)

When confidence is MEDIUM, the run pauses for confirmation:

```json
{
  "run_id": "abc12345-def6-7890-ghij-klmnopqrstuv",
  "status": "pending",
  "confidence_score": 72.0,
  "confidence_level": "MEDIUM",
  "requires_confirmation": true,
  "column_mappings": [...],
  "discrepancies": [
    {
      "type": "type_mismatch",
      "severity": "warning",
      "column": "amount",
      "description": "Expected float, got string",
      "suggestion": "Check data formatting"
    }
  ],
  "message": "Schema has some differences. Please confirm to proceed."
}
```

### Force Run (Skip Confirmation)

To bypass MEDIUM confidence confirmation:

```bash
curl -X POST "https://api.modulardata.io/sessions/550e8400-e29b-41d4-a716-446655440000/run?force=true" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@new_data.csv"
```

### Confirm a Pending Run

Re-upload the file to confirm:

```bash
curl -X POST "https://api.modulardata.io/sessions/550e8400-e29b-41d4-a716-446655440000/runs/abc12345-def6-7890-ghij-klmnopqrstuv/confirm" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@new_data.csv"
```

---

## Viewing Run History

### List All Runs

```bash
curl -X GET "https://api.modulardata.io/sessions/550e8400-e29b-41d4-a716-446655440000/runs?limit=20&offset=0" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

Response:
```json
{
  "runs": [
    {
      "run_id": "abc12345-def6-7890-ghij-klmnopqrstuv",
      "status": "success",
      "confidence_score": 95.5,
      "confidence_level": "HIGH",
      "input_filename": "january_sales.csv",
      "input_row_count": 1000,
      "output_row_count": 850,
      "created_at": "2024-01-16T09:00:00Z",
      "duration_ms": 1250
    },
    {
      "run_id": "xyz98765-abc4-3210-defg-hijklmnopqrs",
      "status": "failed",
      "confidence_score": 25.0,
      "confidence_level": "NO_MATCH",
      "input_filename": "wrong_format.csv",
      "input_row_count": 500,
      "output_row_count": null,
      "created_at": "2024-01-15T14:30:00Z",
      "duration_ms": 150
    }
  ],
  "total": 2
}
```

### Get Run Details

```bash
curl -X GET "https://api.modulardata.io/sessions/550e8400-e29b-41d4-a716-446655440000/runs/abc12345-def6-7890-ghij-klmnopqrstuv" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

Response:
```json
{
  "run_id": "abc12345-def6-7890-ghij-klmnopqrstuv",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "created_at": "2024-01-16T09:00:00Z",
  "input_filename": "january_sales.csv",
  "input_row_count": 1000,
  "input_column_count": 8,
  "input_storage_path": null,
  "confidence_score": 95.5,
  "confidence_level": "HIGH",
  "column_mappings": [...],
  "discrepancies": [],
  "output_row_count": 850,
  "output_column_count": 10,
  "output_storage_path": "runs/abc12345/output.csv",
  "error_message": null,
  "error_details": null,
  "duration_ms": 1250,
  "timing_breakdown": {
    "schema_match_ms": 50,
    "transform_ms": 1100,
    "upload_ms": 100
  }
}
```

### Download Run Output

```bash
curl -X GET "https://api.modulardata.io/sessions/550e8400-e29b-41d4-a716-446655440000/runs/abc12345-def6-7890-ghij-klmnopqrstuv/download" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -o transformed_output.csv
```

This returns the transformed CSV file. Only available for successful runs (`status: success` or `status: warning_confirmed`).

---

## Schema Matching Explained

When you run a module on new data, ModularData compares the incoming file's schema against the expected schema (contract).

### Confidence Levels

| Level | Score Range | Behavior |
|-------|-------------|----------|
| **HIGH** | 85-100% | Automatically processes |
| **MEDIUM** | 60-84% | Requires confirmation (or `force=true`) |
| **LOW** | 40-59% | Rejected - schema too different |
| **NO_MATCH** | 0-39% | Rejected - file does not match |

### How Confidence is Calculated

The schema matcher evaluates:

1. **Column Name Matching**
   - Exact matches (100% confidence)
   - Case-insensitive matches (95%)
   - Fuzzy matches using edit distance (variable)

2. **Type Compatibility**
   - Same type: full points
   - Compatible types (int -> float): partial points
   - Incompatible types: penalty

3. **Required vs Optional Columns**
   - Missing required columns: heavy penalty
   - Extra columns in incoming data: no penalty (ignored)

### Column Mapping

The system maps incoming columns to expected columns:

```json
{
  "incoming_name": "Customer ID",
  "contract_name": "customer_id",
  "match_type": "fuzzy",
  "confidence": 85,
  "type_compatible": true,
  "notes": "Case difference"
}
```

Match types:
- `exact`: Perfect match
- `case_insensitive`: Same name, different case
- `fuzzy`: Similar name (Levenshtein distance)
- `unmapped`: No match found

### Discrepancies

When issues are found, discrepancies are reported:

```json
{
  "type": "missing_column",
  "severity": "error",
  "column": "transaction_date",
  "description": "Required column not found in input",
  "suggestion": "Add a transaction_date column or rename an existing date column"
}
```

Severity levels:
- `error`: Will likely cause transformation failure
- `warning`: May cause issues but can proceed
- `info`: Minor difference, unlikely to cause problems

### Handling Schema Drift

If your data schema changes over time:

1. **Minor changes** (new optional columns, case differences): Usually HIGH/MEDIUM confidence
2. **Renamed columns**: May need to update the module with new transformations
3. **Major changes**: Consider creating a new module

To update a module for schema drift:
1. Upload new sample data
2. Adjust transformations as needed
3. Redeploy

---

## API Reference

### Session Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/sessions` | Create a new session |
| GET | `/sessions/{id}` | Get session details |
| GET | `/sessions` | List all sessions |
| PATCH | `/sessions/{id}` | Update session (rename) |
| DELETE | `/sessions/{id}` | Archive session |
| POST | `/sessions/{id}/deploy` | Deploy module |
| POST | `/sessions/{id}/upload` | Upload data to session |

### Run Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/sessions/{id}/run` | Run module on new data |
| GET | `/sessions/{id}/runs` | List run history |
| GET | `/sessions/{id}/runs/{run_id}` | Get run details |
| GET | `/sessions/{id}/runs/{run_id}/download` | Download output |
| POST | `/sessions/{id}/runs/{run_id}/confirm` | Confirm pending run |

### Query Parameters

**POST /sessions/{id}/run**
- `force` (boolean): Skip confirmation for MEDIUM confidence (default: false)

**GET /sessions/{id}/runs**
- `limit` (int): Max runs to return (1-100, default: 50)
- `offset` (int): Pagination offset (default: 0)

---

## Common Workflows

### Monthly Report Processing

1. Create module with January data
2. Apply transformations (clean, calculate, format)
3. Deploy module
4. Each month: run module on new month's data
5. Download transformed outputs

### Data Validation Pipeline

1. Create module with clean sample data
2. Apply validation transformations
3. Deploy module
4. Run new files through module
5. Check confidence scores to identify problematic files

### Iterative Module Development

1. Create module with sample data
2. Transform data
3. Deploy and test with real data
4. If issues found, edit transformations (reverts to draft)
5. Redeploy when ready

---

## Troubleshooting

### "Module has not been deployed"

You must deploy before running on new data:
```bash
curl -X POST ".../sessions/{id}/deploy" -H "Authorization: Bearer ..."
```

### "Schema confidence too low"

Your new file's structure is too different from the original. Check:
- Column names (case matters!)
- Column order (should not matter, but check for missing columns)
- Data types

### "Transformation failed"

A transformation could not be applied to the new data. Check:
- The error message for which step failed
- Whether the new data has all required columns
- Whether data types are compatible

### Run stuck in "pending"

The run needs confirmation. Either:
- Confirm with `POST .../runs/{run_id}/confirm`
- Re-run with `force=true`
