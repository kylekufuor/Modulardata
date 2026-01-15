# ModularData Roadmap: Phase 2-7

> **Last Updated:** 2026-01-15
> **Status:** Approved

This document outlines the product roadmap from Phase 2 through Phase 7, building on the completed Phase 1 (Core API).

---

## Overview

| Phase | Milestones | Focus | Status |
|-------|------------|-------|--------|
| Phase 1 | 1-9 | Core API (CSV Transformations) | ✅ Complete |
| Phase 2 | 9.5-12 | Foundation + Web UI | 🔜 Next |
| Phase 3 | 13-15 | Module System | Planned |
| Phase 4 | 16-18 | Connections (Sources & Destinations) | Planned |
| Phase 5 | 19-21 | Workflows & Scheduling | Planned |
| Phase 6 | 22-23 | Adaptive AI & Schema Drift | Planned |
| Phase 7 | 24-26 | Multi-Format Support | Planned |

---

## Phase 2: Foundation + Web UI

### Milestone 9.5: Platform Foundation
**Goal:** Set up auth and real-time before building UI

**Deliverables:**
- Supabase Auth integration (email/password, OAuth)
- Add `users` table
- Add `user_id` foreign key to `sessions` table
- Protect all API endpoints with auth
- WebSocket endpoint for real-time session updates
- Update CORS and security headers

**Why First:** Everything depends on user identity. Adding later means painful migrations.

---

### Milestone 10: Frontend Foundation & Node Graph
**Goal:** Visual representation of transformation history

**Deliverables:**
- React frontend application (Vite + TypeScript)
- Authentication UI (login, signup, logout)
- Dashboard showing user's sessions
- Visual node graph component (React Flow or similar)
- Nodes display as connected graph (like Parabola)
- Click node to see preview of data at that point

**Tech:** React, TypeScript, TailwindCSS, React Flow

---

### Milestone 11: Chat Interface Integration
**Goal:** Split-pane transformation experience

**Deliverables:**
- Split-pane layout: Chat (left) + Node Graph (right)
- Chat input sends to existing `/chat` endpoint
- Real-time node creation via WebSocket
- Message history display
- Plan preview before applying
- Apply button triggers transformation

**User Flow:**
```
User types "remove nulls" → Chat shows plan → User clicks Apply → New node appears on graph
```

---

### Milestone 12: Node Inspector & Code Editor
**Goal:** Let users inspect and edit transformation code

**Deliverables:**
- Click node → Side panel opens with:
  - Data preview (first 100 rows)
  - Transformation code (syntax highlighted)
  - Metadata (row count, columns, timestamp)
- Code editor (Monaco Editor)
- "Save & Re-run" button
- Agent detects code changes and validates
- Error handling for invalid code

**Key Feature:** User edits code → Agent validates → Re-executes transformation

---

## Phase 3: Module System

### Milestone 13: Module Creation
**Goal:** Save transformation pipelines as reusable modules

**Deliverables:**
- "Deploy as Module" button on completed session
- Module naming and description
- Module = frozen snapshot of transformation pipeline
- `modules` table in database
- Module detail page showing:
  - Name, description
  - Transformation steps
  - Input schema expected
  - Output schema produced

---

### Milestone 14: Module Versioning
**Goal:** Handle schema changes with versions

**Deliverables:**
- `module_versions` table
- Each module can have multiple versions
- Version comparison view (side-by-side)
- Schema fingerprinting (detect what schema version handles)
- Per-user version limits (e.g., free=2, pro=5, enterprise=unlimited)
- Version metadata (created date, schema handled, usage count)

---

### Milestone 15: Module Library & Management
**Goal:** Organize and manage modules

**Deliverables:**
- Module library page (grid/list view)
- Search and filter modules
- Clone/duplicate module
- Archive/delete module
- Module tags and categories
- "Run Module" with file upload (quick test)
- Module usage statistics

---

## Phase 4: Connections

### Milestone 16: Connection Management
**Goal:** Store and manage external connections

**Deliverables:**
- Connections page in UI
- `connections` table with encrypted credentials
- Create/edit/delete connections
- Connection types supported:
  - S3 (AWS credentials)
  - PostgreSQL (connection string)
  - MySQL (connection string)
  - SFTP (host, user, key)
- Test connection button
- Connection health status

**Security:** Credentials encrypted at rest, never exposed in API responses

---

### Milestone 17: Source Connectors
**Goal:** Pull data from external sources

**Deliverables:**
- S3 source: Select bucket/prefix, list files, pull CSV
- PostgreSQL source: Write query, preview results, pull as CSV
- MySQL source: Same as PostgreSQL
- SFTP source: Browse directories, select file, pull
- Source → Session flow (creates new session with pulled data)
- Incremental pull options (new files only, since timestamp)

---

### Milestone 18: Destination Connectors
**Goal:** Push transformed data to destinations

**Deliverables:**
- S3 destination: Upload to bucket/path
- PostgreSQL destination: Insert/upsert to table
- MySQL destination: Insert/upsert to table
- SFTP destination: Upload to path
- Webhook destination: POST to URL
- Destination mapping (column → field mapping)
- Write modes: append, replace, upsert

---

## Phase 5: Workflows

### Milestone 19: Workflow Builder
**Goal:** Visual workflow creation

**Deliverables:**
- Workflow builder page
- `workflows` table in database
- Visual DAG editor:
  ```
  [Source] → [Module 1] → [Module 2] → [Destination]
  ```
- Drag-and-drop modules
- Connect source connection to start
- Connect destination connection to end
- Workflow validation (check compatibility)
- Save workflow with name/description

---

### Milestone 20: Workflow Execution
**Goal:** Run workflows end-to-end

**Deliverables:**
- "Run Now" button on workflow
- Execution engine:
  1. Pull from source
  2. Run through each module in sequence
  3. Push to destination
- Execution logs (per-step status)
- Error handling and step retry
- Execution history page
- Partial success handling (some steps pass, some fail)

**Tech Decision:** Celery chains for MVP, evaluate Temporal if complexity grows

---

### Milestone 21: Scheduled Workflows
**Goal:** Automate workflow execution

**Deliverables:**
- Schedule configuration UI
- Cron expression builder (or simple: hourly/daily/weekly)
- `workflow_schedules` table
- Celery Beat integration for scheduling
- Event-driven triggers:
  - New file in S3 bucket
  - Webhook trigger endpoint
- Schedule enable/disable toggle
- Next run preview
- Run history with timestamps

---

## Phase 6: Adaptive AI

### Milestone 22: Schema Drift Detection
**Goal:** Detect when incoming data format changes

**Deliverables:**
- Schema fingerprinting (hash of column names + types)
- Compare incoming schema to expected schema
- Drift detection categories:
  - New columns added
  - Columns removed
  - Type changes
  - Name changes (fuzzy match)
- Drift alert to user
- Drift report (what changed)

---

### Milestone 23: Auto-Versioning & Adaptation
**Goal:** AI automatically handles schema changes

**Deliverables:**
- When drift detected:
  1. Check if existing version handles new schema
  2. If yes → use that version
  3. If no → AI builds new version
- Auto-version creation:
  - AI analyzes new schema
  - Generates adapted transformation plan
  - Creates new module version
- Version rotation (oldest replaced when at cap)
- User notification of auto-adaptation
- Manual override option

---

## Phase 7: Multi-Format Support (Future)

### Milestone 24: Excel Support
- .xlsx and .xls file upload
- Excel-specific profiling (sheets, named ranges)
- Sheet selection UI
- Excel output option

### Milestone 25: JSON Support
- .json and .jsonl file upload
- JSON flattening (nested → tabular)
- JSON-specific transformations
- JSON output option

### Milestone 26: Database Direct Mode
- Skip file upload, query database directly
- Live connection to source database
- Transform query results
- Write back to database

---

## Visual Timeline

```
Phase 1 ✅ COMPLETE
├── Milestones 1-9: Core API

Phase 2: Foundation + Web UI
├── 9.5: Auth & Real-Time Foundation
├── 10: Frontend & Node Graph
├── 11: Chat Interface Integration
└── 12: Node Inspector & Code Editor

Phase 3: Module System
├── 13: Module Creation
├── 14: Module Versioning
└── 15: Module Library

Phase 4: Connections
├── 16: Connection Management
├── 17: Source Connectors
└── 18: Destination Connectors

Phase 5: Workflows
├── 19: Workflow Builder
├── 20: Workflow Execution
└── 21: Scheduled Workflows

Phase 6: Adaptive AI
├── 22: Schema Drift Detection
└── 23: Auto-Versioning

Phase 7: Multi-Format (Future)
├── 24: Excel Support
├── 25: JSON Support
└── 26: Database Direct Mode
```

---

## Success Metrics by Phase

| Phase | Key Metric |
|-------|------------|
| Phase 2 | Users can transform CSV via web UI |
| Phase 3 | Users can save and reuse modules |
| Phase 4 | Users can connect to S3/databases |
| Phase 5 | Users can run automated pipelines |
| Phase 6 | System auto-adapts to schema changes |
| Phase 7 | Support for Excel, JSON, direct DB |

---

## Dependencies

```
Phase 2 (UI)
    └── Requires: Phase 1 API ✅

Phase 3 (Modules)
    └── Requires: Phase 2 UI

Phase 4 (Connections)
    └── Requires: Phase 2 Auth
    └── Requires: Phase 3 Modules (to connect to)

Phase 5 (Workflows)
    └── Requires: Phase 3 Modules
    └── Requires: Phase 4 Connections

Phase 6 (Adaptive AI)
    └── Requires: Phase 3 Module Versioning

Phase 7 (Multi-Format)
    └── Can start after Phase 2
    └── Independent of Phases 4-6
```

---

*This roadmap is subject to adjustment based on user feedback and business priorities.*
