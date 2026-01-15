# Database Schema

> **Last Updated:** 2026-01-15

ModularData uses Supabase PostgreSQL. This document describes the complete database schema.

---

## Entity Relationship Diagram

```
┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│  sessions   │───────│    nodes    │───────│  chat_logs  │
└─────────────┘       └─────────────┘       └─────────────┘
      │                     │
      │                     │ (self-reference)
      │                     ▼
      │               ┌─────────────┐
      │               │   parent    │
      │               └─────────────┘
      │
      ▼
┌─────────────┐       ┌─────────────┐
│    plans    │───────│ plan_steps  │
└─────────────┘       └─────────────┘
```

---

## Tables

### sessions

Represents a data transformation workspace.

```sql
CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status TEXT DEFAULT 'active',
    original_filename TEXT,
    current_node_id UUID,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | UUID | No | Primary key |
| `status` | TEXT | No | `active`, `archived`, `deleted` |
| `original_filename` | TEXT | Yes | Name of uploaded CSV |
| `current_node_id` | UUID | Yes | Points to current data version |
| `created_at` | TIMESTAMPTZ | No | Creation timestamp |
| `updated_at` | TIMESTAMPTZ | No | Last update timestamp |

**Indexes:**
```sql
-- Primary key index (automatic)
```

---

### nodes

Represents a version/snapshot of the data. Forms a linked list via `parent_id`.

```sql
CREATE TABLE nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES sessions(id) ON DELETE CASCADE,
    parent_id UUID REFERENCES nodes(id),
    storage_path TEXT,
    transformation TEXT,
    transformation_code TEXT,
    profile_json JSONB,
    row_count INTEGER,
    column_count INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | UUID | No | Primary key |
| `session_id` | UUID | No | Parent session |
| `parent_id` | UUID | Yes | Previous version (null for original) |
| `storage_path` | TEXT | Yes | Path in Supabase Storage |
| `transformation` | TEXT | Yes | Human-readable description |
| `transformation_code` | TEXT | Yes | Executed pandas code |
| `profile_json` | JSONB | Yes | Data profile (columns, types, stats) |
| `row_count` | INTEGER | Yes | Number of rows |
| `column_count` | INTEGER | Yes | Number of columns |
| `created_at` | TIMESTAMPTZ | No | Creation timestamp |

**Indexes:**
```sql
CREATE INDEX idx_nodes_session ON nodes(session_id);
CREATE INDEX idx_nodes_parent ON nodes(parent_id);
```

**Profile JSON Structure:**
```json
{
    "row_count": 100,
    "column_count": 5,
    "columns": [
        {
            "name": "email",
            "dtype": "object",
            "inferred_type": "email",
            "null_count": 3,
            "unique_count": 97,
            "sample_values": ["a@b.com", "c@d.com"]
        }
    ],
    "issues": [
        {
            "type": "null_values",
            "column": "email",
            "count": 3
        }
    ],
    "duplicate_row_count": 0
}
```

---

### chat_logs

Stores conversation history for context.

```sql
CREATE TABLE chat_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES sessions(id) ON DELETE CASCADE,
    node_id UUID REFERENCES nodes(id),
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | UUID | No | Primary key |
| `session_id` | UUID | No | Parent session |
| `node_id` | UUID | Yes | Associated node (if applicable) |
| `role` | TEXT | No | `user` or `assistant` |
| `content` | TEXT | No | Message text |
| `metadata` | JSONB | No | Additional data (plan info, etc.) |
| `created_at` | TIMESTAMPTZ | No | Message timestamp |

**Indexes:**
```sql
CREATE INDEX idx_chat_logs_session ON chat_logs(session_id);
CREATE INDEX idx_chat_logs_created ON chat_logs(created_at);
```

---

### plans

Represents a batch of planned transformations.

```sql
CREATE TABLE plans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES sessions(id) ON DELETE CASCADE,
    status TEXT DEFAULT 'planning',
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | UUID | No | Primary key |
| `session_id` | UUID | No | Parent session |
| `status` | TEXT | No | `planning`, `applied`, `cancelled` |
| `created_at` | TIMESTAMPTZ | No | Creation timestamp |

**Status Values:**
- `planning` - Active plan, accepting new steps
- `applied` - Plan has been executed
- `cancelled` - Plan was cleared/cancelled

**Indexes:**
```sql
CREATE INDEX idx_plans_session ON plans(session_id);
CREATE INDEX idx_plans_status ON plans(status);
```

---

### plan_steps

Individual transformation steps within a plan.

```sql
CREATE TABLE plan_steps (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plan_id UUID REFERENCES plans(id) ON DELETE CASCADE,
    step_number INTEGER NOT NULL,
    transformation_type TEXT NOT NULL,
    target_columns TEXT[],
    parameters JSONB DEFAULT '{}',
    explanation TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | UUID | No | Primary key |
| `plan_id` | UUID | No | Parent plan |
| `step_number` | INTEGER | No | Execution order |
| `transformation_type` | TEXT | No | Type enum value (e.g., `drop_rows`) |
| `target_columns` | TEXT[] | Yes | Columns affected |
| `parameters` | JSONB | No | Transformation parameters |
| `explanation` | TEXT | Yes | Human-readable description |
| `created_at` | TIMESTAMPTZ | No | Step creation timestamp |

**Indexes:**
```sql
CREATE INDEX idx_plan_steps_plan ON plan_steps(plan_id);
```

**Example Parameters:**
```json
// drop_rows
{"condition": "email IS NULL"}

// fill_nulls
{"column": "age", "method": "mean"}

// rename_column
{"old_name": "fname", "new_name": "first_name"}
```

---

## Storage

### Bucket: csv-files

Supabase Storage bucket for CSV files.

**Path Format:** `sessions/{session_id}/{node_id}.csv`

**Permissions:** Private (service key access only)

---

## Complete Schema SQL

```sql
-- Run this in Supabase SQL Editor to set up all tables

-- 1. Sessions
CREATE TABLE IF NOT EXISTS sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status TEXT DEFAULT 'active',
    original_filename TEXT,
    current_node_id UUID,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Nodes
CREATE TABLE IF NOT EXISTS nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES sessions(id) ON DELETE CASCADE,
    parent_id UUID REFERENCES nodes(id),
    storage_path TEXT,
    transformation TEXT,
    transformation_code TEXT,
    profile_json JSONB,
    row_count INTEGER,
    column_count INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nodes_session ON nodes(session_id);
CREATE INDEX IF NOT EXISTS idx_nodes_parent ON nodes(parent_id);

-- 3. Chat Logs
CREATE TABLE IF NOT EXISTS chat_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES sessions(id) ON DELETE CASCADE,
    node_id UUID REFERENCES nodes(id),
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_logs_session ON chat_logs(session_id);
CREATE INDEX IF NOT EXISTS idx_chat_logs_created ON chat_logs(created_at);

-- 4. Plans
CREATE TABLE IF NOT EXISTS plans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES sessions(id) ON DELETE CASCADE,
    status TEXT DEFAULT 'planning',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_plans_session ON plans(session_id);
CREATE INDEX IF NOT EXISTS idx_plans_status ON plans(status);

-- 5. Plan Steps
CREATE TABLE IF NOT EXISTS plan_steps (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plan_id UUID REFERENCES plans(id) ON DELETE CASCADE,
    step_number INTEGER NOT NULL,
    transformation_type TEXT NOT NULL,
    target_columns TEXT[],
    parameters JSONB DEFAULT '{}',
    explanation TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_plan_steps_plan ON plan_steps(plan_id);

-- 6. Add foreign key for current_node_id (after nodes table exists)
ALTER TABLE sessions
ADD CONSTRAINT fk_current_node
FOREIGN KEY (current_node_id) REFERENCES nodes(id);
```

---

## Migrations

### Adding a Column

```sql
ALTER TABLE sessions ADD COLUMN user_id UUID;
```

### Adding an Index

```sql
CREATE INDEX idx_sessions_user ON sessions(user_id);
```

### Renaming a Column

```sql
ALTER TABLE nodes RENAME COLUMN profile_json TO data_profile;
```

---

## Backup & Recovery

Supabase provides:
- **Daily backups** (Pro plan)
- **Point-in-time recovery** (Pro plan)
- **Manual export** via pg_dump

---

## Related Documentation

- [Architecture Overview](ARCHITECTURE.md)
- [Deployment Guide](DEPLOYMENT.md)
