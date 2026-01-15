# Deployment Guide

> **Last Updated:** 2026-01-15

ModularData is deployed on Railway with Supabase for database/storage.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Railway                               │
│                                                              │
│  ┌─────────────────┐    ┌─────────────────┐                 │
│  │  Web Service    │    │  Worker Service │                 │
│  │  (FastAPI)      │    │  (Celery)       │                 │
│  │                 │    │                 │                 │
│  │  Port: $PORT    │    │  No port        │                 │
│  └────────┬────────┘    └────────┬────────┘                 │
│           │                      │                          │
│           └──────────┬───────────┘                          │
│                      │                                      │
│              ┌───────┴───────┐                              │
│              │    Redis      │                              │
│              │  (Internal)   │                              │
│              └───────────────┘                              │
└─────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                      Supabase                                │
│                                                              │
│  ┌─────────────────┐    ┌─────────────────┐                 │
│  │   PostgreSQL    │    │    Storage      │                 │
│  │   (Database)    │    │  (CSV Files)    │                 │
│  └─────────────────┘    └─────────────────┘                 │
└─────────────────────────────────────────────────────────────┘
```

---

## Railway Services

### Web Service

**Purpose:** Serves the FastAPI REST API

**Configuration:**
- Build: Nixpacks (auto-detected Python)
- Start Command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- Health Check: `/api/v1/health`

### Worker Service

**Purpose:** Runs Celery for async task processing

**Configuration:**
- Build: Nixpacks
- Start Command: `celery -A workers.celery_app worker --loglevel=info`
- No exposed port (internal only)

### Redis

**Purpose:** Message broker for Celery

**Configuration:**
- Railway Redis plugin
- Internal URL provided via `REDIS_URL`

---

## Deployment Steps

### Initial Deployment

1. **Create Railway Project**
   ```bash
   # Install Railway CLI
   npm install -g @railway/cli

   # Login
   railway login

   # Initialize project
   railway init
   ```

2. **Add Services**
   - Create "web" service from repo
   - Create "worker" service from same repo
   - Add Redis plugin

3. **Configure Environment Variables**

   For both web and worker services:
   ```
   SUPABASE_URL=https://xxx.supabase.co
   SUPABASE_ANON_KEY=eyJ...
   SUPABASE_SERVICE_KEY=eyJ...
   OPENAI_API_KEY=sk-...
   REDIS_URL=${{Redis.REDIS_URL}}
   ENVIRONMENT=production
   DEBUG=false
   ```

4. **Set Start Commands**

   Web service:
   ```
   uvicorn app.main:app --host 0.0.0.0 --port $PORT
   ```

   Worker service:
   ```
   celery -A workers.celery_app worker --loglevel=info
   ```

5. **Deploy**
   ```bash
   railway up
   ```

### Updating Deployment

```bash
# Push to main branch (if GitHub connected)
git push origin main

# Or manual deploy
railway up
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_ANON_KEY` | Yes | Supabase anon key |
| `SUPABASE_SERVICE_KEY` | Yes | Supabase service key |
| `OPENAI_API_KEY` | Yes | OpenAI API key |
| `REDIS_URL` | Yes | Redis connection URL |
| `ENVIRONMENT` | No | `production` recommended |
| `DEBUG` | No | `false` for production |
| `SECRET_KEY` | No | Random string for tokens |
| `CORS_ORIGINS` | No | Allowed CORS origins |

See [ENV_VARIABLES.md](ENV_VARIABLES.md) for full documentation.

---

## Supabase Setup

### Database Tables

Run these SQL commands in Supabase SQL Editor:

```sql
-- Sessions table
CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status TEXT DEFAULT 'active',
    original_filename TEXT,
    current_node_id UUID,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Nodes table (version control)
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

-- Chat logs
CREATE TABLE chat_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES sessions(id) ON DELETE CASCADE,
    node_id UUID REFERENCES nodes(id),
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Plans
CREATE TABLE plans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES sessions(id) ON DELETE CASCADE,
    status TEXT DEFAULT 'planning',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Plan steps
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

-- Indexes
CREATE INDEX idx_nodes_session ON nodes(session_id);
CREATE INDEX idx_chat_logs_session ON chat_logs(session_id);
CREATE INDEX idx_plans_session ON plans(session_id);
```

### Storage Bucket

1. Go to Storage in Supabase dashboard
2. Create bucket named `csv-files`
3. Set to private (service key access only)

---

## Health Checks

### Endpoints

| Endpoint | Purpose | Expected Response |
|----------|---------|-------------------|
| `/api/v1/health` | Basic check | `{"status": "healthy"}` |
| `/api/v1/health/ready` | DB + storage | `{"status": "ready"}` |
| `/api/v1/health/live` | Liveness | `{"status": "alive"}` |

### Railway Health Check

Configure in Railway service settings:
- Path: `/api/v1/health`
- Timeout: 10s
- Interval: 30s

---

## Monitoring

### Logs

View logs in Railway dashboard or CLI:
```bash
railway logs
railway logs --service web
railway logs --service worker
```

### Metrics

Railway provides:
- CPU/Memory usage
- Request count
- Response times

---

## Troubleshooting

### Task stuck in PENDING

1. Check worker service is running
2. Verify `REDIS_URL` is set in worker env
3. Check worker logs for errors:
   ```bash
   railway logs --service worker
   ```

### 500 errors

1. Check web service logs
2. Verify all env variables are set
3. Test Supabase connectivity

### Database connection errors

1. Verify Supabase credentials
2. Check Supabase service status
3. Ensure IP isn't blocked

### Redis connection errors

1. Check Redis service is running
2. Verify `REDIS_URL` format
3. Check Railway Redis logs

---

## Rollback

### Code Rollback

```bash
# Revert to previous commit
git revert HEAD
git push origin main
```

### Database Rollback

Use Supabase point-in-time recovery (Pro plan) or manual backup restoration.

---

## Cost Optimization

| Resource | Usage Pattern | Optimization |
|----------|---------------|--------------|
| Railway Web | Per request | Scale down during low traffic |
| Railway Worker | Always on | Consider spot instances |
| Redis | Per connection | Single instance sufficient |
| Supabase | Per GB stored | Clean up old sessions |
| OpenAI | Per token | Use GPT-4 Turbo for efficiency |

---

## Related Documentation

- [Architecture Overview](ARCHITECTURE.md)
- [Environment Variables](ENV_VARIABLES.md)
- [Database Schema](DATABASE_SCHEMA.md)
