# ModularData Deployment Guide

This guide covers deploying ModularData to production environments.

---

## Architecture Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Client    │────▶│  API Server │────▶│   Supabase  │
│  (Postman)  │     │  (FastAPI)  │     │ (PostgreSQL)│
└─────────────┘     └──────┬──────┘     └─────────────┘
                          │
                          ▼
                   ┌─────────────┐     ┌─────────────┐
                   │    Redis    │◀────│   Worker    │
                   │  (Broker)   │     │  (Celery)   │
                   └─────────────┘     └─────────────┘
```

**Services:**
- **API Server**: FastAPI application handling HTTP requests
- **Worker**: Celery worker processing background tasks (AI transformations)
- **Redis**: Message broker connecting API to workers
- **Supabase**: PostgreSQL database + file storage (external service)

---

## Option 1: Railway Deployment (Recommended)

Railway provides easy deployment with automatic scaling and managed Redis.

### Step 1: Create Railway Project

1. Go to [railway.app](https://railway.app) and create account
2. Create new project: "ModularData"

### Step 2: Add Redis

1. Click "Add Service" → "Database" → "Redis"
2. Railway auto-provisions Redis and sets `REDIS_URL`

### Step 3: Deploy API Service

1. Click "Add Service" → "GitHub Repo"
2. Select your ModularData repository
3. Configure:
   - **Service Name**: `api`
   - **Start Command**: `web` (from Procfile)
   - **Root Directory**: `/`

4. Add environment variables:
   ```
   ENVIRONMENT=production
   DEBUG=false
   SUPABASE_URL=https://xxx.supabase.co
   SUPABASE_KEY=your-anon-key
   SUPABASE_SERVICE_KEY=your-service-key
   OPENAI_API_KEY=sk-xxx
   CORS_ORIGINS=https://your-frontend.com
   ```

5. Railway auto-links `REDIS_URL` from the Redis service

### Step 4: Deploy Worker Service

1. Click "Add Service" → "GitHub Repo" (same repo)
2. Configure:
   - **Service Name**: `worker`
   - **Start Command**: `worker` (from Procfile)

3. Add same environment variables as API
4. Set worker-specific vars:
   ```
   CELERY_CONCURRENCY=4
   ```

### Step 5: Configure Domain

1. Click on API service → "Settings" → "Domains"
2. Generate Railway domain or add custom domain

### Scaling Workers

To handle more concurrent requests:
1. Go to Worker service → "Settings"
2. Increase `CELERY_CONCURRENCY` (e.g., 8)
3. Or add more worker replicas in Railway

---

## Option 2: Docker Compose (Self-Hosted)

For VPS or on-premise deployment.

### Prerequisites

- Docker & Docker Compose installed
- Domain with SSL (use Caddy or nginx-proxy)

### Step 1: Clone Repository

```bash
git clone https://github.com/your-repo/modulardata.git
cd modulardata
```

### Step 2: Configure Environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

Required variables:
```env
ENVIRONMENT=production
DEBUG=false
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_KEY=your-anon-key
SUPABASE_SERVICE_KEY=your-service-key
OPENAI_API_KEY=sk-xxx
REDIS_URL=redis://redis:6379/0
CORS_ORIGINS=https://your-domain.com
```

### Step 3: Build and Run

```bash
# Build images
docker-compose build

# Start services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### Step 4: Reverse Proxy (nginx)

Example nginx configuration:

```nginx
server {
    listen 443 ssl http2;
    server_name api.yourdomain.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

---

## Option 3: Kubernetes (Advanced)

For high-availability deployments.

### Kubernetes Resources Needed:

1. **Deployment**: API (replicas: 2-3)
2. **Deployment**: Worker (replicas: 2-4)
3. **Service**: API (ClusterIP or LoadBalancer)
4. **StatefulSet**: Redis (or use managed Redis)
5. **ConfigMap**: Environment variables
6. **Secret**: API keys and credentials
7. **HPA**: Auto-scaling based on CPU/memory

Contact for Kubernetes manifests if needed.

---

## Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `ENVIRONMENT` | Yes | `development` or `production` |
| `DEBUG` | No | Enable debug mode (default: false) |
| `SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_KEY` | Yes | Supabase anon/public key |
| `SUPABASE_SERVICE_KEY` | Yes | Supabase service role key |
| `OPENAI_API_KEY` | Yes | OpenAI API key for AI agents |
| `REDIS_URL` | Yes | Redis connection URL |
| `CORS_ORIGINS` | No | Allowed CORS origins (comma-separated) |
| `CELERY_CONCURRENCY` | No | Worker concurrency (default: 4) |

---

## Health Checks

The API provides health check endpoints for monitoring:

| Endpoint | Purpose |
|----------|---------|
| `GET /api/v1/health` | Full health check with version |
| `GET /api/v1/health/live` | Liveness probe (is the process alive?) |
| `GET /api/v1/health/ready` | Readiness probe (can it serve requests?) |

### Monitoring Setup

1. **Uptime Monitoring**: Point your monitoring service to `/api/v1/health/live`
2. **Load Balancer**: Use `/api/v1/health/ready` for backend health
3. **Alerting**: Set up alerts for non-200 responses

---

## Scaling Guidelines

### API Server
- Stateless - scale horizontally
- Recommended: 2+ replicas for high availability
- Memory: ~256MB per instance
- CPU: 0.25-0.5 vCPU per instance

### Celery Worker
- CPU-intensive (AI processing)
- Recommended: 2-4 workers to start
- Memory: ~512MB per worker
- CPU: 0.5-1 vCPU per worker
- Scale based on task queue depth

### Redis
- Low resource requirements
- Memory: 128MB-512MB depending on queue size
- Persistence: Enable AOF for durability

---

## Troubleshooting

### API not responding
```bash
# Check API logs
docker-compose logs api

# Verify Redis connection
docker-compose exec api python -c "import redis; r=redis.from_url('redis://redis:6379'); print(r.ping())"
```

### Tasks stuck in PENDING
```bash
# Check worker logs
docker-compose logs worker

# Verify worker is connected
docker-compose exec worker celery -A workers.celery_app inspect active
```

### Database connection errors
- Verify `SUPABASE_URL` and keys are correct
- Check Supabase dashboard for connection limits
- Ensure IP is whitelisted if using IP restrictions

---

## Backup & Recovery

### Database (Supabase)
- Supabase handles automatic backups
- Enable Point-in-Time Recovery in Supabase dashboard

### Redis
- Enable Redis persistence (AOF mode) in production
- Configure periodic snapshots

### File Storage
- Files stored in Supabase Storage
- Supabase handles replication automatically

---

## Security Checklist

- [ ] Set `DEBUG=false` in production
- [ ] Use strong, unique API keys
- [ ] Enable CORS with specific origins only
- [ ] Use HTTPS (SSL/TLS)
- [ ] Run containers as non-root user
- [ ] Keep dependencies updated
- [ ] Monitor for suspicious activity
- [ ] Set up rate limiting (nginx/API gateway)
