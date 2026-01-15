# Environment Variables

> **Last Updated:** 2026-01-15

Complete reference for all ModularData environment variables.

---

## Required Variables

These must be set for the application to start.

### Supabase

| Variable | Description | Example |
|----------|-------------|---------|
| `SUPABASE_URL` | Supabase project URL | `https://abc123.supabase.co` |
| `SUPABASE_ANON_KEY` | Public/anon API key | `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...` |
| `SUPABASE_SERVICE_KEY` | Service role key (bypasses RLS) | `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...` |

**Where to find:**
1. Go to [Supabase Dashboard](https://supabase.com/dashboard)
2. Select your project
3. Go to Settings > API
4. Copy the URL and keys

### OpenAI

| Variable | Description | Example |
|----------|-------------|---------|
| `OPENAI_API_KEY` | OpenAI API key for AI agents | `sk-proj-abc123...` |

**Where to find:**
1. Go to [OpenAI Platform](https://platform.openai.com/api-keys)
2. Create new secret key
3. Copy immediately (shown only once)

### Redis

| Variable | Description | Example |
|----------|-------------|---------|
| `REDIS_URL` | Redis connection URL | `redis://localhost:6379/0` |

**Format:** `redis://[user:password@]host:port/db`

**Railway:** Use `${{Redis.REDIS_URL}}` to reference internal Redis

---

## Optional Variables

These have sensible defaults but can be customized.

### Application

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT` | `development` | Environment mode: `development`, `staging`, `production` |
| `DEBUG` | `false` | Enable debug logging and features |
| `API_HOST` | `0.0.0.0` | Host to bind API server |
| `API_PORT` | `8000` | Port for API server |

### AI Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_MODEL` | `gpt-4-turbo` | Model for AI agents (must support JSON mode) |
| `STRATEGIST_MAX_MESSAGES` | `10` | Max conversation messages in agent context |
| `STRATEGIST_TEMPERATURE` | `0.2` | AI temperature (0-2, lower = more consistent) |

### Security

| Variable | Default | Description |
|----------|---------|-------------|
| `SECRET_KEY` | `dev-secret-key-...` | Secret for signing tokens. **Change in production!** |
| `CORS_ORIGINS` | `http://localhost:3000` | Allowed CORS origins (comma-separated) |

### File Upload

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_UPLOAD_SIZE_MB` | `50` | Maximum file upload size in MB |
| `ALLOWED_EXTENSIONS` | `.csv` | Allowed file extensions (comma-separated) |

---

## Environment-Specific Configs

### Development (`.env`)

```bash
# Supabase
SUPABASE_URL=https://your-dev-project.supabase.co
SUPABASE_ANON_KEY=eyJ...
SUPABASE_SERVICE_KEY=eyJ...

# OpenAI
OPENAI_API_KEY=sk-...

# Redis (local)
REDIS_URL=redis://localhost:6379/0

# Development settings
ENVIRONMENT=development
DEBUG=true
API_PORT=8000

# CORS (allow all in dev)
CORS_ORIGINS=*
```

### Production (Railway)

```bash
# Supabase
SUPABASE_URL=https://your-prod-project.supabase.co
SUPABASE_ANON_KEY=eyJ...
SUPABASE_SERVICE_KEY=eyJ...

# OpenAI
OPENAI_API_KEY=sk-...

# Redis (Railway internal)
REDIS_URL=${{Redis.REDIS_URL}}

# Production settings
ENVIRONMENT=production
DEBUG=false
SECRET_KEY=generate-a-random-32-char-string

# CORS (restrict to your domains)
CORS_ORIGINS=https://yourapp.com,https://www.yourapp.com
```

---

## Validation

The application validates environment variables on startup using Pydantic.

**If a required variable is missing:**
```
pydantic_core._pydantic_core.ValidationError: 1 validation error for Settings
SUPABASE_URL
  Field required [type=missing, input_value={}, input_type=dict]
```

**If a variable has invalid format:**
```
pydantic_core._pydantic_core.ValidationError: 1 validation error for Settings
STRATEGIST_TEMPERATURE
  Input should be less than or equal to 2 [type=less_than_equal, ...]
```

---

## Adding New Variables

1. Add to `app/config.py`:

```python
class Settings(BaseSettings):
    # ... existing fields ...

    MY_NEW_VAR: str = Field(
        default="default_value",  # or ... for required
        description="What this variable does"
    )
```

2. Update this documentation
3. Update `.env.example` if you have one
4. Update Railway environment variables

---

## Security Notes

1. **Never commit `.env` files** - Add to `.gitignore`
2. **Use different keys per environment** - Dev vs Prod
3. **Rotate keys regularly** - Especially `SECRET_KEY`
4. **Service key is powerful** - Bypasses all Supabase RLS

---

## Troubleshooting

### Variable not loading

```bash
# Check if .env file exists
cat .env

# Verify Poetry is loading it
poetry run python -c "from app.config import settings; print(settings.SUPABASE_URL)"
```

### Wrong value being used

Environment variables override `.env` file. Check system env:

```bash
echo $SUPABASE_URL
```

### Railway not picking up changes

1. Redeploy after changing env vars
2. Use Railway Dashboard > Variables section
3. Don't prefix with `export` in Railway

---

## Related Documentation

- [Development Setup](DEV_SETUP.md)
- [Deployment Guide](DEPLOYMENT.md)
- [Architecture Overview](ARCHITECTURE.md)
