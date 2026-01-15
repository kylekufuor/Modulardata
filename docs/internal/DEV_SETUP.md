# Development Setup

> **Last Updated:** 2026-01-15

This guide covers setting up a local development environment for ModularData.

---

## Prerequisites

- **Python 3.10+** - Required for modern type hints
- **Poetry** - Python dependency management
- **Redis** - Local task queue (or use Railway Redis)
- **Git** - Version control

---

## Quick Setup

```bash
# 1. Clone repository
git clone <repo-url>
cd modulardata

# 2. Install dependencies
poetry install

# 3. Copy environment template
cp .env.example .env

# 4. Edit .env with your credentials
# (see Environment Variables section)

# 5. Start the API server
poetry run uvicorn app.main:app --reload

# 6. Start the Celery worker (in another terminal)
poetry run celery -A workers.celery_app worker --loglevel=info
```

---

## Environment Setup

### 1. Create `.env` File

```bash
# Required - Supabase
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=eyJ...
SUPABASE_SERVICE_KEY=eyJ...

# Required - OpenAI
OPENAI_API_KEY=sk-...

# Required for async tasks
REDIS_URL=redis://localhost:6379/0

# Optional - defaults shown
ENVIRONMENT=development
DEBUG=true
API_HOST=0.0.0.0
API_PORT=8000
```

### 2. Install Redis Locally

**macOS:**
```bash
brew install redis
brew services start redis
```

**Linux:**
```bash
sudo apt install redis-server
sudo systemctl start redis
```

**Or use Docker:**
```bash
docker run -d -p 6379:6379 redis:alpine
```

### 3. Supabase Setup

1. Create project at [supabase.com](https://supabase.com)
2. Copy URL and keys from Project Settings > API
3. Run migrations (see Database Schema doc)

---

## Running the Application

### API Server

```bash
# Development with auto-reload
poetry run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Or use Poetry script
poetry run python -m uvicorn app.main:app --reload
```

**Access:**
- API: http://localhost:8000
- Docs: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Celery Worker

```bash
# Standard worker
poetry run celery -A workers.celery_app worker --loglevel=info

# With concurrency
poetry run celery -A workers.celery_app worker --concurrency=4 --loglevel=info
```

### Running Both

Use two terminals, or:

```bash
# Terminal 1
poetry run uvicorn app.main:app --reload &

# Terminal 2
poetry run celery -A workers.celery_app worker --loglevel=info
```

---

## Testing

### Run All Tests

```bash
poetry run pytest
```

### Run Specific Tests

```bash
# Single file
poetry run pytest tests/test_profiler.py

# Single test
poetry run pytest tests/test_profiler.py::test_profile_basic

# With verbose output
poetry run pytest -v

# With print statements
poetry run pytest -s
```

### Test Coverage

```bash
poetry run pytest --cov=app --cov=agents --cov=lib
```

---

## Project Structure

```
modulardata/
├── app/                    # FastAPI application
│   ├── main.py            # Entry point
│   ├── config.py          # Settings
│   ├── routers/           # API endpoints
│   └── exceptions.py      # Error handling
├── agents/                 # AI agents
│   ├── strategist.py      # Intent parsing
│   ├── engineer.py        # Execution
│   └── transformations/   # 53 transformations
├── core/                   # Domain logic
│   ├── models/            # Pydantic models
│   └── services/          # Business logic
├── lib/                    # Shared utilities
│   ├── supabase_client.py # Database client
│   └── profiler.py        # Data analysis
├── workers/                # Celery tasks
│   ├── celery_app.py      # Worker config
│   └── tasks.py           # Task definitions
├── tests/                  # Test suite
├── scripts/                # Dev scripts
├── docs/                   # Documentation
├── pyproject.toml          # Dependencies
└── .env                    # Environment vars
```

---

## Common Tasks

### Adding a New Transformation

1. Choose the appropriate module in `agents/transformations/`
2. Add the transformation type to `agents/models/technical_plan.py`
3. Implement the function with the `@register_transformation` decorator

```python
# In agents/transformations/cleaning.py

@register_transformation(TransformationType.YOUR_NEW_TYPE)
def your_new_transformation(df: pd.DataFrame, plan: TechnicalPlan) -> pd.DataFrame:
    """
    Description of what this does.
    """
    # Implementation
    return df
```

4. Update `agents/prompts/strategist_system.py` to include the new type
5. Add tests in `tests/test_transformations.py`

### Adding a New API Endpoint

1. Create or modify a router in `app/routers/`
2. Add route with proper decorators
3. Include in `app/main.py` if new router
4. Document in API Reference

### Debugging AI Responses

```python
# In scripts/test_strategist_live.py
# Modify and run to test AI interpretation

python scripts/test_strategist_live.py
```

---

## IDE Setup

### VS Code

Recommended extensions:
- Python (Microsoft)
- Pylance
- Python Indent

`.vscode/settings.json`:
```json
{
    "python.defaultInterpreterPath": ".venv/bin/python",
    "python.formatting.provider": "black",
    "editor.formatOnSave": true
}
```

### PyCharm

1. Set Python interpreter to Poetry venv
2. Mark `app`, `agents`, `core`, `lib` as Sources Root

---

## Troubleshooting

### "Module not found" errors

```bash
# Ensure you're in the Poetry environment
poetry shell

# Or prefix commands
poetry run python ...
```

### Redis connection refused

```bash
# Check Redis is running
redis-cli ping

# Should return: PONG
```

### Supabase errors

1. Verify credentials in `.env`
2. Check Supabase dashboard for service status
3. Ensure tables exist (run migrations)

### Task stuck in PENDING

1. Verify worker is running
2. Check Redis connection
3. Look at worker logs for errors

---

## Related Documentation

- [Architecture Overview](ARCHITECTURE.md)
- [Deployment Guide](DEPLOYMENT.md)
- [Environment Variables](ENV_VARIABLES.md)
