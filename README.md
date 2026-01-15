# ModularData API

AI-powered data transformation API using a 3-agent pipeline.

## Quick Start

```bash
# Install dependencies
poetry install

# Start Redis
brew services start redis

# Start worker (in separate terminal)
poetry run celery -A workers.celery_app worker --loglevel=info

# Start API
poetry run uvicorn app.main:app --reload
```

## API Documentation

Once running, visit: http://localhost:8000/docs

## Environment Variables

Copy `.env.example` to `.env` and fill in your credentials:
- `SUPABASE_URL` - Your Supabase project URL
- `SUPABASE_KEY` - Supabase anon key
- `SUPABASE_SERVICE_KEY` - Supabase service role key
- `OPENAI_API_KEY` - OpenAI API key
- `REDIS_URL` - Redis connection URL
