# Milestone 9.5: Platform Foundation

> **Status:** Planning
> **Dependencies:** Milestones 1-9 (Complete)
> **Blocked By:** None

---

## Overview

This milestone adds authentication and real-time capabilities - the foundation for the web UI and multi-tenant platform.

### Goals
1. Add Supabase Auth integration
2. Protect all API endpoints
3. Add WebSocket for real-time updates
4. Migrate database schema for multi-tenancy

### Decisions Made
- **Auth:** Supabase Auth (email/password, JWT)
- **Existing Data:** Delete all existing sessions (clean slate)
- **WebSocket Events:** Node creation + task complete (MVP scope)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              Client                                       │
│                                                                          │
│  ┌─────────────────┐              ┌─────────────────┐                   │
│  │  REST API       │              │  WebSocket      │                   │
│  │  (with JWT)     │              │  /ws/sessions/  │                   │
│  └────────┬────────┘              └────────┬────────┘                   │
└───────────┼────────────────────────────────┼────────────────────────────┘
            │                                │
            ▼                                ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                           FastAPI Backend                                  │
│                                                                            │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐        │
│  │  Auth Middleware │  │  Auth Dependency │  │  WebSocket       │        │
│  │  (verify JWT)    │  │  (get_current_   │  │  Manager         │        │
│  │                  │  │   user)          │  │  (connections)   │        │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘        │
│           │                     │                     │                   │
│           ▼                     ▼                     ▼                   │
│  ┌─────────────────────────────────────────────────────────────────┐     │
│  │                        Routers                                   │     │
│  │  sessions │ upload │ chat │ data │ history │ tasks │ health     │     │
│  └─────────────────────────────────────────────────────────────────┘     │
└───────────────────────────────────────────────────────────────────────────┘
            │                                │
            ▼                                ▼
┌──────────────────────┐          ┌──────────────────────┐
│   Supabase Auth      │          │   Supabase Database  │
│   (auth.users)       │          │   (with RLS)         │
└──────────────────────┘          └──────────────────────┘
```

---

## Deliverables

### 1. Database Migration

**File:** `migrations/001_add_auth.sql`

```sql
-- =============================================================================
-- Migration: Add Authentication Support
-- =============================================================================

-- 1. Create public users table (mirrors auth.users)
CREATE TABLE IF NOT EXISTS public.users (
    id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    email TEXT UNIQUE,
    display_name TEXT,
    avatar_url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Function to auto-create public user on signup
CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO public.users (id, email)
    VALUES (NEW.id, NEW.email);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 3. Trigger for new user creation
DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
    AFTER INSERT ON auth.users
    FOR EACH ROW EXECUTE FUNCTION public.handle_new_user();

-- 4. Add user_id to sessions (NOT NULL after migration)
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES public.users(id);

-- 5. Delete existing sessions (clean slate)
DELETE FROM sessions;

-- 6. Make user_id required
ALTER TABLE sessions ALTER COLUMN user_id SET NOT NULL;

-- 7. Enable Row Level Security
ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE nodes ENABLE ROW LEVEL SECURITY;
ALTER TABLE chat_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE plan_steps ENABLE ROW LEVEL SECURITY;

-- 8. RLS Policies for sessions
CREATE POLICY "Users can view own sessions"
    ON sessions FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "Users can create own sessions"
    ON sessions FOR INSERT
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can update own sessions"
    ON sessions FOR UPDATE
    USING (auth.uid() = user_id);

CREATE POLICY "Users can delete own sessions"
    ON sessions FOR DELETE
    USING (auth.uid() = user_id);

-- 9. RLS Policies for nodes (via session ownership)
CREATE POLICY "Users can view nodes of own sessions"
    ON nodes FOR SELECT
    USING (
        EXISTS (
            SELECT 1 FROM sessions
            WHERE sessions.id = nodes.session_id
            AND sessions.user_id = auth.uid()
        )
    );

CREATE POLICY "Users can create nodes in own sessions"
    ON nodes FOR INSERT
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM sessions
            WHERE sessions.id = nodes.session_id
            AND sessions.user_id = auth.uid()
        )
    );

-- 10. RLS Policies for chat_logs (via session ownership)
CREATE POLICY "Users can view chat_logs of own sessions"
    ON chat_logs FOR SELECT
    USING (
        EXISTS (
            SELECT 1 FROM sessions
            WHERE sessions.id = chat_logs.session_id
            AND sessions.user_id = auth.uid()
        )
    );

CREATE POLICY "Users can create chat_logs in own sessions"
    ON chat_logs FOR INSERT
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM sessions
            WHERE sessions.id = chat_logs.session_id
            AND sessions.user_id = auth.uid()
        )
    );

-- 11. RLS Policies for plans (via session ownership)
CREATE POLICY "Users can view plans of own sessions"
    ON plans FOR SELECT
    USING (
        EXISTS (
            SELECT 1 FROM sessions
            WHERE sessions.id = plans.session_id
            AND sessions.user_id = auth.uid()
        )
    );

CREATE POLICY "Users can manage plans in own sessions"
    ON plans FOR ALL
    USING (
        EXISTS (
            SELECT 1 FROM sessions
            WHERE sessions.id = plans.session_id
            AND sessions.user_id = auth.uid()
        )
    );

-- 12. RLS Policies for plan_steps (via plan -> session ownership)
CREATE POLICY "Users can view plan_steps of own plans"
    ON plan_steps FOR SELECT
    USING (
        EXISTS (
            SELECT 1 FROM plans
            JOIN sessions ON sessions.id = plans.session_id
            WHERE plans.id = plan_steps.plan_id
            AND sessions.user_id = auth.uid()
        )
    );

CREATE POLICY "Users can manage plan_steps in own plans"
    ON plan_steps FOR ALL
    USING (
        EXISTS (
            SELECT 1 FROM plans
            JOIN sessions ON sessions.id = plans.session_id
            WHERE plans.id = plan_steps.plan_id
            AND sessions.user_id = auth.uid()
        )
    );

-- 13. Index for performance
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
```

---

### 2. Auth Module

**New Directory:** `app/auth/`

#### `app/auth/__init__.py`
```python
from app.auth.dependencies import get_current_user, get_current_user_optional
from app.auth.models import AuthUser

__all__ = ["get_current_user", "get_current_user_optional", "AuthUser"]
```

#### `app/auth/models.py`
```python
from pydantic import BaseModel
from uuid import UUID
from datetime import datetime

class AuthUser(BaseModel):
    """Authenticated user from Supabase JWT."""
    id: UUID
    email: str | None = None

class UserResponse(BaseModel):
    """User response for API."""
    id: UUID
    email: str | None
    display_name: str | None
    created_at: datetime
```

#### `app/auth/dependencies.py`
```python
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from app.config import settings
from app.auth.models import AuthUser

security = HTTPBearer()

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> AuthUser:
    """
    Extract and validate user from Supabase JWT.

    Usage:
        @router.get("/protected")
        async def protected_route(user: AuthUser = Depends(get_current_user)):
            return {"user_id": user.id}
    """
    token = credentials.credentials

    try:
        # Supabase JWTs use the JWT secret from project settings
        payload = jwt.decode(
            token,
            settings.SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            audience="authenticated"
        )

        user_id = payload.get("sub")
        email = payload.get("email")

        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: missing user ID",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return AuthUser(id=user_id, email=email)

    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )

async def get_current_user_optional(
    credentials: HTTPAuthorizationCredentials | None = Depends(HTTPBearer(auto_error=False))
) -> AuthUser | None:
    """
    Optionally get current user. Returns None if no token provided.
    Useful for endpoints that work with or without auth.
    """
    if credentials is None:
        return None

    return await get_current_user(credentials)
```

#### `app/auth/routes.py`
```python
from fastapi import APIRouter, Depends
from app.auth.dependencies import get_current_user
from app.auth.models import AuthUser, UserResponse
from lib.supabase_client import SupabaseClient

router = APIRouter(prefix="/auth", tags=["Auth"])

@router.get("/me", response_model=UserResponse)
async def get_current_user_info(user: AuthUser = Depends(get_current_user)):
    """Get current authenticated user's info."""
    user_data = SupabaseClient.fetch_user(user.id)

    if not user_data:
        # User exists in auth but not in public.users (shouldn't happen)
        return UserResponse(
            id=user.id,
            email=user.email,
            display_name=None,
            created_at=None
        )

    return UserResponse(**user_data)
```

---

### 3. Update Config

**File:** `app/config.py` (additions)

```python
class Settings(BaseSettings):
    # ... existing fields ...

    # Supabase JWT Secret (for verifying tokens)
    SUPABASE_JWT_SECRET: str = Field(
        ...,
        description="Supabase JWT secret for token verification"
    )
```

**Note:** Get JWT secret from Supabase Dashboard → Settings → API → JWT Secret

---

### 4. Protect All Routers

Update each router to require authentication.

**Example: `app/routers/sessions.py`**

```python
from fastapi import APIRouter, Depends
from app.auth import get_current_user, AuthUser

router = APIRouter()

@router.post("/")
async def create_session(user: AuthUser = Depends(get_current_user)):
    """Create a new session for the authenticated user."""
    # Pass user.id to session creation
    session = await session_service.create_session(user_id=user.id)
    return session

@router.get("/")
async def list_sessions(
    user: AuthUser = Depends(get_current_user),
    page: int = 1,
    page_size: int = 10
):
    """List sessions for the authenticated user."""
    # Filter by user_id
    sessions = await session_service.list_sessions(
        user_id=user.id,
        page=page,
        page_size=page_size
    )
    return sessions
```

**Routers to update:**
- `sessions.py` - Add user filter
- `upload.py` - Verify session ownership
- `chat.py` - Verify session ownership
- `data.py` - Verify session ownership
- `history.py` - Verify session ownership
- `tasks.py` - Verify task ownership (via session)
- `health.py` - Keep public (no auth required)

---

### 5. WebSocket Module

**New Directory:** `app/websocket/`

#### `app/websocket/manager.py`
```python
from fastapi import WebSocket
from typing import Dict, Set
import logging
import json

logger = logging.getLogger(__name__)

class ConnectionManager:
    """
    Manages WebSocket connections per session.

    Usage:
        # Add connection
        await manager.connect(session_id, websocket)

        # Broadcast to session
        await manager.broadcast(session_id, {"type": "node_created", ...})

        # Remove connection
        manager.disconnect(session_id, websocket)
    """

    def __init__(self):
        # session_id -> set of WebSocket connections
        self.connections: Dict[str, Set[WebSocket]] = {}

    async def connect(self, session_id: str, websocket: WebSocket):
        """Accept connection and track it."""
        await websocket.accept()

        if session_id not in self.connections:
            self.connections[session_id] = set()

        self.connections[session_id].add(websocket)
        logger.info(f"WebSocket connected to session {session_id}")

    def disconnect(self, session_id: str, websocket: WebSocket):
        """Remove connection from tracking."""
        if session_id in self.connections:
            self.connections[session_id].discard(websocket)

            # Clean up empty sets
            if not self.connections[session_id]:
                del self.connections[session_id]

        logger.info(f"WebSocket disconnected from session {session_id}")

    async def broadcast(self, session_id: str, message: dict):
        """Send message to all connections for a session."""
        if session_id not in self.connections:
            return

        dead_connections = set()

        for websocket in self.connections[session_id]:
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.warning(f"Failed to send to WebSocket: {e}")
                dead_connections.add(websocket)

        # Clean up dead connections
        for ws in dead_connections:
            self.connections[session_id].discard(ws)

# Global instance
websocket_manager = ConnectionManager()
```

#### `app/websocket/routes.py`
```python
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, HTTPException
from jose import jwt, JWTError
from app.config import settings
from app.websocket.manager import websocket_manager
from lib.supabase_client import SupabaseClient
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

@router.websocket("/ws/sessions/{session_id}")
async def session_websocket(
    websocket: WebSocket,
    session_id: str,
    token: str = Query(..., description="JWT token for authentication")
):
    """
    WebSocket endpoint for real-time session updates.

    Connect: ws://host/ws/sessions/{session_id}?token={jwt}

    Events received:
    - {"type": "node_created", "node_id": "...", "transformation": "..."}
    - {"type": "task_complete", "task_id": "...", "status": "SUCCESS", "result": {...}}
    - {"type": "task_failed", "task_id": "...", "error": "..."}
    """
    # 1. Verify JWT token
    try:
        payload = jwt.decode(
            token,
            settings.SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            audience="authenticated"
        )
        user_id = payload.get("sub")
    except JWTError:
        await websocket.close(code=4001, reason="Invalid token")
        return

    # 2. Verify user owns this session
    session = SupabaseClient.fetch_session(session_id)
    if not session or str(session.get("user_id")) != user_id:
        await websocket.close(code=4003, reason="Access denied")
        return

    # 3. Accept connection
    await websocket_manager.connect(session_id, websocket)

    try:
        # Keep connection alive, handle incoming messages if needed
        while True:
            # Wait for messages (or just keep alive)
            data = await websocket.receive_text()

            # Could handle client messages here (e.g., ping/pong)
            if data == "ping":
                await websocket.send_text("pong")

    except WebSocketDisconnect:
        websocket_manager.disconnect(session_id, websocket)
```

---

### 6. Broadcast Events from Workers

**File:** `workers/tasks.py` (modifications)

```python
import asyncio
from app.websocket.manager import websocket_manager

# Helper to run async broadcast from sync Celery task
def broadcast_sync(session_id: str, message: dict):
    """Broadcast message from sync context."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    loop.run_until_complete(
        websocket_manager.broadcast(session_id, message)
    )

@celery_app.task(bind=True)
def apply_plan_task(self, session_id: str, plan_id: str):
    """Apply transformation plan."""
    # ... existing implementation ...

    # After creating new node:
    broadcast_sync(session_id, {
        "type": "node_created",
        "node_id": str(new_node_id),
        "transformation": transformation_description,
        "row_count": result.row_count,
        "column_count": result.column_count
    })

    # On task complete:
    broadcast_sync(session_id, {
        "type": "task_complete",
        "task_id": self.request.id,
        "status": "SUCCESS",
        "result": {
            "node_id": str(new_node_id),
            "transformations_applied": len(steps),
            "rows_before": rows_before,
            "rows_after": result.row_count
        }
    })

    # On task failure:
    # broadcast_sync(session_id, {
    #     "type": "task_failed",
    #     "task_id": self.request.id,
    #     "error": str(error)
    # })
```

---

### 7. Update Main App

**File:** `app/main.py` (additions)

```python
from app.auth.routes import router as auth_router
from app.websocket.routes import router as websocket_router

# Add auth routes
app.include_router(auth_router, prefix="/api/v1", tags=["Auth"])

# Add WebSocket routes (no prefix for WebSocket)
app.include_router(websocket_router)
```

---

## New Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_JWT_SECRET` | Yes | JWT secret for token verification |

**Where to find:** Supabase Dashboard → Settings → API → JWT Secret

---

## File Structure After Milestone

```
app/
├── auth/                    # NEW
│   ├── __init__.py
│   ├── dependencies.py      # get_current_user
│   ├── models.py            # AuthUser
│   └── routes.py            # /auth/me
├── websocket/               # NEW
│   ├── __init__.py
│   ├── manager.py           # ConnectionManager
│   └── routes.py            # /ws/sessions/{id}
├── routers/
│   ├── sessions.py          # MODIFIED - add auth
│   ├── upload.py            # MODIFIED - add auth
│   ├── chat.py              # MODIFIED - add auth
│   ├── data.py              # MODIFIED - add auth
│   ├── history.py           # MODIFIED - add auth
│   ├── tasks.py             # MODIFIED - add auth
│   └── health.py            # UNCHANGED - public
├── config.py                # MODIFIED - add JWT secret
└── main.py                  # MODIFIED - add routers

workers/
└── tasks.py                 # MODIFIED - add broadcasts

migrations/
└── 001_add_auth.sql         # NEW
```

---

## Testing Plan

### Manual Testing

1. **Auth Flow**
   ```bash
   # Sign up (via Supabase client or Dashboard)
   # Get JWT token
   # Test protected endpoint
   curl -H "Authorization: Bearer {token}" \
     https://api/v1/sessions
   ```

2. **WebSocket**
   ```javascript
   // Browser console
   const ws = new WebSocket('ws://localhost:8000/ws/sessions/{id}?token={jwt}');
   ws.onmessage = (e) => console.log(JSON.parse(e.data));
   ```

3. **RLS Verification**
   - Create sessions as User A
   - Try to access as User B → should fail
   - Verify in Supabase Dashboard

### Automated Tests

```python
# tests/test_auth.py
def test_protected_endpoint_requires_auth():
    response = client.get("/api/v1/sessions")
    assert response.status_code == 401

def test_protected_endpoint_with_valid_token():
    response = client.get(
        "/api/v1/sessions",
        headers={"Authorization": f"Bearer {valid_token}"}
    )
    assert response.status_code == 200

def test_user_cannot_access_other_users_session():
    # Create session as user A
    # Try to access as user B
    # Should return 404 (RLS hides it)
```

---

## Dependencies to Install

```bash
poetry add python-jose[cryptography]
```

---

## Rollout Plan

1. **Deploy database migration** (Supabase SQL Editor)
2. **Add SUPABASE_JWT_SECRET** to Railway env vars
3. **Deploy backend changes**
4. **Test auth flow with Postman**
5. **Test WebSocket with browser**

---

## Success Criteria

- [ ] All endpoints except /health require valid JWT
- [ ] Users can only see their own sessions
- [ ] WebSocket connects with valid token
- [ ] WebSocket receives node_created events
- [ ] WebSocket receives task_complete events
- [ ] RLS policies prevent cross-user access

---

## Next Milestone

After 9.5 is complete, proceed to **Milestone 10: Frontend Foundation & Node Graph**.
