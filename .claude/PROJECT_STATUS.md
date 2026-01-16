# ModularData Project Status

> **Last Updated:** 2026-01-15
> **Last Commit:** e2b29dc - Add module rename, delete, and replace file capabilities

---

## Current State

### Deployment
- **Backend:** Railway (web-production-2d224.up.railway.app)
- **Frontend:** Pending deployment to Railway
- **Database:** Supabase PostgreSQL
- **Storage:** Supabase Storage
- **Task Queue:** Redis + Celery worker on Railway

### Recent Changes (v1.1.0)

1. **UI Terminology Change:** Sessions are now displayed as "Modules" in the web interface
2. **Module Type Selection Page:** New page at `/new-module` with cards for different module types (only CSV active, others greyed out with "Coming Soon")
3. **Welcome Message:** Data profiler summary appears after file upload with column details and detected issues
4. **Replace File Button:** Added to NodeDetailPanel for original data nodes
5. **Module Rename:**
   - PATCH endpoint at `/api/v1/sessions/{session_id}`
   - Inline rename on dashboard module cards
   - Rename in session page header
6. **Module Delete:**
   - 3-dot menu on module cards with Rename/Delete options
   - Delete confirmation dialog before archiving

### Known Issues

- **Local Testing:** Frontend gets "failed to fetch" errors locally - likely needs backend running locally or CORS/API URL configuration
- **Recommended:** Test on deployed Railway instance instead of locally

---

## Architecture Overview

### Backend (FastAPI)
```
app/
├── main.py              # FastAPI app with CORS
├── auth.py              # Supabase JWT authentication
├── routers/
│   ├── sessions.py      # CRUD + PATCH for rename
│   ├── upload.py        # CSV upload with profiling
│   ├── chat.py          # AI transformation interface
│   ├── data.py          # Data access/download
│   ├── history.py       # Version control/rollback
│   └── tasks.py         # Async task status
```

### Frontend (React + Vite)
```
frontend/src/
├── pages/
│   ├── DashboardPage.tsx    # Module list with 3-dot menu
│   ├── NewModulePage.tsx    # Module type selection
│   ├── SessionPage.tsx      # Chat + node graph + rename
│   └── AuthPage.tsx         # Login/signup
├── components/
│   ├── NodeDetailPanel.tsx  # Data preview + replace file
│   └── DataNode.tsx         # React Flow node
├── lib/
│   ├── api.ts               # API client with auth
│   └── supabase.ts          # Supabase client
```

### Key APIs
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/sessions` | POST | Create new module |
| `/api/v1/sessions` | GET | List modules (paginated) |
| `/api/v1/sessions/{id}` | GET | Get module details |
| `/api/v1/sessions/{id}` | PATCH | Rename module |
| `/api/v1/sessions/{id}` | DELETE | Archive module |
| `/api/v1/sessions/{id}/upload` | POST | Upload CSV |
| `/api/v1/sessions/{id}/chat` | POST | Send transformation instruction |
| `/api/v1/sessions/{id}/plan/apply` | POST | Execute transformations |

---

## Terminology

| API Term | UI Term | Description |
|----------|---------|-------------|
| Session | Module | A workspace containing data and transformation history |
| Node | Version | A snapshot of data after transformation |
| Plan | Plan | Queue of transformations to apply |

---

## Next Steps (When Resuming)

1. **Test on Railway:** User will test deployed frontend and report results
2. **Potential Issues to Watch:**
   - Authentication flow
   - API connectivity
   - Module CRUD operations (create, rename, delete)
   - File upload and data profiling
   - Chat and transformation flow

3. **Future Module Types (Greyed Out):**
   - Excel Transformation
   - JSON Transformation
   - Text/Log Transformation
   - Custom Integration

---

## File References

- **Documentation Index:** `docs/INDEX.md`
- **API Reference:** `docs/users/API_REFERENCE.md`
- **Changelog:** `docs/users/CHANGELOG.md`
- **Architecture:** `docs/internal/ARCHITECTURE.md`

---

## Environment Variables (Frontend)

```env
VITE_SUPABASE_URL=your-supabase-url
VITE_SUPABASE_ANON_KEY=your-anon-key
VITE_API_URL=https://web-production-2d224.up.railway.app
```

For local testing, set `VITE_API_URL=http://localhost:8000`
