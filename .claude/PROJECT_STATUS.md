# ModularData Project Status

> **Last Updated:** 2026-01-16
> **Last Commit:** d0122d8 - Add apply feedback and shorten node labels

---

## Current State

### Deployment
- **Backend:** Railway (web-production-2d224.up.railway.app)
- **Frontend:** Railway
- **Database:** Supabase PostgreSQL
- **Storage:** Supabase Storage
- **Task Queue:** Redis + Celery worker on Railway

### Recent Changes (Session: 2026-01-16)

#### Chat Persistence
- **Messages now persist** in database across page reloads
- Both user messages and assistant responses saved to `chat_logs` table
- Welcome message saved when file is uploaded
- Frontend prepends welcome message if missing from history (backwards compat)

#### Welcome Message Redesign
Changed from verbose format to **card-style**:
```
📊 customers.csv loaded!

   15 rows  •  6 columns

⚠️ Found 5 missing values across 4 columns
   → email has the most (2 missing)

How can I help clean this data?
```

#### Plan Mode UX Improvements
1. **Apply suggestion timing:** Only suggests "apply" at 3+ steps (not at 1-2 steps)
2. **Keep Adding button:** Added to plan panel to dismiss and continue adding transformations
3. **Clear link:** Moved to small text link below buttons
4. **Apply feedback:** Chat shows success message after applying plan:
   ```
   ✅ Done! Applied 2 transformations to your data.

   Your data has been updated. What would you like to do next?
   ```

#### Node Labels
Shortened verbose transformation labels:
- **Before:** "Fill null or missing values in the email column with the placeholder 'no email'; Fill missing values in the age column with 99"
- **After:** "Fill nulls in email, age" or "2 transformations on email, age"

#### Guardrails (from earlier session)
- Added topic classification to keep chat focused on data transformation
- Off-topic messages get friendly redirect back to data tasks
- App commands ("clear plan", "show plan") always recognized as on-topic

#### NodeDetailPanel Redesign (Latest)
1. **Left Info Panel** (Parabola-inspired):
   - Node type badge (Original Data / Transformation)
   - Node name with edit capability
   - Output stats (rows, columns, row change %)
   - Input reference showing parent node
   - Code preview snippet (clickable to expand)
   - Actions: Branch from here, Replace File

2. **Node Rename Capability**:
   - Hover to reveal pencil icon on transformation nodes
   - Click to edit inline with Enter to save, Escape to cancel
   - New API endpoint: `PATCH /api/v1/sessions/{id}/nodes/{node_id}`
   - Updates `transformation` field in database

### Commits from Today's Session
```
d0122d8 Add apply feedback and shorten node labels
c469ddf Improve welcome message, plan UX, and apply suggestions
6ee9318 Prepend welcome message if missing from chat history
5a5a93a Persist chat messages to database for session continuity
```

---

## Architecture Overview

### Backend (FastAPI)
```
app/
├── main.py              # FastAPI app with CORS
├── auth.py              # Supabase JWT authentication
├── routers/
│   ├── sessions.py      # CRUD + PATCH for rename
│   ├── upload.py        # CSV upload with profiling + welcome message
│   ├── chat.py          # AI chat with message persistence
│   ├── data.py          # Data access/download
│   ├── history.py       # Version control/rollback + message retrieval
│   └── tasks.py         # Async task status
agents/
├── guardrails.py        # Topic classification for chat focus
├── response_generator.py # Friendly AI responses
├── strategist.py        # Transformation planning
├── engineer.py          # Code execution
└── tester.py            # Validation
workers/
└── tasks.py             # Celery tasks with short node labels
```

### Frontend (React + Vite)
```
frontend/src/
├── pages/
│   ├── DashboardPage.tsx    # Module list with 3-dot menu
│   ├── NewModulePage.tsx    # Module type selection
│   ├── SessionPage.tsx      # Chat + node graph + plan panel
│   └── AuthPage.tsx         # Login/signup
├── components/
│   ├── NodeDetailPanel.tsx  # Modal with data preview + profile
│   ├── ChatMessage.tsx      # Chat bubble with avatar + formatting
│   ├── ThinkingIndicator.tsx # Animated dots while AI processes
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
| `/api/v1/sessions/{id}/upload` | POST | Upload CSV (saves welcome msg) |
| `/api/v1/sessions/{id}/chat` | POST | Send message (persists to DB) |
| `/api/v1/sessions/{id}/history` | GET | Get nodes + chat messages |
| `/api/v1/sessions/{id}/nodes/{node_id}` | PATCH | Rename node (transformation label) |
| `/api/v1/sessions/{id}/plan/apply` | POST | Execute transformations |

---

## Key Files Modified Today

| File | Changes |
|------|---------|
| `app/routers/chat.py` | Added `_save_chat_messages()`, persist all messages |
| `app/routers/upload.py` | Card-style welcome message, save to chat_logs |
| `app/routers/history.py` | Added PATCH endpoint for node rename |
| `agents/response_generator.py` | Only suggest apply at 3+ steps |
| `agents/guardrails.py` | Topic classification (earlier) |
| `workers/tasks.py` | `create_short_node_label()` for concise node names |
| `frontend/src/pages/SessionPage.tsx` | Chat persistence, welcome prepend, Keep Adding button, apply feedback, parentNode lookup |
| `frontend/src/components/NodeDetailPanel.tsx` | Left info panel, node rename UI with inline editing |
| `frontend/src/components/ChatMessage.tsx` | Chat bubble formatting |
| `frontend/src/components/ThinkingIndicator.tsx` | Animated thinking dots |
| `frontend/src/lib/api.ts` | Added `renameNode()` API call |

---

## Terminology

| API Term | UI Term | Description |
|----------|---------|-------------|
| Session | Module | A workspace containing data and transformation history |
| Node | Version | A snapshot of data after transformation |
| Plan | Plan | Queue of transformations to apply |

---

## Next Steps (When Resuming)

1. **Transform Mode:** Implement immediate execution (bypass plan queue)
2. **Real-time updates:** WebSocket for live node creation feedback
3. **Undo/Redo:** Test rollback functionality in UI
4. **Error handling:** Better user feedback for transformation failures

---

## File References

- **Documentation Index:** `docs/INDEX.md`
- **API Reference:** `docs/users/API_REFERENCE.md`
- **Architecture:** `docs/internal/ARCHITECTURE.md`

---

## Environment Variables (Frontend)

```env
VITE_SUPABASE_URL=your-supabase-url
VITE_SUPABASE_ANON_KEY=your-anon-key
VITE_API_URL=https://web-production-2d224.up.railway.app
```

For local testing, set `VITE_API_URL=http://localhost:8000`
