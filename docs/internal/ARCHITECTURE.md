# ModularData Architecture

> **Last Updated:** 2026-01-16
> **Version:** 2.0.0

This document describes the technical architecture of ModularData, a deterministic data transformation platform with AI-powered intent parsing.

---

## Architecture Overview

ModularData uses a **four-layer architecture** that separates concerns and ensures deterministic, reproducible transformations:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              API LAYER                                       │
│                            (FastAPI)                                         │
│  ┌─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┐   │
│  │sessions │ upload  │  chat   │  plan   │ history │  runs   │   ws    │   │
│  └─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘   │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          INTELLIGENCE LAYER                                  │
│                         (3-Agent Pipeline)                                   │
│                                                                              │
│    ┌─────────────┐      ┌─────────────┐      ┌─────────────┐               │
│    │ STRATEGIST  │ ───▶ │  ENGINEER   │ ───▶ │   TESTER    │               │
│    │             │      │             │      │             │               │
│    │ Interprets  │      │ Translates  │      │ Validates   │               │
│    │ user intent │      │ & executes  │      │ results     │               │
│    └─────────────┘      └──────┬──────┘      └─────────────┘               │
│                                │                                             │
└────────────────────────────────┼─────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          EXECUTION LAYER                                     │
│                         (transforms_v2)                                      │
│                                                                              │
│  ┌────────────────┐    ┌────────────────┐    ┌────────────────────────┐    │
│  │ PlanTranslator │───▶│     Engine     │───▶│   85 Primitives        │    │
│  │                │    │                │    │                        │    │
│  │ TechnicalPlan  │    │ Executes plan  │    │ rows, columns, text,   │    │
│  │ → primitive    │    │ step by step   │    │ calculate, dates,      │    │
│  │   operations   │    │                │    │ tables, groups, quality│    │
│  └────────────────┘    └────────────────┘    └────────────────────────┘    │
│                                                                              │
│  DETERMINISTIC: Same input + params = Same output (always)                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DATA LAYER                                        │
│                           (Supabase)                                         │
│                                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │
│  │ sessions │  │  nodes   │  │  plans   │  │  runs    │  │ CSV Storage  │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Layer 1: API Layer

The HTTP interface layer built with FastAPI.

### Directory Structure

```
app/
├── main.py              # FastAPI app, lifespan, middleware
├── config.py            # Environment configuration
├── exceptions.py        # Custom exception classes
├── dependencies.py      # Dependency injection
└── routers/
    ├── health.py        # /api/v1/health/*
    ├── sessions.py      # /api/v1/sessions/* (CRUD, deploy)
    ├── upload.py        # CSV upload handling
    ├── chat.py          # AI transformation interface
    ├── plan.py          # Plan mode operations
    ├── history.py       # Version control, rollback
    ├── data.py          # Data access, code chain
    ├── runs.py          # Module execution on new data
    ├── tasks.py         # Async task status
    └── feedback.py      # User feedback
```

### Key Endpoints

| Endpoint | Purpose |
|----------|---------|
| `POST /sessions` | Create new session/module |
| `POST /sessions/{id}/upload` | Upload CSV data |
| `POST /sessions/{id}/chat` | Transform via natural language |
| `POST /sessions/{id}/deploy` | Deploy module for reuse |
| `POST /sessions/{id}/run` | Run module on new data |
| `GET /sessions/{id}/nodes/{id}/code-chain` | Get full transformation code |

### Design Decisions

- All routers are async for non-blocking I/O
- Heavy work offloaded to Celery workers
- WebSocket support for real-time updates
- CORS configured for frontend integration

---

## Layer 2: Intelligence Layer

The AI-powered layer that interprets user intent and orchestrates transformations.

### 3-Agent Pipeline

```
User: "Remove rows where email is blank and standardize phone numbers"
                    │
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│ STRATEGIST (Claude)                                               │
│                                                                   │
│ Input:  User message + data profile (columns, types, samples)    │
│ Output: TechnicalPlan with acceptance criteria                   │
│                                                                   │
│ TechnicalPlan:                                                   │
│   - transformation_type: "drop_rows"                             │
│   - target_columns: ["email"]                                    │
│   - conditions: [{column: "email", operator: "isnull"}]          │
│   - acceptance_criteria: ["No null emails in output"]            │
└───────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│ ENGINEER                                                          │
│                                                                   │
│ Input:  TechnicalPlan + DataFrame                                │
│ Output: Transformed DataFrame + generated code                   │
│                                                                   │
│ Process:                                                         │
│   1. PlanTranslator converts TechnicalPlan → primitive calls     │
│   2. Engine executes primitives sequentially                     │
│   3. Generates readable pandas code for transparency             │
└───────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│ TESTER                                                            │
│                                                                   │
│ Input:  Original DataFrame + Transformed DataFrame + Plan        │
│ Output: Validation report + confidence score                     │
│                                                                   │
│ Validates:                                                       │
│   - Acceptance criteria from Strategist                          │
│   - Data quality checks                                          │
│   - Row/column count sanity                                      │
└───────────────────────────────────────────────────────────────────┘
```

### Directory Structure

```
agents/
├── strategist.py        # Intent interpretation (Claude)
├── engineer.py          # Plan execution
├── tester.py            # Result validation
├── chat_handler.py      # Orchestrates the pipeline
├── plan_translator.py   # TechnicalPlan → primitives
├── risk_assessment.py   # Safety evaluation
└── models/
    ├── technical_plan.py
    └── execution_result.py
```

### Risk Assessment

Before executing transformations, the system evaluates risk:

| Condition | Threshold | Action |
|-----------|-----------|--------|
| Row removal | >20% | Requires confirmation |
| Aggressive filter | <50% rows kept | Requires confirmation |
| Column drops | Any | Requires confirmation |
| Deployed module edit | Any | Higher scrutiny |

---

## Layer 3: Execution Layer (transforms_v2)

**The atomic foundation of all transformations.** Every operation resolves to one or more primitive calls.

### Core Philosophy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DETERMINISTIC GUARANTEE                               │
│                                                                              │
│  • No dynamic code generation - all operations from registered primitives   │
│  • Same input + same parameters = same output (always)                      │
│  • Every primitive validates inputs before execution                        │
│  • Original DataFrame is never modified (immutable operations)              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Directory Structure

```
transforms_v2/
├── types.py             # Core types: Primitive, PrimitiveResult, Condition
├── registry.py          # Global primitive registry
├── engine.py            # Plan executor
└── primitives/
    ├── rows.py          # filter_rows, sort_rows, remove_duplicates, etc.
    ├── columns.py       # select_columns, rename_columns, add_column, etc.
    ├── format.py        # change_text_casing, trim_whitespace, etc.
    ├── text.py          # find_replace, regex_extract, pad_text, etc.
    ├── calculate.py     # math_operation, round_numbers, rank, etc.
    ├── tables.py        # join_tables, union_tables, lookup
    ├── groups.py        # aggregate, pivot, unpivot
    ├── dates.py         # format_date, extract_date_parts, date_difference
    └── quality.py       # detect_nulls, validate_schema, detect_drift, etc.
```

### Primitive Categories

| Category | Count | Examples |
|----------|-------|----------|
| **rows** | 14 | filter_rows, sort_rows, remove_duplicates, limit_rows |
| **columns** | 15 | select_columns, rename_columns, add_column, split_column |
| **format** | 5 | change_text_casing, trim_whitespace, format_phone |
| **text** | 9 | find_replace, regex_extract, regex_replace, pad_text |
| **calculate** | 13 | math_operation, round_numbers, rank, running_total |
| **tables** | 3 | join_tables, union_tables, lookup |
| **groups** | 3 | aggregate, pivot, unpivot |
| **dates** | 3 | format_date, extract_date_parts, date_difference |
| **quality** | 15+ | detect_nulls, validate_schema, detect_drift |
| **Total** | **85** | |

### Primitive Structure

Every primitive follows the same pattern:

```python
@register_primitive
class FilterRows(Primitive):
    @classmethod
    def info(cls) -> PrimitiveInfo:
        return PrimitiveInfo(
            name="filter_rows",
            category="rows",
            description="Keep or remove rows based on filter conditions",
            params=[
                ParamDef(name="conditions", type="list[Condition]", required=True),
                ParamDef(name="logic", type="str", default="and"),
                ParamDef(name="keep", type="bool", default=True),
            ],
            test_prompts=[
                TestPrompt(
                    prompt="Show only rows where lead_score is greater than 80",
                    expected_params={"conditions": [...], "keep": True},
                ),
            ],
            may_change_row_count=True,
        )

    def execute(self, df: pd.DataFrame, params: dict) -> PrimitiveResult:
        # Deterministic implementation
        # Returns PrimitiveResult with success, df, metadata
```

### Engine Execution

Plans are arrays of primitive operations executed sequentially:

```python
plan = [
    {"op": "filter_rows", "params": {"conditions": [...], "keep": False}},
    {"op": "remove_duplicates", "params": {"subset": ["email"]}},
    {"op": "change_text_casing", "params": {"column": "name", "case": "title"}},
]

result = Engine().execute(df, plan)
# → ExecutionResult(success=True, df=..., steps=[...], total_duration_ms=42)
```

---

## Layer 4: Data Layer

Persistent storage for sessions, versions, and files.

### Database Tables (Supabase PostgreSQL)

| Table | Purpose |
|-------|---------|
| `sessions` | Module/session metadata, status, current_node_id |
| `nodes` | Version history (tree via parent_id), transformation code |
| `session_plans` | Queued transformations (plan mode) |
| `chat_history` | Conversation logs |
| `module_runs` | Execution history for deployed modules |

### File Storage (Supabase Storage)

```
data-files/
└── sessions/
    └── {session_id}/
        ├── node_{uuid}.csv      # Data at each version
        └── runs/
            └── {run_id}/
                └── output.csv   # Module run outputs
```

### Key Relationships

```
Session (Module)
    │
    ├── current_node_id ──────▶ Node (current version)
    ├── deployed_node_id ─────▶ Node (production version)
    │
    └── Nodes (version tree)
        ├── Node 1 (root - original data)
        │   └── profile_json, storage_path
        │
        └── Node 2 (parent_id → Node 1)
            └── transformation, transformation_code
```

---

## Module System

A **Module** is a reusable transformation pipeline.

### Module Lifecycle

```
┌──────────┐    ┌──────────┐    ┌───────────┐    ┌──────────┐    ┌─────────┐
│  CREATE  │───▶│  UPLOAD  │───▶│ TRANSFORM │───▶│  DEPLOY  │───▶│   RUN   │
│          │    │          │    │           │    │          │    │         │
│ Session  │    │ CSV data │    │ Build     │    │ Lock in  │    │ New     │
│ created  │    │ uploaded │    │ pipeline  │    │ contract │    │ data    │
└──────────┘    └──────────┘    └───────────┘    └──────────┘    └─────────┘
                                                       │
                                                       ▼
                                              deployed_node_id set
                                              Schema contract created
```

### Running a Module

```
1. POST /sessions/{id}/run (multipart file upload)
       │
       ▼
2. Profile incoming data
       │
       ▼
3. Match schema against contract
   ├── HIGH (≥85%)    → Auto-process
   ├── MEDIUM (60-84%) → Requires confirmation
   ├── LOW (40-59%)    → Rejected
   └── NO_MATCH (<40%) → Rejected
       │
       ▼
4. Replay transformation chain (primitives)
       │
       ▼
5. Store output, log run history
```

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Web Framework | FastAPI 0.109 | Async REST API |
| Task Queue | Celery 5.3 | Background jobs |
| Message Broker | Redis | Celery broker + cache |
| Database | Supabase PostgreSQL | Persistent storage |
| File Storage | Supabase Storage | CSV files |
| AI/LLM | Claude (Anthropic) | Intent parsing |
| Data Processing | Pandas 2.1 | DataFrame operations |
| Deployment | Railway | Container hosting |

---

## Request Flow Examples

### Transformation Flow

```
1. Client ─── POST /sessions/{id}/chat {"message": "remove nulls from email"}
2. Router ─── ChatHandler.handle_message()
3. Strategist ─── Interprets intent → TechnicalPlan
4. PlanTranslator ─── TechnicalPlan → {"op": "filter_rows", "params": {...}}
5. Engine ─── Executes primitives
6. Tester ─── Validates result
7. NodeService ─── Creates new node with transformation
8. Response ─── {success: true, node_id: "...", rows_before: 1000, rows_after: 950}
```

### Module Run Flow

```
1. Client ─── POST /sessions/{id}/run (file: new_data.csv)
2. Router ─── ModuleRunService.run_module()
3. Profiler ─── Profile incoming data
4. Matcher ─── Compare schema to contract → confidence_score
5. If compatible:
   └── ModuleRunService.execute_transformations()
       └── Replay all primitives from deployed_node chain
6. Storage ─── Save output CSV
7. Response ─── {run_id: "...", status: "success", output_rows: 950}
```

---

## Scaling Considerations

### Current Limits

| Resource | Limit | Notes |
|----------|-------|-------|
| File size | 50 MB | Configurable |
| Row count | ~100K | Memory-bound |
| Workers | 1 | Single instance |

### Future Scaling

1. **Horizontal worker scaling** - Add Celery workers
2. **Large file handling** - Chunked/streaming processing
3. **Caching** - Redis for repeated operations
4. **Multi-tenancy** - User isolation with RLS

---

## Related Documentation

- [Primitives Reference](../users/PRIMITIVES_REFERENCE.md) - All 85 primitives
- [Module Guide](../users/MODULE_GUIDE.md) - Creating and running modules
- [API Reference](../users/API_REFERENCE.md) - Endpoint documentation
- [Database Schema](DATABASE_SCHEMA.md) - Full table definitions
- [Development Setup](DEV_SETUP.md) - Local environment
- [Deployment Guide](DEPLOYMENT.md) - Production deployment
