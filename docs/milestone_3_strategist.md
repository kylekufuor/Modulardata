# Milestone 3: Agent A - The Context Strategist

**Status:** ✅ Complete
**Date Completed:** January 2026

---

## Overview

This milestone built the first agent in the 3-agent pipeline - the Strategist. It transforms vague natural language requests ("clean up the data") into structured TechnicalPlans that the Engineer agent can execute. The agent understands context, handles conversational flow, and supports a queue-based workflow where users batch transformations into modules.

---

## What We Built

### 1. Strategist Agent (`agents/strategist.py`)

**Key Features:**

| Feature | Description |
|---------|-------------|
| Intent Mapping | Converts vague requests to specific transformations |
| Context Awareness | Uses data profile to resolve column references |
| Referential Understanding | Handles "undo", "that column", "do same for X" |
| Confidence Scoring | 0.0-1.0 score for plan certainty |
| Clarification Requests | Asks follow-up questions when ambiguous |

**Main Method:**
```python
agent = StrategistAgent()
plan = agent.create_plan(
    session_id="uuid",
    user_message="remove rows where email is blank"
)
```

### 2. Technical Plan Schema (`agents/models/technical_plan.py`)

The contract between Strategist and Engineer:

| Field | Type | Description |
|-------|------|-------------|
| `transformation_type` | TransformationType | What operation to perform |
| `target_columns` | List[ColumnTarget] | Which columns to affect |
| `conditions` | List[FilterCondition] | Row filtering criteria |
| `parameters` | dict | Type-specific options |
| `explanation` | str | Human-readable description |
| `confidence` | float | 0.0-1.0 certainty score |
| `clarification_needed` | str | Question if ambiguous |
| `rollback_to_node_id` | str | For undo operations |

**Transformation Types:**
```
DROP_ROWS, FILTER_ROWS, DEDUPLICATE,
DROP_COLUMNS, RENAME_COLUMN, REORDER_COLUMNS,
FILL_NULLS, REPLACE_VALUES, STANDARDIZE, CONVERT_TYPE,
PARSE_DATE, FORMAT_DATE,
TRIM_WHITESPACE, CHANGE_CASE, EXTRACT_PATTERN,
ROUND_NUMBERS, HANDLE_OUTLIERS,
UNDO, CUSTOM
```

### 3. Memory Module (`lib/memory.py`)

**Key Components:**

| Component | Purpose |
|-----------|---------|
| `ConversationContext` | Dataclass holding session state |
| `ChatMessage` | Message with role and content |
| `build_conversation_context()` | Fetches context from Supabase |

**ConversationContext Fields:**
- `session_id`, `current_node_id`, `parent_node_id`
- `current_profile` (DataProfile)
- `messages` (chat history)
- `recent_transformations`
- `original_filename`, `current_row_count`, `current_column_count`

### 4. System Prompts (`agents/prompts/strategist_system.py`)

Uses Anthropic's recommended XML tag structure:

```xml
<role>Context Strategist definition</role>
<data_profile>{dynamic profile}</data_profile>
<recent_transformations>{history}</recent_transformations>
<output_format>TechnicalPlan JSON schema</output_format>
<intent_mapping_rules>Vague phrases → operations</intent_mapping_rules>
<examples>Few-shot examples</examples>
```

### 5. Interactive Chat (`scripts/chat_interactive.py`)

A conversational CLI for data transformation:

**Usage:**
```bash
# With real CSV
poetry run python scripts/chat_interactive.py /path/to/data.csv

# Demo mode
poetry run python scripts/chat_interactive.py
```

**Features:**

| Feature | Description |
|---------|-------------|
| Welcome Flow | Explains ModularData and shows data profile |
| Queue System | Batch multiple transformations |
| Affirmative Handling | "yes" triggers suggested action |
| State Simulation | Preview changes before applying |
| Commands | `/queue`, `/clear`, `/apply`, `/profile`, `/help` |

**Workflow:**
```
1. User describes transformation
2. Strategist creates TechnicalPlan
3. Plan added to queue (not executed)
4. User says "apply"
5. All queued plans → single module (node)
```

### 6. Chat Handler (`agents/chat_handler.py`)

API-ready interface for the chat flow:

```python
from agents.chat_handler import preview_transformation

response = preview_transformation(
    session_id="uuid",
    message="remove duplicate rows"
)

# Returns PlanResponse with:
# - mode: "transform" | "conversation"
# - can_execute: bool
# - assistant_message: str
# - plan: dict (TechnicalPlan)
# - clarification_needed: str | None
```

---

## Files Created

| File | Lines | Description |
|------|-------|-------------|
| `agents/strategist.py` | ~220 | Main Strategist agent |
| `agents/models/technical_plan.py` | ~180 | TechnicalPlan schema |
| `agents/prompts/strategist_system.py` | ~150 | System prompts |
| `agents/chat_handler.py` | ~120 | Chat API interface |
| `lib/memory.py` | ~150 | Context management |
| `lib/supabase_client.py` | ~100 | Database wrapper |
| `scripts/chat_interactive.py` | ~750 | Interactive CLI |
| `scripts/test_strategist_live.py` | ~250 | Live API tests |
| `scripts/test_queue_flow.py` | ~100 | Queue flow tests |
| `tests/test_strategist.py` | ~300 | Unit tests |
| `tests/test_memory.py` | ~150 | Memory tests |
| `tests/test_chat_modes.py` | ~200 | Chat mode tests |

**Total:** ~2,670 lines of code

---

## Dependencies Added

```toml
# In pyproject.toml
openai = "^1.12.0"    # LLM API client
```

---

## How It Works

### 1. User Sends Message

```python
user_message = "remove rows where email is blank"
```

### 2. Strategist Builds Context

```python
context = build_conversation_context(session_id)
# Returns: ConversationContext with profile, history, etc.
```

### 3. LLM Generates Plan

```python
# System prompt includes:
# - Data profile (columns, types, null counts)
# - Recent transformations
# - Intent mapping rules
# - Output schema

response = openai.chat.completions.create(
    model="gpt-4-turbo",
    messages=[system_prompt, user_message],
    response_format={"type": "json_object"},
)
```

### 4. Plan Validated & Returned

```python
plan = TechnicalPlan.model_validate_json(response)

# Result:
# {
#   "transformation_type": "drop_rows",
#   "target_columns": [{"column_name": "email"}],
#   "conditions": [{"column": "email", "operator": "isnull"}],
#   "explanation": "Remove rows where email is null",
#   "confidence": 0.95
# }
```

### 5. Queue-Based Workflow

```python
# User queues multiple transformations
pending_plans.append(plan1)  # Remove missing emails
pending_plans.append(plan2)  # Fill missing ages
pending_plans.append(plan3)  # Standardize names

# User says "apply"
# All plans executed → single module created
```

---

## Conversation Flow Example

```
============================================================
  Welcome to ModularData!
============================================================

I'm your data transformation assistant. I help you clean,
transform, and prepare your data through conversation.

------------------------------------------------------------
  HOW IT WORKS
------------------------------------------------------------
  1. Tell me what you want to fix or change
  2. I'll queue up each transformation step
  3. Say 'apply' to create the transformation batch

============================================================
  Your Data: sales_data.csv
============================================================
  Rows: 15  |  Columns: 8

  Columns:
    • order_id [id]
    • customer_name [name] (1 missing)
    • email [email] (3 missing)

  Issues Detected:
    • 3 missing email (20.0%)
    • 1 missing customer_name (6.7%)

------------------------------------------------------------

  What would you like to tackle first?

You: remove rows with missing emails