# Milestone 4: Agent B - The Engineer

**Status:** ✅ Complete
**Date Completed:** January 2026

---

## Overview

This milestone built the second agent in the 3-agent pipeline - the Engineer. It executes TechnicalPlans from the Strategist by applying hardcoded pandas transformations to DataFrames. The design prioritizes determinism, safety, and transparency over flexibility.

---

## What We Built

### 1. Engineer Agent (`agents/engineer.py`)

**Key Features:**

| Feature | Description |
|---------|-------------|
| Plan Execution | Transforms DataFrames based on TechnicalPlan |
| Code Transparency | Returns generated pandas code for every operation |
| Batch Execution | Apply multiple plans as a single atomic operation |
| Column Validation | Verifies target columns exist before execution |
| Error Handling | Actionable error messages with suggestions |

**Main Methods:**

```python
engineer = EngineerAgent()

# Execute single plan (with database persistence)
result = engineer.execute_plan(session_id, plan)

# Execute directly on DataFrame (in-memory)
df, code = engineer.execute_on_dataframe(df, plan)

# Execute multiple plans as batch
result = engineer.execute_batch(session_id, [plan1, plan2, plan3])
```

### 2. Transformation Registry (`agents/transformations/registry.py`)

A decorator-based registry pattern for mapping transformation types to handlers:

```python
from agents.transformations.registry import register, get_transformer

@register(TransformationType.TRIM_WHITESPACE)
def trim_whitespace(df: pd.DataFrame, plan: TechnicalPlan) -> tuple[pd.DataFrame, str]:
    # Implementation
    return result_df, "df['col'] = df['col'].str.strip()"

# Usage
transformer = get_transformer(TransformationType.TRIM_WHITESPACE)
result_df, code = transformer(df, plan)
```

### 3. Transformation Modules

**33 operations across 5 modules:**

| Module | Operations |
|--------|------------|
| `cleaning.py` | trim_whitespace, change_case, deduplicate, replace_values, fill_nulls, format_date, sanitize_headers, standardize |
| `filtering.py` | filter_rows, drop_rows, sort_rows, select_columns, drop_columns, slice_rows |
| `restructuring.py` | split_column, merge_columns, pivot, melt, transpose, reorder_columns, rename_column |
| `column_math.py` | add_column, convert_type, round_numbers, normalize, extract_pattern, parse_date |
| `aggregation.py` | group_by, cumulative, join |

### 4. Execution Result Schema (`agents/models/execution_result.py`)

| Field | Type | Description |
|-------|------|-------------|
| `success` | bool | Whether execution succeeded |
| `new_node_id` | str | ID of created version node |
| `row_count` | int | Rows in result DataFrame |
| `column_count` | int | Columns in result DataFrame |
| `rows_affected` | int | Number of rows changed |
| `transformation_code` | str | Generated pandas code |
| `execution_time_ms` | float | Time taken |
| `error_message` | str | Error details if failed |

### 5. Utility Functions (`agents/transformations/utils.py`)

| Function | Purpose |
|----------|---------|
| `build_condition_mask()` | Convert FilterConditions to pandas boolean mask |
| `conditions_to_code()` | Generate readable code for conditions |

**Supported operators:** eq, ne, gt, lt, gte, lte, isnull, notnull, contains, startswith, endswith, regex, in, not_in

---

## Architecture Decision: Hardcoded vs LLM-Generated Code

We chose **hardcoded transformations** over LLM-generated pandas code:

| Factor | Hardcoded | LLM-Generated |
|--------|-----------|---------------|
| **Determinism** | Same input → same output | May vary between calls |
| **Safety** | No code injection risk | Potential security issues |
| **Speed** | Instant execution | API latency per operation |
| **Debugging** | Predictable behavior | Hard to trace errors |
| **Coverage** | ~90% of common operations | Can handle anything |

**Trade-off:** Users cannot request arbitrary transformations. If an operation isn't hardcoded, the system either maps it to something close or asks for clarification.

---

## Integration with Chat

The `scripts/chat_interactive.py` now uses the real Engineer:

```
You: remove rows where email is missing
Assistant: Queued: DROP_ROWS on email. [1 change(s) pending]

You: apply
Assistant: ✓ Applied 1 transformation(s):
  1. drop_rows on email
Data now has 950 rows × 5 columns.
Generated pandas code:
  df = df[~(df['email'].isna())]
```

---

## Test Coverage

| Test File | Tests | Coverage |
|-----------|-------|----------|
| `tests/test_transformations.py` | 41 | All transformation functions |
| `scripts/test_each_category.py` | 9 | End-to-end Strategist → Engineer |

**All tests passing.**

---

## Files Created

```
agents/
├── engineer.py                    # EngineerAgent class
├── models/
│   └── execution_result.py        # Result schemas
└── transformations/
    ├── __init__.py                # Package exports
    ├── registry.py                # Decorator registry
    ├── utils.py                   # Condition mask builder
    ├── cleaning.py                # 8 cleaning operations
    ├── filtering.py               # 6 filtering operations
    ├── restructuring.py           # 7 restructuring operations
    ├── column_math.py             # 6 column math operations
    └── aggregation.py             # 4 aggregation operations

tests/
└── test_transformations.py        # 41 unit tests

scripts/
└── test_each_category.py          # Integration test script
```

---

## Known Limitations

1. **No `custom` type implementation** - LLM-generated code path not built (intentional for safety)
2. **In-memory only for chat** - `execute_on_dataframe()` doesn't persist to database
3. **No undo in chat** - Version tree rollback not wired up yet
4. **Join operation incomplete** - Requires loading second DataFrame (placeholder only)

---

## Next Steps

Potential directions for Milestone 5:
- **Tester Agent** - Validate transformations, check data quality
- **Persistence Layer** - Wire up Supabase storage for version history
- **API Endpoints** - REST API for frontend integration
