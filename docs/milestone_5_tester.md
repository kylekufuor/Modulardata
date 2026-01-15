# Milestone 5: Agent C - The Tester

**Status:** ✅ Complete
**Date Completed:** January 2026

---

## Overview

This milestone completed the 3-agent pipeline by building the Tester Agent. The Tester validates transformation results, checks data quality, and catches potential issues before they propagate.

**Complete Pipeline:**
```
User Request → Strategist (Plan) → Engineer (Execute) → Tester (Validate) → User
```

---

## What We Built

### 1. Tester Agent (`agents/tester.py`)

**Key Features:**

| Feature | Description |
|---------|-------------|
| Result Validation | Verifies transformations executed correctly |
| Quality Checks | Runs relevant checks based on transformation type |
| Issue Detection | Flags problems with severity levels |
| Human-Readable Output | `format_for_display()` for terminal output |
| Strict Mode | Option to treat warnings as errors |

**Main Method:**

```python
tester = TesterAgent()
result = tester.validate(
    before_df=original_data,
    after_df=transformed_data,
    plan=technical_plan
)

if result.passed:
    print("✅ All checks passed")
else:
    print(result.format_for_display())
```

### 2. Test Result Schema (`agents/models/test_result.py`)

| Model | Purpose |
|-------|---------|
| `TestResult` | Complete validation result with stats and issues |
| `CheckResult` | Result from a single quality check |
| `QualityIssue` | Individual problem found during validation |
| `Severity` | Enum: success, info, warning, error |

**TestResult Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `passed` | bool | Whether all critical checks passed |
| `severity` | Severity | Overall result severity |
| `rows_before/after` | int | Row counts |
| `checks_run` | list[str] | Names of checks executed |
| `issues` | list[QualityIssue] | All problems found |
| `summary` | str | Human-readable summary |

### 3. Quality Check Registry (`agents/quality_checks/registry.py`)

Same pattern as transformation registry:

```python
@register_check("row_count", applies_to=["drop_rows", "filter_rows"])
def check_row_count(before_df, after_df, plan) -> list[QualityIssue]:
    issues = []
    # Check logic
    return issues

@register_check("schema_valid", universal=True)
def check_schema(before_df, after_df, plan):
    # Runs on ALL transformations
    return []
```

### 4. Quality Check Modules

| Module | Checks |
|--------|--------|
| `schema.py` | schema_valid, target_columns_exist, column_types_preserved |
| `rows.py` | row_count_change, row_count_unchanged, aggregation_row_count, slice_bounds |
| `nulls.py` | fill_nulls_success, new_nulls_introduced, drop_rows_null_check |
| `duplicates.py` | deduplicate_success, new_duplicates_check, join_duplicates |
| `values.py` | numeric_bounds, value_changes, sort_order_valid, aggregation_values |

### 5. Check Matrix

| Transformation | Checks Run |
|----------------|------------|
| `drop_rows` | schema_valid, target_columns_exist, row_count_change, drop_rows_null_check |
| `fill_nulls` | schema_valid, row_count_unchanged, fill_nulls_success, new_duplicates_check |
| `deduplicate` | schema_valid, row_count_change, deduplicate_success |
| `normalize` | schema_valid, row_count_unchanged, numeric_bounds |
| `group_by` | schema_valid, aggregation_row_count, aggregation_values |
| All others | schema_valid, target_columns_exist |

---

## Integration with Chat

The `apply_all_changes()` function now includes Tester validation:

```python
# Execute transformation
result_df, code = engineer.execute_on_dataframe(df, plan)

# Validate with Tester
test_result = tester.validate(before_df, result_df, plan)
all_validations.append(test_result)
```

**Example Output:**

```
You: remove rows where email is missing
You: apply

✓ Applied 1 transformation(s):
  1. drop_rows on email

Data now has 950 rows × 5 columns.

✅ Quality Check: All validations passed

Generated pandas code:
  df = df[~(df['email'].isna())]
```

**With Warnings:**

```
You: remove rows where age > 10
You: apply

✓ Applied 1 transformation(s):
  1. drop_rows on age

Data now has 100 rows × 5 columns.

⚠️  Quality Check: Passed with warnings
  - Removed 900 rows (90.0% of data)

Generated pandas code:
  df = df[~(df['age'] > 10)]
```

---

## Test Coverage

| Test File | Tests | Coverage |
|-----------|-------|----------|
| `tests/test_tester.py` | 27 | All quality checks and TesterAgent methods |

**Test Categories:**
- Registry tests (3)
- TesterAgent tests (3)
- Schema check tests (3)
- Row count tests (3)
- Null check tests (3)
- Duplicate check tests (3)
- Value check tests (2)
- Integration tests (3)
- Edge case tests (4)

---

## Files Created

```
agents/
├── tester.py                      # TesterAgent class
├── models/
│   └── test_result.py             # TestResult, QualityIssue schemas
└── quality_checks/
    ├── __init__.py                # Package exports
    ├── registry.py                # Check registration
    ├── schema.py                  # Schema validation checks
    ├── rows.py                    # Row count checks
    ├── nulls.py                   # Null value checks
    ├── duplicates.py              # Duplicate detection checks
    └── values.py                  # Value range checks

tests/
└── test_tester.py                 # 27 tests

scripts/
└── test_pipeline.py               # 3-agent pipeline test
```

---

## Full Test Suite

```bash
$ poetry run pytest tests/ -q
255 passed
```

---

## Architecture Summary

The 3-agent pipeline is now complete:

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  STRATEGIST │      │  ENGINEER   │      │   TESTER    │
│             │      │             │      │             │
│ NL Request  │─────▶│ TechnicalPlan│─────▶│ Before/After│
│     ↓       │      │     ↓       │      │     ↓       │
│ TechnicalPlan│      │ DataFrame   │      │ TestResult  │
│             │      │ + Code      │      │             │
└─────────────┘      └─────────────┘      └─────────────┘
```

**Key Design Decisions:**
- Decorator-based registry for extensible checks
- Universal checks run on all transformations
- Type-specific checks based on transformation_type
- Severity levels (info, warning, error) for flexible handling
- Human-readable summaries and suggestions

---

## Known Limitations

1. **Performance on large DataFrames** - Some checks skip for >100k rows
2. **Join check incomplete** - JOIN transformation not fully implemented in Engineer
3. **No async validation** - Checks run synchronously

---

## Next Steps

With the 3-agent pipeline complete, potential next milestones:
- **Persistence Layer** - Save transformations and version history to Supabase
- **REST API** - Create endpoints for frontend integration
- **Frontend UI** - Build visual interface for the system
