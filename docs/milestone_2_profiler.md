# Milestone 2: Data Profiling Engine (Enhanced)

**Status:** ✅ Complete
**Date Completed:** January 2026
**Updated:** January 2026 (Enhanced with semantic detection and quality analysis)

---

## Overview

This milestone built the "AI's eyes" - a Python module that transforms raw CSV data into a comprehensive text summary that LLMs can understand. Since AI models cannot read files directly, the profiler extracts key metadata, detects semantic types, identifies data quality issues, and presents everything in a compact, informative format.

---

## What We Built

### 1. Core Profiler Module (`lib/profiler.py`)

**Key Functions:**

| Function | Purpose |
|----------|---------|
| `read_csv_safe()` | Robust CSV reader with auto-detection of encoding and delimiters. Returns `(DataFrame, encoding, delimiter)` tuple |
| `generate_profile()` | Creates DataProfile from DataFrame with full analysis including semantic types and issues |
| `analyze_column()` | Extracts per-column statistics, semantic type, distribution, and quality issues |
| `detect_encoding()` | Tries multiple encodings (UTF-8, Latin-1, CP1252, etc.) |
| `detect_delimiter()` | Auto-detects separator (comma, semicolon, tab, pipe) |
| `detect_semantic_type()` | Identifies semantic meaning (email, phone, date, URL, currency, etc.) |
| `compute_statistics()` | Calculates mean, median, std, quartiles, skewness, and outliers for numeric columns |
| `compute_distribution()` | Analyzes value distribution and cardinality for categorical columns |
| `detect_column_issues()` | Finds data quality problems (whitespace, casing, empty strings, etc.) |
| `profile_from_file()` | Convenience function combining read + profile |

**Edge Cases Handled:**
- Encoding issues (UTF-8, UTF-8-BOM, Latin-1, Windows-1252)
- Different delimiters (`,`, `;`, `\t`, `|`)
- Large files (configurable row limit to prevent memory issues)
- Empty files (returns empty DataFrame gracefully)
- Malformed rows (warns but continues)
- All-null columns
- Mixed type columns

### 2. Semantic Type Detection

The profiler automatically identifies what kind of data each column contains:

| Semantic Type | Detection Method |
|---------------|------------------|
| `EMAIL` | Column name contains "email" or values match email regex |
| `PHONE` | Column name contains "phone"/"tel" or values match phone patterns |
| `URL` | Values match URL pattern (http://, https://, www.) |
| `DATE` | Values match ISO date (YYYY-MM-DD) or US date (MM/DD/YYYY) patterns |
| `DATETIME` | Values match ISO datetime pattern |
| `CURRENCY` | Values contain currency symbols ($, €, £) with numbers |
| `PERCENTAGE` | Values contain % symbol with numbers |
| `BOOLEAN` | Values are true/false, yes/no, 0/1 patterns |
| `ID` | Column name ends with "_id" and all values are unique |
| `CATEGORY` | Low cardinality string column (<10% unique values) |
| `ZIPCODE` | Column name contains "zip" and values match zipcode patterns |
| `SSN` | Column name suggests SSN and values match XXX-XX-XXXX pattern |

### 3. Data Quality Issue Detection

The profiler detects common real-world data problems:

| Issue Type | Severity | Description |
|------------|----------|-------------|
| `MISSING_VALUES` | Warning/Critical | Null values in column (>20% = warning, >50% = critical) |
| `EMPTY_STRINGS` | Warning | Strings that are empty but not null |
| `WHITESPACE` | Warning | Leading/trailing whitespace in values |
| `INCONSISTENT_CASING` | Info | Mix of UPPER, lower, and Title case in same column |
| `OUTLIERS` | Info | Numeric values beyond 1.5 × IQR |
| `INVALID_FORMAT` | Warning | Values that don't match expected pattern (e.g., invalid emails) |
| `DUPLICATE_ROWS` | Warning | Dataset contains duplicate rows |
| `CONSTANT_COLUMN` | Info | Column has only one unique value |

Each issue includes:
- Severity level (info, warning, critical)
- Affected count and percentage
- Suggestion for fixing
- Example values

### 4. Statistical Analysis

For numeric columns, the profiler calculates:

| Statistic | Description |
|-----------|-------------|
| `mean` | Average value |
| `median` | Middle value (50th percentile) |
| `std` | Standard deviation |
| `q1`, `q3` | 25th and 75th percentiles |
| `skewness` | Distribution asymmetry |
| `outlier_count` | Number of values outside IQR bounds |
| `outlier_bounds` | The (lower, upper) bounds for outlier detection |

### 5. Value Distribution Analysis

For categorical/string columns:

| Field | Description |
|-------|-------------|
| `top_values` | Most common values with counts and percentages |
| `cardinality` | "low" (<10%), "medium" (10-50%), or "high" (>50% unique) |
| `is_categorical` | Whether column should be treated as categorical |

### 6. Enhanced Data Models

New Pydantic models in `core/models/profile.py`:

| Model | Purpose |
|-------|---------|
| `SemanticType` | Enum of all detectable semantic types |
| `IssueSeverity` | Enum: info, warning, critical |
| `IssueType` | Enum of all data quality issue types |
| `DataIssue` | Detailed issue with type, severity, count, suggestion, examples |
| `ColumnStatistics` | Statistical measures for numeric columns |
| `ValueDistribution` | Top values and cardinality info |

### 7. Test Fixtures

Sample CSV files for testing various scenarios:

| File | Purpose |
|------|---------|
| `sample_normal.csv` | Standard CSV with mixed data types |
| `sample_with_nulls.csv` | CSV with missing values in multiple columns |
| `sample_semicolon.csv` | European-style semicolon delimiter |
| `sample_empty.csv` | Header only, no data rows |
| `sample_duplicates.csv` | Contains duplicate rows |

### 8. Test Suite (`tests/test_profiler.py`)

35 unit tests covering:
- CSV reading (normal, limited rows, semicolon, empty, missing file)
- Delimiter detection
- Semantic type detection (email, ID, boolean, date, category)
- Column analysis (integers, strings, nulls, statistics, distribution)
- Profile generation (basic, empty, null detection, duplicates)
- Text summary output
- Data quality issues (whitespace, empty strings, outliers)
- Edge cases (single column, single row, all-null, mixed types)

---

## Files Created/Modified

| File | Lines | Description |
|------|-------|-------------|
| `lib/profiler.py` | ~850 | Enhanced profiling module with semantic detection and quality analysis |
| `core/models/profile.py` | ~350 | Extended with new model classes |
| `tests/test_profiler.py` | ~430 | Comprehensive test suite |
| `tests/fixtures/sample_normal.csv` | 6 | Standard test data |
| `tests/fixtures/sample_with_nulls.csv` | 11 | Null value test data |
| `tests/fixtures/sample_semicolon.csv` | 6 | Delimiter test data |
| `tests/fixtures/sample_empty.csv` | 1 | Empty file test |
| `tests/fixtures/sample_duplicates.csv` | 9 | Duplicate row test |

**Total:** ~1,650 lines of code

---

## Dependencies

```toml
# In pyproject.toml
pandas = "^2.1.0"    # Data manipulation
numpy = "^1.26.0"    # Numerical computing
```

---

## How It Works

### 1. Reading CSV Files

```python
from lib.profiler import read_csv_safe

# Auto-detects encoding and delimiter
df, encoding, delimiter = read_csv_safe("data.csv")

# With row limit (for large files)
df, encoding, delimiter = read_csv_safe("big_data.csv", max_rows=10000)
```

### 2. Generating Profiles

```python
from lib.profiler import generate_profile

profile = generate_profile(df)

# Profile contains:
# - row_count, column_count
# - columns: list of ColumnProfile objects (with semantic_type, statistics, distribution, issues)
# - sample_rows: first N rows as dicts
# - warnings: detected data quality issues
# - issues: list of DataIssue objects
# - duplicate_row_count, complete_row_count
```

### 3. Creating AI-Readable Summary

```python
# Convert to text for the LLM
text_summary = profile.to_text_summary()
print(text_summary)
```

**Example output (enhanced):**
```
============================================================
DATASET PROFILE
============================================================

## OVERVIEW
- Rows: 1,000
- Columns: 5
- Complete Rows: 950 (95.0%)
- Duplicate Rows: 12

## COLUMNS

### email
- Type: object
- Semantic Type: EMAIL
- Missing: 5 nulls (0.5%)
- Unique Values: 995
- Samples: ['john@test.com', 'jane@example.org', 'bob@corp.net']
- Pattern: Standard email format

### age
- Type: int64
- Semantic Type: NUMERIC
- Missing: 50 nulls (5.0%)
- Unique Values: 65
- Statistics: mean=35.2, median=33.0, std=12.4
- Min/Max: 18 → 89
- Outliers: 3 values outside bounds (5.0, 85.0)

### status
- Type: object
- Semantic Type: CATEGORY
- Missing: 0 nulls (0.0%)
- Unique Values: 3 (low cardinality)
- Distribution: active (60.0%), inactive (30.0%), pending (10.0%)

## DATA QUALITY ISSUES

[WARNING] Column 'name' has whitespace issues
  - Affected: 45 values (4.5%)
  - Suggestion: Trim leading/trailing whitespace
  - Examples: ['  John', 'Jane  ', ' Bob ']

[WARNING] Column 'email' has invalid format
  - Affected: 8 values (0.8%)
  - Suggestion: Review and correct email format
  - Examples: ['not-an-email', 'missing@', '@domain.com']

## SAMPLE DATA (First 3 Rows)

Row 1: {'email': 'john@test.com', 'age': 28, 'status': 'active', ...}
Row 2: {'email': 'jane@example.org', 'age': 35, 'status': 'pending', ...}
Row 3: {'email': 'bob@corp.net', 'age': 42, 'status': 'active', ...}
```

---

## Verification / Testing

### Run All Tests
```bash
poetry run pytest tests/ -v
# Result: 58 passed (23 models + 35 profiler)
```

### Run Profiler Tests Only
```bash
poetry run pytest tests/test_profiler.py -v
# Result: 35 passed
```

### Manual Verification
```python
from lib.profiler import profile_from_file

profile = profile_from_file("tests/fixtures/sample_normal.csv")
print(f"Rows: {profile.row_count}")
print(f"Columns: {profile.column_count}")
print(f"Issues found: {len([i for col in profile.columns for i in col.issues])}")
print(profile.to_text_summary())
```

---

## Design Decisions

1. **Encoding detection order** - UTF-8 first (most common), then UTF-8-BOM (Windows), Latin-1, CP1252

2. **Delimiter scoring** - Score based on consistency (same column count across rows) and column count (more = better)

3. **Row limit default (100k)** - Prevents memory issues with huge files while still providing representative sample

4. **Sample values (5)** - Enough to show format without bloating the profile

5. **Null thresholds** - 20% nulls = warning, 50% nulls = critical

6. **Outlier detection** - Uses IQR method (1.5 × IQR from Q1/Q3)

7. **Cardinality thresholds** - <10% unique = low, 10-50% = medium, >50% = high

8. **Semantic type confidence** - Uses both column name hints and value pattern matching

9. **JSON-safe conversion** - All numpy types converted to Python primitives for serialization

10. **Issue severity levels** - Info (cosmetic), Warning (should fix), Critical (data integrity risk)

---

## Integration Points

The profiler integrates with:

1. **core/models/profile.py** - Uses `DataProfile`, `ColumnProfile`, `DataIssue`, and related schemas
2. **Supabase nodes.profile_json** - Profile stored as JSONB in database
3. **AI Agents (Milestone 3)** - `to_text_summary()` output passed to Strategist agent

---

## What the AI Can Now Understand

With the enhanced profiler, the AI agent receives:

1. **Data Shape** - Row/column counts, sample rows
2. **Column Types** - Both technical (int64, object) and semantic (EMAIL, DATE, CATEGORY)
3. **Data Quality** - Specific issues with counts, examples, and fix suggestions
4. **Statistics** - Distribution info, outliers, cardinality
5. **Patterns** - Date formats, email validity, casing consistency

This enables the AI to:
- Suggest appropriate transformations based on semantic type
- Prioritize data cleaning by issue severity
- Understand column relationships (ID columns, categories)
- Provide specific, actionable recommendations

---

## Schema Matching (Phase 2 Foundation)

The profiler now includes schema matching capabilities to support the enterprise workflow vision where:
- Trained modules can be saved and reused
- New files are matched against saved module contracts
- Modules can be chained together in workflows

### New Models Added

| Model | Purpose |
|-------|---------|
| `MatchConfidence` | Enum: HIGH (85%+), MEDIUM (60-84%), LOW (40-59%), NO_MATCH (<40%) |
| `ColumnContract` | Expected column definition (name, type, pattern, constraints) |
| `SchemaContract` | Full module contract with columns, metadata, fingerprint |
| `ColumnMapping` | Maps incoming column → expected column with confidence |
| `SchemaDiscrepancy` | Reports differences between incoming data and contract |
| `SchemaMatch` | Complete match result with score, mappings, discrepancies |
| `ContractMatch` | Result of contract-to-contract matching for workflow validation |

### New Functions Added

| Function | Purpose |
|----------|---------|
| `generate_contract()` | Create SchemaContract from DataProfile (last node) |
| `match_schema()` | Match incoming profile against contract → SchemaMatch |
| `match_contracts()` | Match two contracts for workflow chaining → ContractMatch |
| `normalize_column_name()` | Normalize names for comparison (case, separator insensitive) |
| `match_column_name()` | Match column names with exact/fuzzy/synonym matching |
| `semantic_types_compatible()` | Check if two semantic types are compatible |
| `calculate_value_overlap()` | Calculate sample value overlap between columns |

### Column Name Matching

The matcher uses multiple strategies:

1. **Exact Match** (100%) - Same name
2. **Normalized Match** (95%) - Same after lowercase + remove separators
3. **Alternative Names** (95%) - Matches predefined alternatives
4. **Synonym Match** (85%) - Matches known synonyms (email ↔ e_mail ↔ mail)
5. **Fuzzy Match** (70%+) - Levenshtein distance similarity

### Common Synonyms Supported

```python
"email": ["email", "e_mail", "email_address", "mail"]
"phone": ["phone", "telephone", "tel", "mobile", "cell"]
"name": ["name", "full_name", "customer_name", "user_name"]
"first_name": ["first_name", "firstname", "fname", "given_name"]
"customer_id": ["customer_id", "cust_id", "client_id"]
# ... and many more
```

### Confidence Scoring

The overall match confidence considers:
- **Required column match rate** (60% weight) - How many required columns were matched
- **Mapping quality** (40% weight) - Average confidence of column mappings
- **Penalties** - 15 points per missing required column

### Example Usage

```python
from lib.profiler import generate_profile, generate_contract, match_schema

# 1. Train module with data
training_data = pd.read_csv("training_orders.csv")
profile = generate_profile(training_data)

# 2. Save contract from last node
contract = generate_contract(
    profile,
    module_id="order_cleanup_v1",
    module_name="Order Data Cleanup"
)

# 3. New file arrives
new_data = pd.read_csv("new_orders.csv")
new_profile = generate_profile(new_data)

# 4. Match against contract
match = match_schema(new_profile, contract)

if match.confidence_level == MatchConfidence.HIGH:
    print("Auto-processing: High confidence match")
    # Apply transformations using match.column_mappings
elif match.confidence_level == MatchConfidence.MEDIUM:
    print("Review recommended")
    print(match.to_summary())
else:
    print("File rejected - doesn't match expected format")
    for d in match.discrepancies:
        print(f"  - {d.description}")
```

### Example Match Output

```
==================================================
SCHEMA MATCH RESULT
==================================================

Confidence: 87.5% (high)
Compatible: Yes
Auto-processable: Yes

COLUMN MAPPINGS:
  ✓ 'Customer ID' → 'customer_id' (normalized, 95%)
  ✓ 'Email Address' → 'email' (synonym, 85%)
  ✓ 'Order Date' → 'order_date' (normalized, 95%)
  ✓ 'Total' → 'total_amount' (fuzzy, 72%)

EXTRA COLUMNS (not in contract): ['notes']
```

### Test Coverage

61 new tests in `tests/test_schema_matching.py` covering:
- Column name normalization and Levenshtein distance
- Synonym matching and fuzzy matching
- Semantic type compatibility
- Contract generation from profiles
- Schema matching (profile vs contract)
- Contract-to-contract matching (workflow validation)
- Integration tests simulating full workflow

---

## Summary Statistics

| Metric | Value |
|--------|-------|
| Total Test Count | 119 (58 original + 61 schema matching) |
| lib/profiler.py | ~1,600 lines |
| core/models/profile.py | ~1,250 lines |
| tests/test_profiler.py | ~430 lines |
| tests/test_schema_matching.py | ~420 lines |

---

## Next Milestone

**Milestone 3: Agent A - The Context Strategist**
- Build memory integration to fetch chat history from Supabase
- Write system prompts for intent mapping
- Test referential understanding ("undo that", "do the same for column X")
