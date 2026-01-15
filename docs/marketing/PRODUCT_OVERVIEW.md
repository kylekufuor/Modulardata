# Product Overview

> **Status:** In Progress
> **Last Updated:** 2026-01-15

One-page overview of ModularData for prospects and partners.

---

## What is ModularData?

ModularData is an AI-powered data transformation API. Transform your data by describing what you want in plain English - no coding required.

---

## The Problem

- Data cleaning takes 80% of any data project
- Requires pandas, SQL, or complex tools
- Non-technical users are blocked
- Changes are hard to reproduce

---

## Our Solution

**Natural language data transformation.**

Instead of:
```python
df = df.dropna(subset=['email'])
df['name'] = df['name'].str.strip()
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
```

Just say:
- "Remove rows where email is blank"
- "Trim whitespace from name column"
- "Convert dates to YYYY-MM-DD format"

---

## Key Features

### 53 Transformations
From basic cleaning to advanced analytics - all accessible via natural language.

### Plan Mode
Review transformations before applying. Batch multiple changes.

### Full Version History
Every change creates a snapshot. Rollback to any previous version.

### API-First
Integrate into your existing workflows. Automate data pipelines.

---

## How It Works

```
1. Upload CSV    →    2. Describe changes    →    3. Review plan    →    4. Apply & download
```

---

## Use Cases

| Industry | Use Case |
|----------|----------|
| Marketing | Clean campaign data, standardize formats |
| Finance | Validate transactions, normalize accounts |
| Operations | Deduplicate records, fill missing data |
| Analytics | Prepare data for visualization |

---

## Why ModularData?

| vs. Manual Coding | vs. Traditional Tools |
|-------------------|----------------------|
| 10x faster | No learning curve |
| No pandas knowledge | Plain English |
| Reproducible | Version control built-in |

---

## Getting Started

```bash
# 1. Create session
curl -X POST https://web-production-2d224.up.railway.app/api/v1/sessions

# 2. Upload data
curl -X POST .../upload -F "file=@data.csv"

# 3. Transform
curl -X POST .../chat -d '{"message": "remove duplicates"}'

# 4. Apply
curl -X POST .../plan/apply
```

---

## Learn More

- [API Documentation](../users/API_REFERENCE.md)
- [Transformation Catalog](../users/TRANSFORMATION_CATALOG.md)
- [Getting Started Guide](../users/GETTING_STARTED.md)

---

*Transform data with words, not code.*
