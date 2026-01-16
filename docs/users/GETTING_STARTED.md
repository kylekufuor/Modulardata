# Getting Started with ModularData

> **Time to complete:** 5 minutes
> **Prerequisites:** A CSV file to transform

---

## What is ModularData?

ModularData is an AI-powered data transformation API. Instead of writing pandas code, you describe what you want in plain English:

- "Remove rows where email is blank"
- "Trim whitespace from the name column"
- "Convert all dates to YYYY-MM-DD format"

The AI understands your intent and applies the correct transformations.

---

## Quick Start

### Step 1: Create a Session

```bash
curl -X POST https://web-production-2d224.up.railway.app/api/v1/sessions
```

**Response:**
```json
{
    "session_id": "9040d1ad-d698-40be-b4cb-279f91b95b71",
    "status": "active"
}
```

Save your `session_id` - you'll need it for all subsequent requests.

---

### Step 2: Upload Your CSV

```bash
curl -X POST \
  https://web-production-2d224.up.railway.app/api/v1/sessions/{session_id}/upload \
  -F "file=@your_data.csv"
```

**Response includes:**
- Data profile (column types, null counts, issues detected)
- Preview of first 5 rows
- Quality issues found (duplicates, missing values, etc.)

---

### Step 3: Describe Your Transformation

```bash
curl -X POST \
  https://web-production-2d224.up.railway.app/api/v1/sessions/{session_id}/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "remove rows where email is blank"}'
```

The AI will:
1. Parse your instruction
2. Create a transformation plan
3. Show you what it will do

**Response:**
```json
{
    "plan": {
        "steps": [{
            "transformation_type": "drop_rows",
            "explanation": "Remove all rows where email is null"
        }]
    },
    "assistant_response": "Added to plan: Remove all rows where email is null"
}
```

---

### Step 4: Add More Transformations (Optional)

You can batch multiple transformations:

```bash
curl -X POST ... -d '{"message": "trim whitespace from name column"}'
curl -X POST ... -d '{"message": "standardize status to lowercase"}'
```

---

### Step 5: Apply the Plan

```bash
curl -X POST \
  https://web-production-2d224.up.railway.app/api/v1/sessions/{session_id}/plan/apply
```

This returns a `task_id` for tracking.

---

### Step 6: Check Task Status

```bash
curl https://web-production-2d224.up.railway.app/api/v1/tasks/{task_id}
```

**Response:**
```json
{
    "status": "SUCCESS",
    "result": {
        "transformations_applied": 3,
        "rows_before": 100,
        "rows_after": 95
    }
}
```

---

### Step 7: Download Your Transformed Data

```bash
curl https://web-production-2d224.up.railway.app/api/v1/sessions/{session_id}/data \
  -o transformed_data.csv
```

---

## Key Concepts

### Sessions (API) / Modules (UI)
A session is your workspace. It holds your data and remembers all transformations. Sessions persist until you delete them.

> **Terminology:** The API uses "sessions" in endpoints (e.g., `/api/v1/sessions`), but the web interface displays these as "Modules" for user-friendliness. You can rename modules from the dashboard or within a session.

### Nodes (Version Control)
Every transformation creates a new "node" (version). You can:
- View history of all changes
- Rollback to any previous version
- Never lose your original data

### Plan Mode
Transformations are batched into a "plan" before executing. This lets you:
- Review before applying
- Combine multiple changes
- Cancel if something looks wrong

---

## Example Transformations

Here are some things you can say:

| Instruction | What it does |
|-------------|--------------|
| "remove rows where email is blank" | Drops rows with null emails |
| "trim whitespace from all columns" | Removes leading/trailing spaces |
| "convert status to lowercase" | Standardizes case |
| "remove duplicate rows" | Deduplicates data |
| "fill missing ages with the average" | Imputes nulls with mean |
| "extract year from signup_date" | Creates new year column |
| "remove HTML tags from description" | Strips HTML |
| "mask the SSN column" | Replaces with asterisks |

See [Transformation Catalog](TRANSFORMATION_CATALOG.md) for all 53 available transformations.

---

## Need Help?

- **API Reference:** [API_REFERENCE.md](API_REFERENCE.md)
- **All Transformations:** [TRANSFORMATION_CATALOG.md](TRANSFORMATION_CATALOG.md)
- **Error Codes:** [ERROR_CODES.md](ERROR_CODES.md)

---

## What's Next?

1. **Explore transformations** - See what else you can do
2. **Build an integration** - Use the API in your app
3. **Report issues** - Help us improve

---

*Happy transforming!*
