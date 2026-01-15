# Technical Debt Tracker

> **Last Updated:** 2026-01-15

This document tracks known technical debt, workarounds, and areas for improvement.

---

## High Priority

### 1. Authentication Not Implemented

**Status:** Not started
**Impact:** API is publicly accessible
**Description:** All endpoints are currently unauthenticated. Anyone can access any session.

**Workaround:** None - API is public

**Solution:**
- Implement API key authentication
- Add user_id to sessions table
- Implement RLS in Supabase

---

### 2. No Rate Limiting

**Status:** Not started
**Impact:** Vulnerable to abuse
**Description:** No limits on API calls or AI usage.

**Workaround:** None

**Solution:**
- Implement rate limiting middleware
- Track usage per API key
- Add quotas for AI calls

---

## Medium Priority

### 3. Large File Handling

**Status:** Known limitation
**Impact:** Files >50MB may fail
**Description:** Entire CSV is loaded into memory. Large files can cause OOM.

**Workaround:** Limit file size to 50MB

**Solution:**
- Implement chunked processing
- Use Dask for out-of-memory DataFrames
- Stream results instead of loading all at once

---

### 4. No Input Validation on Transformations

**Status:** Partial implementation
**Impact:** Bad parameters can cause cryptic errors
**Description:** Transformation parameters aren't fully validated before execution.

**Workaround:** AI tends to generate valid parameters

**Solution:**
- Add Pydantic models for each transformation type
- Validate before sending to worker
- Return clear error messages

---

### 5. Strategist-Engineer Parameter Mismatch

**Status:** Partially fixed (2026-01-15)
**Impact:** Some transformations fail silently
**Description:** Strategist AI sometimes uses different parameter names than Engineer expects.

**Workaround:** Added fallback parameter name handling

**Solution:**
- Standardize parameter names in AI prompts
- Add parameter name mapping layer
- More comprehensive testing

---

## Low Priority

### 6. No Automated Testing in CI

**Status:** Tests exist, CI not configured
**Impact:** Regressions can slip through
**Description:** Tests run manually but not in GitHub Actions.

**Workaround:** Run tests manually before deploying

**Solution:**
- Set up GitHub Actions workflow
- Run tests on PR
- Add coverage reporting

---

### 7. Hardcoded AI Model

**Status:** Configurable but not optimized
**Impact:** Could reduce costs
**Description:** Always uses GPT-4 Turbo, even for simple operations.

**Workaround:** None

**Solution:**
- Use GPT-3.5 for simple parsing
- Reserve GPT-4 for complex transformations
- Add model selection logic

---

### 8. No Retry Logic for AI Calls

**Status:** Not implemented
**Impact:** Transient failures fail permanently
**Description:** If OpenAI returns an error, the task fails immediately.

**Workaround:** User can retry manually

**Solution:**
- Add exponential backoff
- Retry on 429 (rate limit) and 5xx errors
- Set max retry count

---

### 9. Celery Result Backend Cleanup

**Status:** Not configured
**Impact:** Redis memory grows over time
**Description:** Task results accumulate in Redis.

**Workaround:** Manual Redis cleanup

**Solution:**
- Configure `result_expires` in Celery config
- Add periodic cleanup task

---

### 10. No Metrics/Observability

**Status:** Basic logging only
**Impact:** Hard to debug production issues
**Description:** No structured metrics, tracing, or alerting.

**Workaround:** Check Railway logs manually

**Solution:**
- Add structured logging (JSON)
- Integrate with monitoring service
- Add custom metrics (transformation count, latency, etc.)

---

## Resolved

### [FIXED] Upload Returns 500 Error

**Fixed:** 2026-01-15
**Description:** NumPy types weren't JSON serializable in upload response.
**Fix:** Added `_sanitize_value()` helper to convert to native Python types.

---

### [FIXED] Worker Not Processing Tasks

**Fixed:** 2026-01-15
**Description:** Railway worker was running web command instead of Celery.
**Fix:** Changed start command to `celery -A workers.celery_app worker --loglevel=info`

---

### [FIXED] CUSTOM Transformation Not Implemented

**Fixed:** 2026-01-15
**Description:** Strategist could generate CUSTOM transformation but Engineer didn't handle it.
**Fix:** Implemented `custom` transformation in `advanced_ops.py`

---

## Adding New Debt

When adding technical debt, include:

1. **Status:** Not started / In progress / Blocked
2. **Impact:** How it affects users/system
3. **Description:** What the issue is
4. **Workaround:** Current mitigation (if any)
5. **Solution:** How to properly fix it

---

## Related Documentation

- [Architecture Overview](ARCHITECTURE.md)
- [Roadmap](../business/ROADMAP.md)
