# Testing Strategy

> **Status:** Placeholder
> **Last Updated:** 2026-01-15

This document will define the testing approach and coverage goals.

---

## Planned Content

- Testing pyramid strategy
- Unit test guidelines
- Integration test guidelines
- E2E test guidelines
- Coverage requirements
- CI/CD integration

---

## Current State

### Test Structure

```
tests/
├── conftest.py           # Shared fixtures
├── test_profiler.py      # Profiler unit tests
├── test_models.py        # Pydantic model tests
├── test_strategist.py    # AI agent tests
├── test_transformations.py # Transformation tests
├── test_memory.py        # Memory/context tests
├── test_chat_modes.py    # Chat handler tests
└── test_tester.py        # Tester agent tests
```

### Running Tests

```bash
poetry run pytest
poetry run pytest -v
poetry run pytest tests/test_profiler.py
```

### Current Coverage

Coverage not formally tracked. Estimated ~40% code coverage.

---

## Future Implementation

- [ ] Add coverage reporting
- [ ] Set minimum coverage threshold
- [ ] Add GitHub Actions CI
- [ ] Add integration tests for API endpoints
- [ ] Add E2E tests with real AI

---

*To be expanded as testing infrastructure matures.*
