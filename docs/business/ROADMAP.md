# Product Roadmap

> **Status:** In Progress
> **Last Updated:** 2026-01-15

---

## Current State: v1.0.0

### Completed Features
- Session management
- CSV upload with profiling
- AI chat interface (53 transformations)
- Plan mode (batch before apply)
- Async task processing
- Version control with rollback
- API deployed on Railway

### Known Limitations
- No authentication
- No rate limiting
- No web UI
- Single file only
- CSV only

---

## Phase 1: Foundation (Current)

**Goal:** Stable, production-ready API

- [x] Core transformation engine
- [x] 53 transformation types
- [x] Plan mode workflow
- [x] Version history
- [x] Railway deployment
- [ ] Error handling improvements
- [ ] Input validation
- [ ] API documentation

---

## Phase 2: Access Control

**Goal:** Multi-tenant with usage tracking

- [ ] API key authentication
- [ ] User management
- [ ] Per-user session isolation
- [ ] Usage tracking
- [ ] Rate limiting
- [ ] Basic billing integration

---

## Phase 3: User Experience

**Goal:** Self-service web application

- [ ] Web UI for transformation
- [ ] Data preview/visualization
- [ ] Interactive plan editing
- [ ] Export options (Excel, JSON, etc.)
- [ ] Transformation history dashboard

---

## Phase 4: Extended Data Support

**Goal:** Handle more data sources

- [ ] Excel file support
- [ ] JSON file support
- [ ] Database connections (PostgreSQL, MySQL)
- [ ] Multi-file joins
- [ ] Larger file handling (streaming)

---

## Phase 5: Automation

**Goal:** Production data pipelines

- [ ] Saved transformation templates
- [ ] Scheduled transformations
- [ ] Webhook notifications
- [ ] Python/JavaScript SDKs
- [ ] CLI tool

---

## Phase 6: Intelligence

**Goal:** Proactive data assistance

- [ ] Auto-suggest transformations
- [ ] Quality score and recommendations
- [ ] Anomaly detection
- [ ] Schema validation rules
- [ ] Natural language data queries

---

## Feature Ideas (Backlog)

Not yet scheduled:

- Real-time collaboration
- Custom transformation functions
- Data lineage visualization
- Integration marketplace
- Audit logging for compliance
- On-premise deployment option

---

## Prioritization Criteria

Features are prioritized by:

1. **User value** - Does it solve a real pain point?
2. **Business impact** - Does it enable revenue or growth?
3. **Technical feasibility** - Can we build it with current architecture?
4. **Dependencies** - What must exist first?

---

*Roadmap is subject to change based on user feedback and business priorities.*
