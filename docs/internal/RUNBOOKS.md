# Operational Runbooks

> **Status:** Placeholder
> **Last Updated:** 2026-01-15

This document will contain step-by-step procedures for common operational tasks.

---

## Planned Runbooks

### Deployment
- [ ] Deploy new version
- [ ] Rollback deployment
- [ ] Emergency hotfix

### Database
- [ ] Run migrations
- [ ] Backup database
- [ ] Restore from backup

### Troubleshooting
- [ ] Debug stuck tasks
- [ ] Investigate 500 errors
- [ ] Handle rate limit issues

### Maintenance
- [ ] Scale workers
- [ ] Clear Redis cache
- [ ] Archive old sessions

---

## Current Procedures

### Redeploy

```bash
git push origin main
# Railway auto-deploys from main
```

### Check Logs

```bash
railway logs --service web
railway logs --service worker
```

### Check Task Status

```bash
curl https://web-production-2d224.up.railway.app/api/v1/tasks/{task_id}
```

---

*To be expanded as operational procedures are formalized.*
