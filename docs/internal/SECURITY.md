# Security Practices

> **Status:** Placeholder
> **Last Updated:** 2026-01-15

This document will define security guidelines and practices.

---

## Planned Content

- Authentication strategy
- Authorization model
- Data encryption
- Secret management
- Vulnerability scanning
- Security audit schedule

---

## Current State

### Implemented
- HTTPS everywhere (Railway default)
- CORS restrictions in production
- Supabase service key kept secret

### Not Yet Implemented
- User authentication
- API key management
- Rate limiting
- Input sanitization audit
- Security headers

---

## Known Security Gaps

1. **No Authentication** - API is publicly accessible
2. **No Rate Limiting** - Vulnerable to abuse
3. **CUSTOM Transformation** - Executes user-provided code (sandboxed but risky)

---

## Future Implementation

- [ ] Implement API key authentication
- [ ] Add rate limiting
- [ ] Security audit of CUSTOM transformation
- [ ] Add security headers
- [ ] Set up vulnerability scanning
- [ ] Create security policy document

---

*To be expanded when security features are implemented.*
