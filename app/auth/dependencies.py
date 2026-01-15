# =============================================================================
# app/auth/dependencies.py - FastAPI Auth Dependencies
# =============================================================================
# Provides dependency injection for authentication.
#
# Supports both:
# - ES256 (new Supabase JWT signing keys) via JWKS
# - HS256 (legacy Supabase JWT secret) as fallback
#
# Usage:
#   from app.auth import get_current_user, AuthUser
#
#   @router.get("/protected")
#   async def protected(user: AuthUser = Depends(get_current_user)):
#       return {"user_id": user.id}
# =============================================================================

import logging
from typing import Optional
from uuid import UUID
import httpx

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError, ExpiredSignatureError, jwk
from jose.exceptions import JWKError

from app.config import settings
from app.auth.models import AuthUser

logger = logging.getLogger(__name__)

# HTTP Bearer token extractor
security = HTTPBearer()
security_optional = HTTPBearer(auto_error=False)

# Cache for JWKS keys
_jwks_cache: dict = {}
_jwks_cache_time: float = 0
JWKS_CACHE_TTL = 3600  # 1 hour


def _get_jwks_url() -> str:
    """Get the JWKS URL from Supabase URL."""
    # Extract project ref from SUPABASE_URL
    # Format: https://<project-ref>.supabase.co
    supabase_url = settings.SUPABASE_URL.rstrip('/')
    return f"{supabase_url}/auth/v1/.well-known/jwks.json"


def _fetch_jwks() -> dict:
    """Fetch JWKS from Supabase with caching."""
    import time
    global _jwks_cache, _jwks_cache_time

    current_time = time.time()

    # Return cached if valid
    if _jwks_cache and (current_time - _jwks_cache_time) < JWKS_CACHE_TTL:
        return _jwks_cache

    try:
        jwks_url = _get_jwks_url()
        response = httpx.get(jwks_url, timeout=10)
        response.raise_for_status()
        _jwks_cache = response.json()
        _jwks_cache_time = current_time
        logger.debug(f"Fetched JWKS from {jwks_url}")
        return _jwks_cache
    except Exception as e:
        logger.warning(f"Failed to fetch JWKS: {e}")
        # Return cached even if expired, as fallback
        if _jwks_cache:
            return _jwks_cache
        return {"keys": []}


def _get_signing_key(token: str) -> tuple[str, str]:
    """
    Get the appropriate signing key for a token.

    Returns:
        Tuple of (key, algorithm) to use for verification
    """
    # Decode header without verification to get algorithm and key ID
    try:
        unverified_header = jwt.get_unverified_header(token)
    except JWTError:
        # Fall back to HS256 if we can't read the header
        return settings.SUPABASE_JWT_SECRET, "HS256"

    alg = unverified_header.get("alg", "HS256")
    kid = unverified_header.get("kid")

    # If HS256, use the legacy secret
    if alg == "HS256":
        return settings.SUPABASE_JWT_SECRET, "HS256"

    # For ES256 or other algorithms, use JWKS
    if kid:
        jwks = _fetch_jwks()
        for key in jwks.get("keys", []):
            if key.get("kid") == kid:
                try:
                    # Convert JWK to PEM format for jose
                    return key, alg
                except Exception as e:
                    logger.warning(f"Failed to parse JWK: {e}")

    # Fallback to HS256
    logger.warning(f"Could not find key for alg={alg}, kid={kid}, falling back to HS256")
    return settings.SUPABASE_JWT_SECRET, "HS256"


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> AuthUser:
    """
    Extract and validate user from Supabase JWT token.

    This dependency:
    1. Extracts the Bearer token from the Authorization header
    2. Verifies the JWT signature (supports ES256 and HS256)
    3. Validates the token hasn't expired
    4. Returns an AuthUser with the user's ID and email

    Args:
        credentials: Bearer token from Authorization header

    Returns:
        AuthUser: The authenticated user

    Raises:
        HTTPException: 401 if token is invalid or expired

    Usage:
        @router.get("/protected")
        async def protected_route(user: AuthUser = Depends(get_current_user)):
            return {"user_id": user.id}
    """
    token = credentials.credentials

    try:
        # Get the appropriate signing key
        signing_key, algorithm = _get_signing_key(token)

        # Decode and verify the JWT
        payload = jwt.decode(
            token,
            signing_key,
            algorithms=[algorithm],
            audience="authenticated"
        )

        # Extract user info from payload
        user_id = payload.get("sub")
        email = payload.get("email")

        if not user_id:
            logger.warning("JWT token missing 'sub' claim")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: missing user ID",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Convert string UUID to UUID object
        try:
            user_uuid = UUID(user_id)
        except ValueError:
            logger.warning(f"Invalid UUID in token: {user_id}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: malformed user ID",
                headers={"WWW-Authenticate": "Bearer"},
            )

        logger.debug(f"Authenticated user: {user_id}")
        return AuthUser(id=user_uuid, email=email)

    except ExpiredSignatureError:
        logger.warning("JWT token has expired")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )

    except JWTError as e:
        logger.warning(f"JWT validation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security_optional)
) -> Optional[AuthUser]:
    """
    Optionally get the current user from JWT token.

    Returns None if no token is provided, instead of raising an error.
    Useful for endpoints that work with or without authentication.

    Args:
        credentials: Optional Bearer token from Authorization header

    Returns:
        AuthUser if valid token provided, None otherwise

    Usage:
        @router.get("/public-or-private")
        async def flexible_route(user: AuthUser | None = Depends(get_current_user_optional)):
            if user:
                return {"user_id": user.id}
            return {"message": "anonymous access"}
    """
    if credentials is None:
        return None

    try:
        return await get_current_user(credentials)
    except HTTPException:
        # If token is invalid, treat as no auth rather than error
        return None
