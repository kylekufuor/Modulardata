# =============================================================================
# app/auth/dependencies.py - FastAPI Auth Dependencies
# =============================================================================
# Provides dependency injection for authentication.
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

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError, ExpiredSignatureError

from app.config import settings
from app.auth.models import AuthUser

logger = logging.getLogger(__name__)

# HTTP Bearer token extractor
security = HTTPBearer()
security_optional = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> AuthUser:
    """
    Extract and validate user from Supabase JWT token.

    This dependency:
    1. Extracts the Bearer token from the Authorization header
    2. Verifies the JWT signature using Supabase JWT secret
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
        # Decode and verify the JWT
        # Supabase uses HS256 algorithm
        payload = jwt.decode(
            token,
            settings.SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
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
