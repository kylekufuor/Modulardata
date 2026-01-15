# =============================================================================
# app/auth/__init__.py - Authentication Module
# =============================================================================
# Provides JWT-based authentication using Supabase Auth.
#
# Usage:
#   from app.auth import get_current_user, AuthUser
#
#   @router.get("/protected")
#   async def protected(user: AuthUser = Depends(get_current_user)):
#       return {"user_id": user.id}
# =============================================================================

from app.auth.dependencies import get_current_user, get_current_user_optional
from app.auth.models import AuthUser, UserResponse

__all__ = [
    "get_current_user",
    "get_current_user_optional",
    "AuthUser",
    "UserResponse",
]
