# =============================================================================
# app/auth/routes.py - Authentication Routes
# =============================================================================
# API endpoints for authentication-related operations.
#
# Note: Actual signup/login is handled by Supabase Auth client-side.
# These routes are for getting user info after authentication.
# =============================================================================

import logging
from fastapi import APIRouter, Depends, HTTPException, status

from app.auth.dependencies import get_current_user
from app.auth.models import AuthUser, UserResponse
from lib.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["Auth"])


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    user: AuthUser = Depends(get_current_user)
) -> UserResponse:
    """
    Get the current authenticated user's profile.

    Returns:
        UserResponse: User profile with id, email, display_name, etc.

    Raises:
        401: If not authenticated
    """
    # Fetch full user profile from public.users table
    client = SupabaseClient.get_client()

    try:
        response = (
            client.table("users")
            .select("*")
            .eq("id", str(user.id))
            .single()
            .execute()
        )

        if response.data:
            return UserResponse(**response.data)

    except Exception as e:
        logger.warning(f"Could not fetch user profile: {e}")

    # User exists in auth but not yet in public.users
    # (might happen if trigger hasn't run yet)
    return UserResponse(
        id=user.id,
        email=user.email,
        display_name=None,
        avatar_url=None,
        created_at=None,
        updated_at=None
    )


@router.get("/verify")
async def verify_token(
    user: AuthUser = Depends(get_current_user)
) -> dict:
    """
    Verify that the current token is valid.

    Useful for checking if a stored token is still valid.

    Returns:
        dict: Confirmation with user_id

    Raises:
        401: If token is invalid or expired
    """
    return {
        "valid": True,
        "user_id": str(user.id),
        "email": user.email
    }
