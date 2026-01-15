# =============================================================================
# app/auth/models.py - Authentication Models
# =============================================================================
# Pydantic models for authentication data.
# =============================================================================

from pydantic import BaseModel
from uuid import UUID
from datetime import datetime
from typing import Optional


class AuthUser(BaseModel):
    """
    Authenticated user extracted from Supabase JWT.

    This is the minimal user info available from the token itself,
    without querying the database.
    """
    id: UUID
    email: Optional[str] = None

    class Config:
        frozen = True  # Make immutable


class UserResponse(BaseModel):
    """
    Full user response for API endpoints.

    Includes additional profile data from the public.users table.
    """
    id: UUID
    email: Optional[str] = None
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class TokenPayload(BaseModel):
    """
    Decoded JWT token payload from Supabase.

    Supabase tokens include standard JWT claims plus custom claims.
    """
    sub: str  # User ID
    email: Optional[str] = None
    aud: str  # Audience (should be "authenticated")
    exp: int  # Expiration timestamp
    iat: int  # Issued at timestamp
    role: Optional[str] = None  # User role
