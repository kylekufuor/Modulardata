# =============================================================================
# app/dependencies.py - Shared Dependencies
# =============================================================================
# FastAPI dependency injection for shared resources.
# These are injected into route handlers using Depends().
# =============================================================================

from typing import Annotated

from fastapi import Depends

from lib.supabase_client import SupabaseClient


def get_supabase_client() -> SupabaseClient:
    """
    Get Supabase client instance.

    Returns the singleton client wrapper.
    """
    return SupabaseClient


# Type alias for dependency injection
SupabaseDep = Annotated[type[SupabaseClient], Depends(get_supabase_client)]
