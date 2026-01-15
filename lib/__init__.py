# =============================================================================
# lib/ - Standalone Utility Modules
# =============================================================================
# This package contains reusable utilities:
# - profiler.py: Data Profiling Engine - turns CSV into AI-readable summary
# - supabase_client.py: Typed Supabase wrapper for database operations
# - memory.py: Conversation context builder for AI agents
# - utils.py: Shared utilities (error handling, UUID normalization)
#
# These modules are self-contained and can be tested in isolation.
# =============================================================================

from lib.supabase_client import SupabaseClient, SupabaseClientError
from lib.memory import (
    ConversationContext,
    ChatMessage,
    TransformationRecord,
    build_conversation_context,
    format_messages_for_openai,
    ContextBuildError,
)
from lib.utils import ApplicationError, normalize_uuid

__all__ = [
    # Supabase
    "SupabaseClient",
    "SupabaseClientError",
    # Memory/Context
    "ConversationContext",
    "ChatMessage",
    "TransformationRecord",
    "build_conversation_context",
    "format_messages_for_openai",
    "ContextBuildError",
    # Utils
    "ApplicationError",
    "normalize_uuid",
]
