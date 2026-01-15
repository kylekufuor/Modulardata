# =============================================================================
# app/config.py - Application Settings
# =============================================================================
# This module loads configuration from environment variables using pydantic-settings.
# It provides a single Settings class with all configuration values.
#
# Usage:
#   from app.config import settings
#   print(settings.SUPABASE_URL)
#
# Environment variables are loaded from:
# 1. System environment variables
# 2. .env file in project root (if exists)
#
# The Settings class validates all values at startup, catching configuration
# errors early rather than at runtime.
# =============================================================================

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Uses pydantic-settings to:
    - Automatically load from .env file
    - Validate types and constraints
    - Provide sensible defaults for development

    All settings are accessed via the global `settings` instance.
    """

    # -------------------------------------------------------------------------
    # Supabase Configuration
    # -------------------------------------------------------------------------
    # These are required - app won't start without them

    SUPABASE_URL: str = Field(
        ...,  # ... means required (no default)
        description="Supabase project URL (e.g., https://xxx.supabase.co)"
    )

    SUPABASE_ANON_KEY: str = Field(
        ...,
        description="Supabase anon/public API key"
    )

    SUPABASE_SERVICE_KEY: str = Field(
        ...,
        description="Supabase service_role key (bypasses RLS)"
    )

    # -------------------------------------------------------------------------
    # Redis Configuration (for Celery)
    # -------------------------------------------------------------------------
    # Default to localhost for development

    REDIS_URL: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL for Celery broker"
    )

    # -------------------------------------------------------------------------
    # OpenAI / LLM Configuration
    # -------------------------------------------------------------------------
    # Required for the AI agents

    OPENAI_API_KEY: str = Field(
        ...,
        description="OpenAI API key for AI agents"
    )

    OPENAI_MODEL: str = Field(
        default="gpt-4-turbo",
        description="Default model for AI agents (must support JSON mode)"
    )

    # -------------------------------------------------------------------------
    # Strategist Agent Settings
    # -------------------------------------------------------------------------
    # Configuration for Agent A (Context Strategist)

    STRATEGIST_MAX_MESSAGES: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Max conversation messages to include in agent context"
    )

    STRATEGIST_TEMPERATURE: float = Field(
        default=0.2,
        ge=0.0,
        le=2.0,
        description="OpenAI temperature for Strategist (lower = more consistent)"
    )

    # -------------------------------------------------------------------------
    # Application Settings
    # -------------------------------------------------------------------------

    ENVIRONMENT: Literal["development", "staging", "production"] = Field(
        default="development",
        description="Current environment"
    )

    DEBUG: bool = Field(
        default=False,
        description="Enable debug mode (verbose logging, auto-reload)"
    )

    API_HOST: str = Field(
        default="0.0.0.0",
        description="Host to bind the API server to"
    )

    API_PORT: int = Field(
        default=8000,
        ge=1,
        le=65535,
        description="Port for the API server"
    )

    # -------------------------------------------------------------------------
    # Security
    # -------------------------------------------------------------------------

    SECRET_KEY: str = Field(
        default="dev-secret-key-change-in-production",
        min_length=16,
        description="Secret key for signing tokens"
    )

    # CORS origins (comma-separated string that gets parsed)
    CORS_ORIGINS: str = Field(
        default="http://localhost:3000",
        description="Allowed CORS origins (comma-separated)"
    )

    # -------------------------------------------------------------------------
    # File Upload Settings
    # -------------------------------------------------------------------------

    MAX_UPLOAD_SIZE_MB: int = Field(
        default=50,
        ge=1,
        le=500,
        description="Maximum file upload size in MB"
    )

    ALLOWED_EXTENSIONS: str = Field(
        default=".csv",
        description="Allowed file extensions (comma-separated)"
    )

    # -------------------------------------------------------------------------
    # Pydantic Settings Configuration
    # -------------------------------------------------------------------------

    model_config = SettingsConfigDict(
        # Load from .env file in project root
        env_file=".env",
        # .env values override system env vars
        env_file_encoding="utf-8",
        # Don't fail if .env doesn't exist (useful for production where
        # env vars are set directly)
        env_ignore_empty=True,
        # Case-sensitive environment variable names
        case_sensitive=True,
    )

    # -------------------------------------------------------------------------
    # Computed Properties
    # -------------------------------------------------------------------------

    @property
    def cors_origins_list(self) -> list[str]:
        """
        Parse CORS_ORIGINS string into a list.

        Handles comma-separated values and strips whitespace.
        Example: "http://localhost:3000, https://myapp.com" -> ["http://localhost:3000", "https://myapp.com"]
        """
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]

    @property
    def allowed_extensions_list(self) -> list[str]:
        """
        Parse ALLOWED_EXTENSIONS string into a list.

        Example: ".csv, .tsv" -> [".csv", ".tsv"]
        """
        return [ext.strip().lower() for ext in self.ALLOWED_EXTENSIONS.split(",")]

    @property
    def max_upload_size_bytes(self) -> int:
        """
        Convert MB to bytes for file size validation.
        """
        return self.MAX_UPLOAD_SIZE_MB * 1024 * 1024

    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.ENVIRONMENT == "development"

    @property
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.ENVIRONMENT == "production"


@lru_cache
def get_settings() -> Settings:
    """
    Get cached Settings instance.

    Using lru_cache ensures we only parse .env and validate once,
    not on every access. This is the recommended pattern for
    pydantic-settings.

    Returns:
        Settings: The application settings instance
    """
    return Settings()


# Global settings instance for easy importing
# Usage: from app.config import settings
settings = get_settings()
