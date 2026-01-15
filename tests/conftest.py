# =============================================================================
# tests/conftest.py - Pytest Configuration
# =============================================================================
# This module provides pytest fixtures and configuration for all tests.
#
# Key features:
# - Sets up mock environment variables before any imports
# - Provides common fixtures for testing
# =============================================================================

import os
import sys

# =============================================================================
# Set up test environment BEFORE any imports
# =============================================================================
# This must happen before importing app.config which loads settings immediately

os.environ.setdefault("SUPABASE_URL", "https://test-project.supabase.co")
os.environ.setdefault("SUPABASE_ANON_KEY", "test-anon-key")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DEBUG", "true")

import pytest


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_profile_dict():
    """Sample profile data for testing."""
    return {
        "row_count": 1000,
        "column_count": 5,
        "columns": [
            {
                "name": "email",
                "dtype": "object",
                "semantic_type": "EMAIL",
                "null_count": 50,
                "null_percentage": 5.0,
                "unique_count": 950,
                "sample_values": ["test@example.com", "user@domain.com"],
            },
            {
                "name": "customer_name",
                "dtype": "object",
                "semantic_type": "TEXT",
                "null_count": 0,
                "null_percentage": 0.0,
                "unique_count": 980,
                "sample_values": ["John Doe", "Jane Smith"],
            },
            {
                "name": "order_date",
                "dtype": "object",
                "semantic_type": "DATE",
                "null_count": 10,
                "null_percentage": 1.0,
                "unique_count": 365,
                "sample_values": ["2024-01-15", "2024-01-16"],
            },
            {
                "name": "amount",
                "dtype": "float64",
                "semantic_type": "CURRENCY",
                "null_count": 5,
                "null_percentage": 0.5,
                "unique_count": 500,
                "sample_values": [99.99, 149.50, 299.00],
            },
            {
                "name": "status",
                "dtype": "object",
                "semantic_type": "CATEGORY",
                "null_count": 0,
                "null_percentage": 0.0,
                "unique_count": 4,
                "sample_values": ["completed", "pending", "shipped"],
            },
        ],
        "sample_rows": [
            {"email": "test@example.com", "customer_name": "John Doe", "order_date": "2024-01-15", "amount": 99.99, "status": "completed"},
        ],
        "issues": [],
        "duplicate_row_count": 0,
        "complete_row_count": 935,
    }


@pytest.fixture
def sample_chat_messages():
    """Sample chat messages for testing."""
    return [
        {"role": "user", "content": "Remove rows where email is blank"},
        {"role": "assistant", "content": "Removed 50 rows with null emails"},
        {"role": "user", "content": "Now standardize the customer names"},
    ]


@pytest.fixture
def sample_session_data():
    """Sample session data for testing."""
    return {
        "id": "test-session-123",
        "created_at": "2024-01-15T10:00:00Z",
        "current_node_id": "node-456",
        "original_filename": "sales_data.csv",
        "status": "active",
    }


@pytest.fixture
def sample_node_data():
    """Sample node data for testing."""
    return {
        "id": "node-456",
        "session_id": "test-session-123",
        "parent_id": "node-455",
        "created_at": "2024-01-15T10:30:00Z",
        "storage_path": "sessions/test-session-123/node_456.csv",
        "row_count": 950,
        "column_count": 5,
        "profile_json": None,
        "transformation": "Dropped null emails",
        "transformation_code": "df = df.dropna(subset=['email'])",
        "preview_rows": [{"email": "test@example.com", "name": "John"}],
    }
