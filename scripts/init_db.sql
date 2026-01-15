-- =============================================================================
-- ModularData API - Database Schema
-- =============================================================================
-- This script creates the core tables for the data transformation API.
-- Run this in Supabase SQL Editor: Dashboard > SQL Editor > New Query
--
-- Tables:
--   sessions  - Container for each user interaction session
--   nodes     - Version tree for CSV transformations (linked list)
--   chat_logs - Message history linked to specific data versions
--
-- The node structure enables "Time Travel":
--   Node 0 (original) -> Node 1 (transform A) -> Node 2 (transform B)
--                                             -> Node 2b (branch after undo)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Enable UUID extension (if not already enabled)
-- -----------------------------------------------------------------------------
-- Supabase uses UUIDs for primary keys. This extension provides uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -----------------------------------------------------------------------------
-- Table: sessions
-- -----------------------------------------------------------------------------
-- Purpose: Container for a user's data transformation session
-- One session = one uploaded CSV file with all its transformations
CREATE TABLE IF NOT EXISTS sessions (
    -- Primary key: Unique identifier for this session
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- When the session was created
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,

    -- Points to the currently "active" version of the data
    -- This gets updated when user makes changes or rolls back
    -- NULL initially, set after first node is created
    current_node_id UUID,

    -- The original filename uploaded by the user (e.g., "sales_data.csv")
    -- Stored for display purposes - actual file is in Supabase Storage
    original_filename TEXT NOT NULL,

    -- Session status: 'active' (in use) or 'archived' (closed/deleted)
    -- We soft-delete by archiving rather than hard delete
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'archived'))
);

-- Comment explaining the table's purpose (visible in Supabase dashboard)
COMMENT ON TABLE sessions IS 'Container for user data transformation sessions';
COMMENT ON COLUMN sessions.current_node_id IS 'Points to the active version (node) of the CSV data';

-- -----------------------------------------------------------------------------
-- Table: nodes
-- -----------------------------------------------------------------------------
-- Purpose: Version tracking for CSV transformations
-- Each node represents one version of the data at a point in time
-- Linked list structure: parent_id points to the previous version
CREATE TABLE IF NOT EXISTS nodes (
    -- Primary key: Unique identifier for this version
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Which session this node belongs to
    session_id UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,

    -- The previous version this was derived from
    -- NULL for "Node 0" (the original upload)
    -- This creates a tree structure enabling branching on undo
    parent_id UUID REFERENCES nodes(id) ON DELETE SET NULL,

    -- When this version was created
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,

    -- Path to the CSV file in Supabase Storage
    -- Format: "sessions/{session_id}/node_{node_id}.csv"
    storage_path TEXT NOT NULL,

    -- Metadata about the data at this version (for quick reference)
    row_count INTEGER NOT NULL DEFAULT 0,
    column_count INTEGER NOT NULL DEFAULT 0,

    -- Snapshot of the data profile at this version
    -- Stored as JSONB so we don't need to re-profile for history views
    -- Contains: column names, types, sample values, null counts
    profile_json JSONB,

    -- Human-readable description of what transformation created this version
    -- Examples: "Original upload", "Dropped rows where age is null"
    -- NULL for Node 0, populated by agents for subsequent nodes
    transformation TEXT,

    -- The actual Python/pandas code that was executed
    -- Allows users to view, copy, and learn from the generated code
    -- NULL for Node 0 (original upload)
    transformation_code TEXT,

    -- Preview of first N rows as JSONB for quick display
    -- Avoids fetching full file from storage for previews (typically 10-20 rows)
    preview_rows JSONB
);

-- Comments for documentation
COMMENT ON TABLE nodes IS 'Version tree for CSV transformations - enables undo/redo';
COMMENT ON COLUMN nodes.parent_id IS 'Previous version (NULL for original upload)';
COMMENT ON COLUMN nodes.profile_json IS 'Cached data profile snapshot for this version';
COMMENT ON COLUMN nodes.transformation_code IS 'Python/pandas code executed for this transformation';
COMMENT ON COLUMN nodes.preview_rows IS 'First N rows as JSON for quick preview display';

-- Index on session_id for fast lookup of all nodes in a session
CREATE INDEX IF NOT EXISTS idx_nodes_session_id ON nodes(session_id);

-- Index on parent_id for traversing the version tree
CREATE INDEX IF NOT EXISTS idx_nodes_parent_id ON nodes(parent_id);

-- -----------------------------------------------------------------------------
-- Table: chat_logs
-- -----------------------------------------------------------------------------
-- Purpose: Store conversation history between user and AI
-- Each message is linked to a specific node (version) for context
CREATE TABLE IF NOT EXISTS chat_logs (
    -- Primary key: Unique identifier for this message
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Which session this message belongs to
    session_id UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,

    -- Which version of the data this message relates to
    -- For user messages: the version they were looking at when they sent it
    -- For assistant messages: the version that was created/referenced
    node_id UUID REFERENCES nodes(id) ON DELETE SET NULL,

    -- Who sent the message: 'user' or 'assistant'
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),

    -- The actual message text
    content TEXT NOT NULL,

    -- When the message was sent
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,

    -- Optional metadata as JSONB for extensibility
    -- Can store: agent reasoning, generated code, error details, etc.
    metadata JSONB DEFAULT '{}'::JSONB
);

-- Comments for documentation
COMMENT ON TABLE chat_logs IS 'Conversation history linked to data versions';
COMMENT ON COLUMN chat_logs.node_id IS 'The data version this message relates to';
COMMENT ON COLUMN chat_logs.metadata IS 'Optional: agent reasoning, generated code, etc.';

-- Index on session_id for fetching conversation history
CREATE INDEX IF NOT EXISTS idx_chat_logs_session_id ON chat_logs(session_id);

-- Index on node_id for finding messages related to a specific version
CREATE INDEX IF NOT EXISTS idx_chat_logs_node_id ON chat_logs(node_id);

-- Composite index for fetching messages in chronological order
CREATE INDEX IF NOT EXISTS idx_chat_logs_session_created
    ON chat_logs(session_id, created_at);

-- -----------------------------------------------------------------------------
-- Add foreign key from sessions to nodes (for current_node_id)
-- -----------------------------------------------------------------------------
-- We add this after nodes table exists to avoid circular dependency
ALTER TABLE sessions
    ADD CONSTRAINT fk_sessions_current_node
    FOREIGN KEY (current_node_id)
    REFERENCES nodes(id)
    ON DELETE SET NULL;

-- -----------------------------------------------------------------------------
-- Row Level Security (RLS) - Placeholder
-- -----------------------------------------------------------------------------
-- RLS policies restrict which rows users can access based on their identity.
-- For now, we're using service_role key which bypasses RLS.
-- In production, you'd enable RLS and add policies like:
--
-- ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY "Users can only access their own sessions" ON sessions
--     FOR ALL USING (user_id = auth.uid());
--
-- We'll implement this in a later milestone when we add authentication.

-- =============================================================================
-- Verification Query
-- =============================================================================
-- After running this script, you can verify the tables were created:
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
