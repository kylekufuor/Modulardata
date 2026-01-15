-- =============================================================================
-- Migration: Add session_plans table for Plan Mode
-- =============================================================================
-- This table stores accumulated transformation plans before they are applied.
--
-- Run this in Supabase SQL Editor:
-- 1. Go to Supabase Dashboard → SQL Editor
-- 2. Paste this script
-- 3. Click "Run"
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Create session_plans table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS session_plans (
    -- Primary key
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Foreign key to sessions
    session_id UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,

    -- Accumulated transformation steps (JSONB array)
    steps JSONB NOT NULL DEFAULT '[]',

    -- Plan status: 'planning', 'ready', 'applied', 'cancelled'
    status TEXT NOT NULL DEFAULT 'planning',

    -- When to suggest applying (default: 3 transformations)
    suggest_apply_at INTEGER NOT NULL DEFAULT 3,

    -- Result node ID (set when plan is applied)
    result_node_id UUID REFERENCES nodes(id) ON DELETE SET NULL,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------------

-- Fast lookup by session
CREATE INDEX IF NOT EXISTS idx_session_plans_session_id
ON session_plans(session_id);

-- Find active plans (planning status)
CREATE INDEX IF NOT EXISTS idx_session_plans_status
ON session_plans(status)
WHERE status = 'planning';

-- -----------------------------------------------------------------------------
-- Trigger: Auto-update updated_at timestamp
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_session_plans_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_session_plans_updated_at ON session_plans;
CREATE TRIGGER trigger_session_plans_updated_at
    BEFORE UPDATE ON session_plans
    FOR EACH ROW
    EXECUTE FUNCTION update_session_plans_updated_at();

-- -----------------------------------------------------------------------------
-- Row Level Security (RLS)
-- -----------------------------------------------------------------------------
-- Enable RLS (already enabled for sessions, consistent behavior)
ALTER TABLE session_plans ENABLE ROW LEVEL SECURITY;

-- Allow all operations for authenticated users (adjust based on your auth setup)
CREATE POLICY "Enable all for authenticated users" ON session_plans
    FOR ALL
    USING (true)
    WITH CHECK (true);

-- -----------------------------------------------------------------------------
-- Comments
-- -----------------------------------------------------------------------------
COMMENT ON TABLE session_plans IS 'Stores accumulated transformation plans before application (Plan Mode)';
COMMENT ON COLUMN session_plans.steps IS 'JSONB array of TransformationStep objects';
COMMENT ON COLUMN session_plans.status IS 'planning|ready|applied|cancelled';
COMMENT ON COLUMN session_plans.suggest_apply_at IS 'Number of steps before suggesting to apply';
