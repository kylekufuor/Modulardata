-- =============================================================================
-- Migration: Add module_runs table
-- =============================================================================
-- This table tracks each time a module is run on new data.
-- Provides audit trail, debugging info, and run history.
--
-- Run this in Supabase SQL Editor: Dashboard > SQL Editor > New Query
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Table: module_runs
-- -----------------------------------------------------------------------------
-- Purpose: Log each execution of a module on new data
-- Captures input, schema matching, outcome, and output
CREATE TABLE IF NOT EXISTS module_runs (
    -- Primary key: Unique identifier for this run
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Which module (session) was run
    session_id UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,

    -- Who ran it (for multi-user support)
    user_id UUID NOT NULL,

    -- When the run was initiated
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,

    -- -------------------------------------------------------------------------
    -- Input Information
    -- -------------------------------------------------------------------------
    -- Original filename of the uploaded file
    input_filename TEXT NOT NULL,

    -- Input file metrics
    input_row_count INTEGER NOT NULL,
    input_column_count INTEGER NOT NULL,

    -- Where the input file was stored (for reference/debugging)
    input_storage_path TEXT,

    -- -------------------------------------------------------------------------
    -- Schema Matching Results
    -- -------------------------------------------------------------------------
    -- Confidence score (0-100) from schema matching
    confidence_score FLOAT NOT NULL,

    -- Confidence level: HIGH, MEDIUM, LOW, NO_MATCH
    confidence_level TEXT NOT NULL CHECK (confidence_level IN ('HIGH', 'MEDIUM', 'LOW', 'NO_MATCH')),

    -- How columns were mapped (from match_schema result)
    -- Array of {incoming_name, contract_name, match_type, confidence}
    column_mappings JSONB,

    -- Issues found during matching
    -- Array of {type, severity, column, description, suggestion}
    discrepancies JSONB,

    -- -------------------------------------------------------------------------
    -- Run Outcome
    -- -------------------------------------------------------------------------
    -- Status of the run:
    --   'success'           - Ran successfully
    --   'warning_confirmed' - User confirmed despite warnings
    --   'failed'            - Could not run (schema mismatch or error)
    --   'pending'           - Awaiting user confirmation (for MEDIUM confidence)
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('success', 'warning_confirmed', 'failed', 'pending')),

    -- Detailed error message if failed
    -- Should be user-friendly and actionable
    error_message TEXT,

    -- Full error details for debugging (stack trace, etc.)
    error_details JSONB,

    -- -------------------------------------------------------------------------
    -- Output Information (populated on success)
    -- -------------------------------------------------------------------------
    -- Output file metrics
    output_row_count INTEGER,
    output_column_count INTEGER,

    -- Where the output file was stored
    output_storage_path TEXT,

    -- -------------------------------------------------------------------------
    -- Performance Metrics
    -- -------------------------------------------------------------------------
    -- Total run duration in milliseconds
    duration_ms INTEGER,

    -- Breakdown of time spent in each phase
    -- {schema_match_ms, transform_ms, upload_ms}
    timing_breakdown JSONB
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_module_runs_session_id ON module_runs(session_id);
CREATE INDEX IF NOT EXISTS idx_module_runs_user_id ON module_runs(user_id);
CREATE INDEX IF NOT EXISTS idx_module_runs_created_at ON module_runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_module_runs_status ON module_runs(status);

-- Comments for documentation
COMMENT ON TABLE module_runs IS 'Audit log of module executions on new data';
COMMENT ON COLUMN module_runs.confidence_score IS 'Schema match confidence 0-100';
COMMENT ON COLUMN module_runs.confidence_level IS 'HIGH (>=85), MEDIUM (60-84), LOW (40-59), NO_MATCH (<40)';
COMMENT ON COLUMN module_runs.status IS 'Run outcome: success, warning_confirmed, failed, pending';

-- -----------------------------------------------------------------------------
-- Row Level Security (RLS)
-- -----------------------------------------------------------------------------
-- Enable RLS so users can only see their own runs
ALTER TABLE module_runs ENABLE ROW LEVEL SECURITY;

-- Policy: Users can only see runs they created
CREATE POLICY "Users can view own module runs"
    ON module_runs FOR SELECT
    USING (auth.uid() = user_id);

-- Policy: Users can insert their own runs
CREATE POLICY "Users can create own module runs"
    ON module_runs FOR INSERT
    WITH CHECK (auth.uid() = user_id);

-- Policy: Users can update their own runs (for confirming warnings)
CREATE POLICY "Users can update own module runs"
    ON module_runs FOR UPDATE
    USING (auth.uid() = user_id);
