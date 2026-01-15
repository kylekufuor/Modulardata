-- =============================================================================
-- Migration: Add transformation_code and preview_rows to nodes table
-- =============================================================================
-- Run this in Supabase SQL Editor if you already created the tables
-- and need to add the new columns for code viewing and preview functionality.
--
-- This migration adds:
--   - transformation_code: The actual Python/pandas code executed
--   - preview_rows: First N rows as JSONB for quick preview
-- =============================================================================

-- Add transformation_code column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'nodes' AND column_name = 'transformation_code'
    ) THEN
        ALTER TABLE nodes ADD COLUMN transformation_code TEXT;
        COMMENT ON COLUMN nodes.transformation_code IS 'Python/pandas code executed for this transformation';
        RAISE NOTICE 'Added transformation_code column to nodes table';
    ELSE
        RAISE NOTICE 'transformation_code column already exists';
    END IF;
END $$;

-- Add preview_rows column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'nodes' AND column_name = 'preview_rows'
    ) THEN
        ALTER TABLE nodes ADD COLUMN preview_rows JSONB;
        COMMENT ON COLUMN nodes.preview_rows IS 'First N rows as JSON for quick preview display';
        RAISE NOTICE 'Added preview_rows column to nodes table';
    ELSE
        RAISE NOTICE 'preview_rows column already exists';
    END IF;
END $$;

-- =============================================================================
-- Verification Query
-- =============================================================================
-- Check that the new columns exist:
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'nodes'
AND column_name IN ('transformation_code', 'preview_rows');
