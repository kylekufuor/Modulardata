-- =============================================================================
-- Migration: Add deployment status to sessions
-- =============================================================================
-- Updates the status field to support draft/deployed/archived states.
--
-- Status meanings:
--   draft    - Module is being trained/edited, cannot be run
--   deployed - Module is ready to run on new data
--   archived - Module is soft-deleted
--
-- Run this in Supabase SQL Editor: Dashboard > SQL Editor > New Query
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Step 1: Drop the existing check constraint
-- -----------------------------------------------------------------------------
ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_status_check;

-- -----------------------------------------------------------------------------
-- Step 2: Update existing 'active' status to 'draft'
-- -----------------------------------------------------------------------------
UPDATE sessions SET status = 'draft' WHERE status = 'active';

-- -----------------------------------------------------------------------------
-- Step 3: Add new check constraint with all three statuses
-- -----------------------------------------------------------------------------
ALTER TABLE sessions
ADD CONSTRAINT sessions_status_check
CHECK (status IN ('draft', 'deployed', 'archived'));

-- -----------------------------------------------------------------------------
-- Step 4: Add deployed_at timestamp (optional, for tracking)
-- -----------------------------------------------------------------------------
ALTER TABLE sessions
ADD COLUMN IF NOT EXISTS deployed_at TIMESTAMP WITH TIME ZONE;

-- -----------------------------------------------------------------------------
-- Step 5: Add deployed_node_id to track the deployed version
-- -----------------------------------------------------------------------------
-- This allows the module to be runnable even while being edited.
-- deployed_node_id points to the last deployed transformation chain.
-- Runs use this node, not current_node_id.
ALTER TABLE sessions
ADD COLUMN IF NOT EXISTS deployed_node_id UUID REFERENCES nodes(id) ON DELETE SET NULL;

-- Comment for documentation
COMMENT ON COLUMN sessions.status IS 'Module status: draft (training), deployed (ready to run), archived (deleted)';
COMMENT ON COLUMN sessions.deployed_at IS 'When the module was last deployed (NULL if never deployed)';
COMMENT ON COLUMN sessions.deployed_node_id IS 'The node that was deployed - runs use this version';

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
-- After running, verify with:
-- SELECT id, status, deployed_at FROM sessions LIMIT 10;
