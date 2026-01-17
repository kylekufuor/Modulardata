# =============================================================================
# transforms_v2/primitives - All Transformation Primitives
# =============================================================================
# This package contains all transformation primitives organized by category.
#
# Categories:
#   - rows: Operations that affect rows (filter, sort, dedupe, etc.)
#   - columns: Operations that affect columns (select, rename, split, etc.)
#   - format: Operations that format values (dates, phones, text case, etc.)
#   - text: Text manipulation operations (coming in Phase 2)
#   - calculate: Mathematical operations (coming in Phase 2)
#   - tables: Multi-table operations (coming in Phase 3)
#   - groups: Aggregation operations (coming in Phase 3)
#
# Import all primitives here to register them with the global registry.
# =============================================================================

# Phase 1: Core primitives
from transforms_v2.primitives import rows
from transforms_v2.primitives import columns
from transforms_v2.primitives import format

# Future phases will add:
# from transforms_v2.primitives import text
# from transforms_v2.primitives import calculate
# from transforms_v2.primitives import tables
# from transforms_v2.primitives import groups
