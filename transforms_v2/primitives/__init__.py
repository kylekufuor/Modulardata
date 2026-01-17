# =============================================================================
# transforms_v2/primitives - All Transformation Primitives
# =============================================================================
# This package contains all transformation primitives organized by category.
#
# Categories:
#   - rows: Operations that affect rows (filter, sort, dedupe, sample, offset, etc.)
#   - columns: Operations that affect columns (select, rename, split, etc.)
#   - format: Operations that format values (dates, phones, text case, etc.)
#   - text: Text manipulation operations (find/replace, extract, pad, etc.)
#   - calculate: Mathematical operations (math, round, percentage, rank, floor, bin, etc.)
#   - tables: Multi-table operations (join, union, lookup)
#   - groups: Aggregation operations (aggregate, pivot, unpivot)
#   - dates: Date/time operations (extract parts, calculate differences)
#   - quality: Data quality operations (detect nulls, profile columns)
#
# Import all primitives here to register them with the global registry.
# =============================================================================

# Phase 1: Core primitives
from transforms_v2.primitives import rows
from transforms_v2.primitives import columns
from transforms_v2.primitives import format

# Phase 2: Text and calculation primitives
from transforms_v2.primitives import text
from transforms_v2.primitives import calculate

# Phase 3: Multi-table and aggregation primitives
from transforms_v2.primitives import tables
from transforms_v2.primitives import groups

# Phase 4: Date/time and data quality primitives
from transforms_v2.primitives import dates
from transforms_v2.primitives import quality
