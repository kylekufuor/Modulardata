# Changelog

All notable changes to ModularData will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [1.0.0] - 2026-01-15

### Added

#### Core Features
- **Session Management**: Create, list, get, and archive data transformation sessions
- **CSV Upload**: Upload CSV files with automatic profiling and issue detection
- **AI Chat Interface**: Natural language transformation instructions
- **Plan Mode**: Batch transformations before applying
- **Async Task Processing**: Background processing via Celery workers
- **Version Control**: Full history with rollback to any previous version

#### Data Profiling
- Automatic column type detection (id, email, phone, date, numeric, name)
- Quality issue detection:
  - Null/missing values
  - Duplicate rows
  - Whitespace issues
  - Format inconsistencies
  - Case inconsistencies

#### Transformations (53 total)
- **Row Operations**: drop_rows, filter_rows, deduplicate, sort_rows
- **Column Operations**: drop_columns, rename_column, reorder_columns, add_column
- **Value Transformations**: fill_nulls, replace_values, standardize, trim_whitespace, change_case, sanitize_headers
- **Type Operations**: convert_type, parse_date, format_date
- **Date Operations**: date_diff, date_add, extract_date_part, date_to_epoch
- **Numeric Operations**: round_numbers, handle_outliers, normalize, abs_value, percent_of_total, bin_numeric, floor_ceiling
- **Text Operations**: extract_pattern, split_column, merge_columns, substring, pad_string, clean_text, remove_html
- **Restructuring**: pivot, melt, transpose, select_columns
- **Filtering**: slice_rows, sample_rows
- **Aggregation**: group_by, cumulative, join, rank
- **Data Quality**: validate_format, mask_data, flag_duplicates
- **Advanced**: conditional_replace, coalesce, explode, lag_lead, custom

#### Infrastructure
- Railway deployment (web + worker services)
- Supabase integration (PostgreSQL + Storage)
- Redis task queue
- Health check endpoints

### Fixed
- Upload endpoint JSON serialization for numpy types
- Parameter name compatibility between Strategist AI and Engineer

---

## [Unreleased]

### Planned
- User authentication (API keys)
- Rate limiting
- Webhook notifications
- Python SDK
- Web UI
- Multi-file joins
- Data export to databases
- Scheduled transformations

---

## Version History

| Version | Date | Highlights |
|---------|------|------------|
| 1.0.0 | 2026-01-15 | Initial release with 53 transformations |

---

*For detailed API changes, see [API_REFERENCE.md](API_REFERENCE.md)*
