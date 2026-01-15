# =============================================================================
# core/models/profile.py - Data Profile Schemas (Enhanced)
# =============================================================================
# These models define the structure of CSV metadata used to give AI agents
# "vision" into the data. The profiler (lib/profiler.py) generates these.
#
# Enhanced to include:
# - Semantic type detection (email, phone, URL, date, currency)
# - Data quality issues with severity levels
# - Statistical insights (mean, median, std, outliers)
# - Pattern detection and value distributions
# =============================================================================

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# =============================================================================
# Enums for Type Classification
# =============================================================================

class SemanticType(str, Enum):
    """
    Detected semantic meaning of a column beyond its data type.

    For example, a column might be dtype="object" (string) but
    semantically it contains email addresses.
    """
    # Identifiers
    ID = "id"                      # Unique identifier column

    # Contact info
    EMAIL = "email"                # Email addresses
    PHONE = "phone"                # Phone numbers
    URL = "url"                    # Web URLs

    # Dates and times
    DATE = "date"                  # Date values
    DATETIME = "datetime"          # Date + time values
    TIME = "time"                  # Time only

    # Numeric meanings
    CURRENCY = "currency"          # Money values
    PERCENTAGE = "percentage"      # Percentage values

    # Geographic
    ZIPCODE = "zipcode"            # Postal codes
    COUNTRY = "country"            # Country names/codes
    STATE = "state"                # State/province

    # Text types
    NAME = "name"                  # Person or entity names
    TEXT = "text"                  # Free-form text
    CATEGORY = "category"          # Categorical values

    # Other
    BOOLEAN = "boolean"            # Yes/No, True/False, 1/0
    NUMERIC = "numeric"            # Generic numbers
    UNKNOWN = "unknown"            # Could not determine


class IssueSeverity(str, Enum):
    """Severity level for data quality issues."""
    INFO = "info"           # Informational, not necessarily a problem
    WARNING = "warning"     # Potential issue, may need attention
    CRITICAL = "critical"   # Serious problem, likely needs fixing


class IssueType(str, Enum):
    """Types of data quality issues that can be detected."""
    # Missing data
    MISSING_VALUES = "missing_values"
    HIGH_NULL_RATE = "high_null_rate"
    EMPTY_STRINGS = "empty_strings"

    # Formatting issues
    WHITESPACE = "whitespace"              # Leading/trailing whitespace
    INCONSISTENT_CASING = "inconsistent_casing"
    INCONSISTENT_FORMAT = "inconsistent_format"
    MIXED_TYPES = "mixed_types"

    # Data quality
    DUPLICATES = "duplicates"
    OUTLIERS = "outliers"
    INVALID_VALUES = "invalid_values"

    # Structure issues
    POSSIBLE_ID_COLUMN = "possible_id_column"
    LOW_CARDINALITY = "low_cardinality"
    HIGH_CARDINALITY = "high_cardinality"


# =============================================================================
# Data Quality Issue Model
# =============================================================================

class DataIssue(BaseModel):
    """
    A specific data quality issue detected in the dataset.

    Provides actionable information for both the AI and the user.

    Example:
        {
            "issue_type": "whitespace",
            "severity": "warning",
            "column": "customer_name",
            "description": "15 values have leading/trailing whitespace",
            "affected_count": 15,
            "suggestion": "Trim whitespace from 'customer_name' column"
        }
    """

    issue_type: IssueType = Field(
        ...,
        description="Type of issue detected"
    )

    severity: IssueSeverity = Field(
        ...,
        description="How serious is this issue"
    )

    column: str | None = Field(
        default=None,
        description="Which column is affected (None for dataset-level issues)"
    )

    description: str = Field(
        ...,
        description="Human-readable description of the issue"
    )

    affected_count: int = Field(
        default=0,
        ge=0,
        description="Number of rows/values affected"
    )

    affected_percent: float = Field(
        default=0.0,
        ge=0.0,
        le=100.0,
        description="Percentage of data affected"
    )

    suggestion: str | None = Field(
        default=None,
        description="Suggested action to fix this issue"
    )

    examples: list[Any] = Field(
        default_factory=list,
        description="Example values showing the issue"
    )


# =============================================================================
# Column Statistics Model
# =============================================================================

class ColumnStatistics(BaseModel):
    """
    Statistical measures for a numeric column.

    Only populated for numeric columns (int, float).

    Example:
        {
            "mean": 45000.50,
            "median": 42000.00,
            "std": 15000.25,
            "q1": 35000.00,
            "q3": 55000.00,
            "skewness": 0.45
        }
    """

    mean: float | None = Field(default=None, description="Average value")
    median: float | None = Field(default=None, description="Middle value")
    std: float | None = Field(default=None, description="Standard deviation")

    # Quartiles for understanding distribution
    q1: float | None = Field(default=None, description="25th percentile")
    q3: float | None = Field(default=None, description="75th percentile")

    # Distribution shape
    skewness: float | None = Field(default=None, description="Distribution skewness")

    # Outlier info using IQR method
    outlier_count: int = Field(default=0, description="Number of outliers detected")
    outlier_bounds: tuple[float, float] | None = Field(
        default=None,
        description="(lower_bound, upper_bound) for outlier detection"
    )


class ValueDistribution(BaseModel):
    """
    Distribution of values for categorical columns.

    Shows the most common values and their frequencies.

    Example:
        {
            "top_values": [
                {"value": "Completed", "count": 450, "percent": 45.0},
                {"value": "Pending", "count": 300, "percent": 30.0}
            ],
            "is_categorical": true,
            "cardinality": "low"
        }
    """

    top_values: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Most common values with counts"
    )

    is_categorical: bool = Field(
        default=False,
        description="Whether this appears to be a categorical column"
    )

    cardinality: str = Field(
        default="unknown",
        description="low (<10 unique), medium (10-100), high (>100)"
    )


# =============================================================================
# Enhanced Column Profile Model
# =============================================================================

class ColumnProfile(BaseModel):
    """
    Comprehensive profile of a single column in the dataset.

    Enhanced to give the AI deep understanding of each column:
    - Basic info (name, type, nulls)
    - Semantic type (email, phone, date, etc.)
    - Statistics for numeric columns
    - Value distribution for categorical columns
    - Data quality issues specific to this column

    Example:
        {
            "name": "customer_email",
            "dtype": "object",
            "semantic_type": "email",
            "null_count": 50,
            "null_percent": 5.0,
            "unique_count": 950,
            "sample_values": ["john@acme.com", "jane@corp.net"],
            "issues": [{"issue_type": "invalid_values", ...}]
        }
    """

    # -------------------------------------------------------------------------
    # Basic Information
    # -------------------------------------------------------------------------

    name: str = Field(
        ...,
        description="Column name from the CSV header"
    )

    dtype: str = Field(
        ...,
        description="Pandas data type (int64, float64, object, datetime64, bool)"
    )

    # -------------------------------------------------------------------------
    # Semantic Understanding
    # -------------------------------------------------------------------------

    semantic_type: SemanticType = Field(
        default=SemanticType.UNKNOWN,
        description="Inferred semantic meaning (email, phone, date, etc.)"
    )

    detected_pattern: str | None = Field(
        default=None,
        description="Detected format pattern (e.g., 'YYYY-MM-DD' for dates)"
    )

    # -------------------------------------------------------------------------
    # Missing Data Analysis
    # -------------------------------------------------------------------------

    null_count: int = Field(
        default=0,
        ge=0,
        description="Count of null/NaN values"
    )

    null_percent: float = Field(
        default=0.0,
        ge=0.0,
        le=100.0,
        description="Percentage of null values (0-100)"
    )

    empty_string_count: int = Field(
        default=0,
        ge=0,
        description="Count of empty strings (different from null)"
    )

    whitespace_only_count: int = Field(
        default=0,
        ge=0,
        description="Count of values that are only whitespace"
    )

    # -------------------------------------------------------------------------
    # Uniqueness
    # -------------------------------------------------------------------------

    unique_count: int = Field(
        default=0,
        ge=0,
        description="Count of unique values"
    )

    is_unique: bool = Field(
        default=False,
        description="Whether all non-null values are unique (potential ID)"
    )

    # -------------------------------------------------------------------------
    # Sample Values
    # -------------------------------------------------------------------------

    sample_values: list[Any] = Field(
        default_factory=list,
        description="Sample values from this column (up to 5)"
    )

    # -------------------------------------------------------------------------
    # Numeric Column Info
    # -------------------------------------------------------------------------

    min_value: Any | None = Field(
        default=None,
        description="Minimum value (for numeric columns)"
    )

    max_value: Any | None = Field(
        default=None,
        description="Maximum value (for numeric columns)"
    )

    statistics: ColumnStatistics | None = Field(
        default=None,
        description="Statistical measures (for numeric columns)"
    )

    # -------------------------------------------------------------------------
    # Categorical Column Info
    # -------------------------------------------------------------------------

    distribution: ValueDistribution | None = Field(
        default=None,
        description="Value distribution (for categorical columns)"
    )

    # -------------------------------------------------------------------------
    # Data Quality Issues
    # -------------------------------------------------------------------------

    issues: list[DataIssue] = Field(
        default_factory=list,
        description="Data quality issues detected in this column"
    )


# =============================================================================
# Enhanced Data Profile Model
# =============================================================================

class DataProfile(BaseModel):
    """
    Complete profile of a CSV dataset with enhanced AI context.

    Designed to give AI agents comprehensive understanding:
    1. Structure - rows, columns, types
    2. Quality - issues, warnings, suggestions
    3. Content - samples, distributions, statistics
    4. Actionable insights - what needs fixing and how

    Example usage:
        profile = generate_profile(df)
        ai_context = profile.to_text_summary()  # Send to AI agent
    """

    # -------------------------------------------------------------------------
    # Basic Structure
    # -------------------------------------------------------------------------

    row_count: int = Field(
        ...,
        ge=0,
        description="Total number of rows in the dataset"
    )

    column_count: int = Field(
        ...,
        ge=0,
        description="Total number of columns"
    )

    columns: list[ColumnProfile] = Field(
        default_factory=list,
        description="Profile information for each column"
    )

    # -------------------------------------------------------------------------
    # Sample Data
    # -------------------------------------------------------------------------

    sample_rows: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Sample rows from the dataset (up to 5)"
    )

    # -------------------------------------------------------------------------
    # File Metadata
    # -------------------------------------------------------------------------

    file_size_bytes: int | None = Field(
        default=None,
        ge=0,
        description="File size in bytes"
    )

    encoding_detected: str | None = Field(
        default=None,
        description="Detected file encoding"
    )

    delimiter_detected: str | None = Field(
        default=None,
        description="Detected column delimiter"
    )

    # -------------------------------------------------------------------------
    # Data Quality
    # -------------------------------------------------------------------------

    issues: list[DataIssue] = Field(
        default_factory=list,
        description="Dataset-level data quality issues"
    )

    # Legacy field for backward compatibility
    warnings: list[str] = Field(
        default_factory=list,
        description="Simple warning strings (deprecated, use issues)"
    )

    # -------------------------------------------------------------------------
    # Header Detection
    # -------------------------------------------------------------------------

    detected_header_row: int = Field(
        default=0,
        ge=0,
        description="Which row contains the column headers (0-indexed)"
    )

    header_confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Confidence score for header detection (0.0-1.0)"
    )

    skipped_rows: int = Field(
        default=0,
        ge=0,
        description="Number of metadata rows before the header"
    )

    user_header_row: int | None = Field(
        default=None,
        description="User-specified header row override"
    )

    # -------------------------------------------------------------------------
    # Footer Detection
    # -------------------------------------------------------------------------

    detected_data_end_row: int | None = Field(
        default=None,
        description="Last row of actual data (None = all rows are data)"
    )

    detected_footer_rows: int = Field(
        default=0,
        ge=0,
        description="Number of footer/summary rows detected"
    )

    footer_type: str | None = Field(
        default=None,
        description="Type of footer: 'summary', 'metadata', 'empty', or None"
    )

    # -------------------------------------------------------------------------
    # Dataset Statistics
    # -------------------------------------------------------------------------

    duplicate_row_count: int = Field(
        default=0,
        ge=0,
        description="Number of duplicate rows"
    )

    complete_row_count: int = Field(
        default=0,
        ge=0,
        description="Number of rows with no missing values"
    )

    memory_usage_bytes: int | None = Field(
        default=None,
        ge=0,
        description="Memory usage of DataFrame in bytes"
    )

    # =========================================================================
    # Text Summary Methods
    # =========================================================================

    def to_text_summary(self, verbose: bool = True) -> str:
        """
        Convert the profile to a rich text summary for AI agents.

        This is what gets passed to the AI. It includes:
        - Dataset overview
        - Column details with semantic types
        - Statistical insights
        - Data quality issues with suggestions

        Args:
            verbose: If True, include all details. If False, compact summary.

        Returns:
            Formatted string optimized for AI understanding
        """
        lines = []

        # -----------------------------------------------------------------
        # Header
        # -----------------------------------------------------------------
        lines.append("=" * 60)
        lines.append("DATASET PROFILE")
        lines.append("=" * 60)
        lines.append("")

        # -----------------------------------------------------------------
        # Overview
        # -----------------------------------------------------------------
        lines.append("## OVERVIEW")
        lines.append(f"- Rows: {self.row_count:,}")
        lines.append(f"- Columns: {self.column_count}")

        if self.file_size_bytes:
            size_kb = self.file_size_bytes / 1024
            if size_kb > 1024:
                lines.append(f"- File Size: {size_kb/1024:.1f} MB")
            else:
                lines.append(f"- File Size: {size_kb:.1f} KB")

        if self.duplicate_row_count > 0:
            dup_pct = (self.duplicate_row_count / self.row_count * 100) if self.row_count > 0 else 0
            lines.append(f"- Duplicate Rows: {self.duplicate_row_count:,} ({dup_pct:.1f}%)")

        if self.complete_row_count > 0:
            complete_pct = (self.complete_row_count / self.row_count * 100) if self.row_count > 0 else 0
            lines.append(f"- Complete Rows (no nulls): {self.complete_row_count:,} ({complete_pct:.1f}%)")

        # Header detection info
        if self.skipped_rows > 0:
            lines.append(f"- Header Row: {self.detected_header_row} (skipped {self.skipped_rows} metadata rows)")
            if self.header_confidence < 0.8:
                lines.append(f"  ⚠️ Low confidence ({self.header_confidence:.0%}) - verify header is correct")

        # Footer detection info
        if self.detected_footer_rows > 0:
            lines.append(f"- Footer Rows: {self.detected_footer_rows} {self.footer_type} row(s) detected at end")
            if self.detected_data_end_row is not None:
                lines.append(f"  → Actual data ends at row {self.detected_data_end_row}")

        lines.append("")

        # -----------------------------------------------------------------
        # Column Details
        # -----------------------------------------------------------------
        lines.append("## COLUMNS")
        lines.append("")

        for col in self.columns:
            lines.append(self._format_column(col, verbose))
            lines.append("")

        # -----------------------------------------------------------------
        # Data Quality Issues
        # -----------------------------------------------------------------
        all_issues = self.issues.copy()
        for col in self.columns:
            all_issues.extend(col.issues)

        if all_issues:
            lines.append("## DATA QUALITY ISSUES")
            lines.append("")

            # Group by severity
            critical = [i for i in all_issues if i.severity == IssueSeverity.CRITICAL]
            warnings = [i for i in all_issues if i.severity == IssueSeverity.WARNING]
            info = [i for i in all_issues if i.severity == IssueSeverity.INFO]

            if critical:
                lines.append("### CRITICAL (Must Fix)")
                for issue in critical:
                    lines.append(self._format_issue(issue))
                lines.append("")

            if warnings:
                lines.append("### WARNINGS (Should Review)")
                for issue in warnings:
                    lines.append(self._format_issue(issue))
                lines.append("")

            if info and verbose:
                lines.append("### INFO")
                for issue in info:
                    lines.append(self._format_issue(issue))
                lines.append("")

        # -----------------------------------------------------------------
        # Sample Data
        # -----------------------------------------------------------------
        if self.sample_rows and verbose:
            lines.append("## SAMPLE DATA (First 3 Rows)")
            lines.append("")
            for i, row in enumerate(self.sample_rows[:3], 1):
                lines.append(f"Row {i}: {row}")
            lines.append("")

        return "\n".join(lines)

    def _format_column(self, col: ColumnProfile, verbose: bool) -> str:
        """Format a single column for the text summary."""
        parts = [f"### {col.name}"]

        # Type info
        type_str = f"Type: {col.dtype}"
        if col.semantic_type != SemanticType.UNKNOWN:
            type_str += f" (detected: {col.semantic_type.value})"
        parts.append(f"- {type_str}")

        # Pattern if detected
        if col.detected_pattern:
            parts.append(f"- Pattern: {col.detected_pattern}")

        # Missing data
        if col.null_count > 0 or col.empty_string_count > 0:
            missing_parts = []
            if col.null_count > 0:
                missing_parts.append(f"{col.null_count:,} nulls ({col.null_percent:.1f}%)")
            if col.empty_string_count > 0:
                missing_parts.append(f"{col.empty_string_count:,} empty strings")
            if col.whitespace_only_count > 0:
                missing_parts.append(f"{col.whitespace_only_count:,} whitespace-only")
            parts.append(f"- Missing: {', '.join(missing_parts)}")
        else:
            parts.append("- Missing: None")

        # Uniqueness
        parts.append(f"- Unique Values: {col.unique_count:,}" +
                    (" (all unique - possible ID)" if col.is_unique else ""))

        # Statistics for numeric columns
        if col.statistics and verbose:
            stats = col.statistics
            if stats.mean is not None:
                parts.append(f"- Stats: mean={stats.mean:,.2f}, median={stats.median:,.2f}, std={stats.std:,.2f}")
            if stats.outlier_count > 0:
                parts.append(f"- Outliers: {stats.outlier_count} values outside normal range")

        # Distribution for categorical columns
        if col.distribution and col.distribution.top_values and verbose:
            dist = col.distribution
            parts.append(f"- Cardinality: {dist.cardinality}")
            top_3 = dist.top_values[:3]
            top_str = ", ".join([f"'{v['value']}' ({v['percent']:.0f}%)" for v in top_3])
            parts.append(f"- Top Values: {top_str}")

        # Numeric range
        if col.min_value is not None and col.max_value is not None:
            parts.append(f"- Range: {col.min_value} to {col.max_value}")

        # Samples
        if col.sample_values:
            samples = ", ".join([repr(v) for v in col.sample_values[:3]])
            parts.append(f"- Samples: [{samples}]")

        return "\n".join(parts)

    def _format_issue(self, issue: DataIssue) -> str:
        """Format a data quality issue for the text summary."""
        prefix = "  "
        if issue.severity == IssueSeverity.CRITICAL:
            prefix = "  🔴 "
        elif issue.severity == IssueSeverity.WARNING:
            prefix = "  🟡 "
        else:
            prefix = "  🔵 "

        col_str = f"[{issue.column}] " if issue.column else ""
        line = f"{prefix}{col_str}{issue.description}"

        if issue.suggestion:
            line += f"\n     → Suggestion: {issue.suggestion}"

        return line

    def to_compact_summary(self) -> str:
        """
        Generate a very compact summary for token-limited contexts.

        Use when context window is limited and you need just the essentials.
        """
        return self.to_text_summary(verbose=False)


# =============================================================================
# Minimal Summary (Backward Compatibility)
# =============================================================================

class ProfileSummary(BaseModel):
    """
    Minimal profile for API responses (less verbose than full DataProfile).
    """
    row_count: int = Field(..., ge=0)
    column_count: int = Field(..., ge=0)
    column_names: list[str] = Field(default_factory=list)


# =============================================================================
# Schema Matching Models (Phase 2 Foundation)
# =============================================================================
# These models support module contracts and schema matching for:
# - Validating incoming data against a saved module's expected schema
# - Chaining modules together in workflows
# - Confidence scoring for automatic vs. manual processing
# =============================================================================


class MatchConfidence(str, Enum):
    """
    Confidence levels for schema matching.

    Determines whether processing can proceed automatically or needs
    human intervention.
    """
    HIGH = "high"         # 85-100%: Auto-process, very likely same structure
    MEDIUM = "medium"     # 60-84%: Review recommended, likely compatible
    LOW = "low"           # 40-59%: Manual review required
    NO_MATCH = "no_match" # <40%: Different file, reject or start fresh


class ColumnContract(BaseModel):
    """
    Expected column definition for a module contract.

    Defines what a column should look like for the module to process it.
    Generated from the last node's profile.

    Example:
        {
            "name": "customer_email",
            "semantic_type": "email",
            "dtype": "object",
            "required": true,
            "nullable": false,
            "expected_pattern": "standard email format",
            "sample_values": ["john@acme.com", "jane@corp.net"],
            "alternative_names": ["email", "e_mail", "email_address"]
        }
    """

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------

    name: str = Field(
        ...,
        description="Expected column name"
    )

    alternative_names: list[str] = Field(
        default_factory=list,
        description="Alternative acceptable names (synonyms, variations)"
    )

    # -------------------------------------------------------------------------
    # Type Expectations
    # -------------------------------------------------------------------------

    semantic_type: SemanticType = Field(
        default=SemanticType.UNKNOWN,
        description="Expected semantic type"
    )

    dtype: str = Field(
        default="object",
        description="Expected pandas dtype"
    )

    expected_pattern: str | None = Field(
        default=None,
        description="Expected format pattern (e.g., 'YYYY-MM-DD')"
    )

    # -------------------------------------------------------------------------
    # Constraints
    # -------------------------------------------------------------------------

    required: bool = Field(
        default=True,
        description="Whether this column must be present"
    )

    nullable: bool = Field(
        default=True,
        description="Whether null values are acceptable"
    )

    max_null_percent: float = Field(
        default=100.0,
        ge=0.0,
        le=100.0,
        description="Maximum acceptable null percentage"
    )

    # -------------------------------------------------------------------------
    # Value Expectations
    # -------------------------------------------------------------------------

    sample_values: list[Any] = Field(
        default_factory=list,
        description="Example values from training data"
    )

    allowed_values: list[Any] | None = Field(
        default=None,
        description="If categorical, the set of expected values"
    )

    # -------------------------------------------------------------------------
    # Numeric Ranges
    # -------------------------------------------------------------------------

    min_value: Any | None = Field(
        default=None,
        description="Expected minimum value (for numeric columns)"
    )

    max_value: Any | None = Field(
        default=None,
        description="Expected maximum value (for numeric columns)"
    )


class SchemaContract(BaseModel):
    """
    A saved schema contract from a module's last node.

    This is the "interface" definition for the module:
    - Incoming data must match this contract to be processed
    - Module output must match the next module's contract for chaining

    Example usage:
        # Generate contract from last node's profile
        contract = generate_contract(last_node_profile)

        # Validate new incoming data
        match = match_schema(new_data_profile, contract)
        if match.confidence_level == MatchConfidence.HIGH:
            # Auto-process
        else:
            # Request human review
    """

    # -------------------------------------------------------------------------
    # Metadata
    # -------------------------------------------------------------------------

    contract_id: str | None = Field(
        default=None,
        description="Unique identifier for this contract"
    )

    module_id: str | None = Field(
        default=None,
        description="ID of the module this contract belongs to"
    )

    module_name: str | None = Field(
        default=None,
        description="Human-readable module name"
    )

    version: str = Field(
        default="1.0",
        description="Contract version for tracking changes"
    )

    created_at: str | None = Field(
        default=None,
        description="ISO timestamp of contract creation"
    )

    # -------------------------------------------------------------------------
    # Schema Definition
    # -------------------------------------------------------------------------

    columns: list[ColumnContract] = Field(
        default_factory=list,
        description="Expected columns with their contracts"
    )

    required_column_count: int = Field(
        default=0,
        ge=0,
        description="Number of required columns"
    )

    # -------------------------------------------------------------------------
    # Dataset Expectations
    # -------------------------------------------------------------------------

    min_row_count: int = Field(
        default=1,
        ge=0,
        description="Minimum expected rows"
    )

    expected_delimiter: str | None = Field(
        default=None,
        description="Expected delimiter (or None for auto-detect)"
    )

    expected_encoding: str | None = Field(
        default=None,
        description="Expected encoding (or None for auto-detect)"
    )

    # -------------------------------------------------------------------------
    # Fingerprint for Quick Comparison
    # -------------------------------------------------------------------------

    fingerprint: str | None = Field(
        default=None,
        description="Hash fingerprint for quick schema comparison"
    )

    # -------------------------------------------------------------------------
    # Methods
    # -------------------------------------------------------------------------

    def get_required_columns(self) -> list[ColumnContract]:
        """Get list of required columns."""
        return [c for c in self.columns if c.required]

    def get_optional_columns(self) -> list[ColumnContract]:
        """Get list of optional columns."""
        return [c for c in self.columns if not c.required]

    def get_column_names(self) -> list[str]:
        """Get all expected column names."""
        return [c.name for c in self.columns]

    def get_all_acceptable_names(self) -> dict[str, list[str]]:
        """
        Get mapping of canonical name -> all acceptable variants.

        Returns:
            {"email": ["email", "e_mail", "email_address", "EMAIL"], ...}
        """
        result = {}
        for col in self.columns:
            all_names = [col.name] + col.alternative_names
            # Add common variations automatically
            all_names.extend([
                col.name.lower(),
                col.name.upper(),
                col.name.replace("_", " "),
                col.name.replace(" ", "_"),
            ])
            result[col.name] = list(set(all_names))
        return result


class ColumnMapping(BaseModel):
    """
    Maps an incoming column to an expected contract column.

    Used in SchemaMatch to show how columns were matched.

    Example:
        {
            "incoming_name": "Email Address",
            "contract_name": "email",
            "match_type": "fuzzy_name",
            "confidence": 0.92,
            "notes": "Matched via fuzzy name similarity (92%)"
        }
    """

    incoming_name: str = Field(
        ...,
        description="Column name from the incoming file"
    )

    contract_name: str = Field(
        ...,
        description="Column name in the contract"
    )

    match_type: str = Field(
        default="exact",
        description="How the match was made: exact, fuzzy_name, semantic_type, value_overlap"
    )

    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Match confidence (0.0 to 1.0)"
    )

    notes: str | None = Field(
        default=None,
        description="Explanation of how/why this match was made"
    )

    # Type compatibility
    type_compatible: bool = Field(
        default=True,
        description="Whether semantic types are compatible"
    )

    dtype_compatible: bool = Field(
        default=True,
        description="Whether data types are compatible"
    )


class SchemaDiscrepancy(BaseModel):
    """
    A difference found between incoming data and contract.

    Used to report what doesn't match and why.

    Example:
        {
            "discrepancy_type": "missing_required",
            "severity": "critical",
            "column": "customer_id",
            "description": "Required column 'customer_id' not found in incoming data",
            "suggestion": "Check if column has a different name (e.g., 'id', 'cust_id')"
        }
    """

    discrepancy_type: str = Field(
        ...,
        description="Type: missing_required, extra_column, type_mismatch, value_mismatch"
    )

    severity: IssueSeverity = Field(
        default=IssueSeverity.WARNING,
        description="How serious is this discrepancy"
    )

    column: str | None = Field(
        default=None,
        description="Which column is affected"
    )

    description: str = Field(
        ...,
        description="Human-readable description"
    )

    suggestion: str | None = Field(
        default=None,
        description="Suggested resolution"
    )

    incoming_value: Any | None = Field(
        default=None,
        description="What the incoming data has"
    )

    expected_value: Any | None = Field(
        default=None,
        description="What the contract expects"
    )


class SchemaMatch(BaseModel):
    """
    Result of matching incoming data profile against a contract.

    This is the main output of schema matching - tells you:
    - Overall confidence score
    - How columns map between incoming and expected
    - What discrepancies exist
    - Whether to auto-process or request review

    Example usage:
        match = match_schema(incoming_profile, module_contract)

        if match.confidence_level == MatchConfidence.HIGH:
            # Auto-process with the mappings
            apply_transformations(data, match.column_mappings)
        elif match.confidence_level == MatchConfidence.MEDIUM:
            # Show user the mappings for confirmation
            show_mapping_review(match)
        else:
            # Reject or start fresh training
            reject_file(match.discrepancies)
    """

    # -------------------------------------------------------------------------
    # Match Results
    # -------------------------------------------------------------------------

    confidence_score: float = Field(
        default=0.0,
        ge=0.0,
        le=100.0,
        description="Overall match confidence (0-100%)"
    )

    confidence_level: MatchConfidence = Field(
        default=MatchConfidence.NO_MATCH,
        description="Categorical confidence level"
    )

    is_compatible: bool = Field(
        default=False,
        description="Whether the data can be processed (confidence >= 40%)"
    )

    auto_processable: bool = Field(
        default=False,
        description="Whether to auto-process (confidence >= 85%)"
    )

    # -------------------------------------------------------------------------
    # Column Mappings
    # -------------------------------------------------------------------------

    column_mappings: list[ColumnMapping] = Field(
        default_factory=list,
        description="How incoming columns map to contract columns"
    )

    unmapped_incoming: list[str] = Field(
        default_factory=list,
        description="Incoming columns that couldn't be mapped"
    )

    unmapped_required: list[str] = Field(
        default_factory=list,
        description="Required contract columns that weren't matched"
    )

    # -------------------------------------------------------------------------
    # Discrepancies
    # -------------------------------------------------------------------------

    discrepancies: list[SchemaDiscrepancy] = Field(
        default_factory=list,
        description="List of differences found"
    )

    # -------------------------------------------------------------------------
    # Metadata
    # -------------------------------------------------------------------------

    contract_id: str | None = Field(
        default=None,
        description="ID of the contract matched against"
    )

    module_name: str | None = Field(
        default=None,
        description="Name of the module for display"
    )

    match_timestamp: str | None = Field(
        default=None,
        description="When the match was performed"
    )

    # -------------------------------------------------------------------------
    # Methods
    # -------------------------------------------------------------------------

    def to_summary(self) -> str:
        """Generate a human-readable summary of the match."""
        lines = [
            "=" * 50,
            "SCHEMA MATCH RESULT",
            "=" * 50,
            "",
            f"Confidence: {self.confidence_score:.1f}% ({self.confidence_level.value})",
            f"Compatible: {'Yes' if self.is_compatible else 'No'}",
            f"Auto-processable: {'Yes' if self.auto_processable else 'No'}",
            "",
        ]

        # Column mappings
        if self.column_mappings:
            lines.append("COLUMN MAPPINGS:")
            for m in self.column_mappings:
                status = "✓" if m.confidence >= 0.8 else "?"
                lines.append(f"  {status} '{m.incoming_name}' → '{m.contract_name}' ({m.match_type}, {m.confidence*100:.0f}%)")
            lines.append("")

        # Unmapped columns
        if self.unmapped_incoming:
            lines.append(f"EXTRA COLUMNS (not in contract): {self.unmapped_incoming}")

        if self.unmapped_required:
            lines.append(f"MISSING REQUIRED: {self.unmapped_required}")

        # Discrepancies
        if self.discrepancies:
            lines.append("")
            lines.append("DISCREPANCIES:")
            for d in self.discrepancies:
                icon = "🔴" if d.severity == IssueSeverity.CRITICAL else "🟡"
                lines.append(f"  {icon} {d.description}")
                if d.suggestion:
                    lines.append(f"     → {d.suggestion}")

        return "\n".join(lines)


class ContractMatch(BaseModel):
    """
    Result of matching two contracts against each other.

    Used for workflow validation: Does Module A's output contract
    match Module B's input contract?

    Example:
        match = match_contracts(module_a.output_contract, module_b.input_contract)
        if match.is_chainable:
            # Modules can be connected
        else:
            # Show incompatibilities
    """

    confidence_score: float = Field(
        default=0.0,
        ge=0.0,
        le=100.0,
        description="How well the contracts align"
    )

    is_chainable: bool = Field(
        default=False,
        description="Whether modules can be connected"
    )

    source_contract_id: str | None = Field(
        default=None,
        description="ID of the source (output) contract"
    )

    target_contract_id: str | None = Field(
        default=None,
        description="ID of the target (input) contract"
    )

    column_mappings: list[ColumnMapping] = Field(
        default_factory=list,
        description="How columns align between contracts"
    )

    discrepancies: list[SchemaDiscrepancy] = Field(
        default_factory=list,
        description="Incompatibilities found"
    )
