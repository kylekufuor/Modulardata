# =============================================================================
# core/models/ - Pydantic Data Models
# =============================================================================
# This package contains Pydantic schemas for data validation:
# - profile.py: DataProfile schema (CSV metadata for AI context)
# - session.py: Session CRUD schemas
# - node.py: Node/version tracking schemas
# - chat.py: Chat request/response and task status schemas
# - plan.py: Session plan schemas (Plan Mode)
#
# These models define the "contract" between API and clients.
# =============================================================================

# -----------------------------------------------------------------------------
# Profile Models - CSV metadata for AI agents
# -----------------------------------------------------------------------------
from .profile import (
    ColumnProfile,
    ColumnStatistics,
    DataIssue,
    DataProfile,
    IssueSeverity,
    IssueType,
    ProfileSummary,
    SemanticType,
    ValueDistribution,
    # Schema Matching Models (Phase 2 Foundation)
    ColumnContract,
    ColumnMapping,
    ContractMatch,
    MatchConfidence,
    SchemaContract,
    SchemaDiscrepancy,
    SchemaMatch,
)

# -----------------------------------------------------------------------------
# Session Models - User session management
# -----------------------------------------------------------------------------
from .session import (
    SessionCreate,
    SessionList,
    SessionResponse,
    SessionStatus,
    SessionUpdate,
)

# -----------------------------------------------------------------------------
# Node Models - Version tracking (Time Travel)
# -----------------------------------------------------------------------------
from .node import (
    NodeCreate,
    NodeHistory,
    NodeResponse,
    RollbackRequest,
    RollbackResponse,
)

# -----------------------------------------------------------------------------
# Chat Models - Conversational interface
# -----------------------------------------------------------------------------
from .chat import (
    ChatMessage,
    ChatMode,
    ChatRequest,
    ChatResponse,
    ConversationHistory,
    MessageRole,
    PlanResponse,
    TaskError,
    TaskResult,
    TaskState,
    TaskStatus,
)

# -----------------------------------------------------------------------------
# Plan Models - Session Plan Mode
# -----------------------------------------------------------------------------
from .plan import (
    ApplyPlanRequest,
    ApplyPlanResponse,
    PlanStatus,
    SessionPlan,
    SessionPlanResponse,
    TransformationStep,
)

# -----------------------------------------------------------------------------
# __all__ - Explicit public API
# -----------------------------------------------------------------------------
# This tells Python which names to export when someone does:
# from core.models import *
__all__ = [
    # Profile
    "ColumnProfile",
    "ColumnStatistics",
    "DataIssue",
    "DataProfile",
    "IssueSeverity",
    "IssueType",
    "ProfileSummary",
    "SemanticType",
    "ValueDistribution",
    # Schema Matching
    "ColumnContract",
    "ColumnMapping",
    "ContractMatch",
    "MatchConfidence",
    "SchemaContract",
    "SchemaDiscrepancy",
    "SchemaMatch",
    # Session
    "SessionCreate",
    "SessionList",
    "SessionResponse",
    "SessionStatus",
    "SessionUpdate",
    # Node
    "NodeCreate",
    "NodeHistory",
    "NodeResponse",
    "RollbackRequest",
    "RollbackResponse",
    # Chat
    "ChatMessage",
    "ChatMode",
    "ChatRequest",
    "ChatResponse",
    "ConversationHistory",
    "MessageRole",
    "PlanResponse",
    "TaskError",
    "TaskResult",
    "TaskState",
    "TaskStatus",
    # Plan
    "ApplyPlanRequest",
    "ApplyPlanResponse",
    "PlanStatus",
    "SessionPlan",
    "SessionPlanResponse",
    "TransformationStep",
]
