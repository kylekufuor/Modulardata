# =============================================================================
# core/models/chat.py - Chat & Task Schemas
# =============================================================================
# These models define the API contract for conversational data transformation:
# - ChatRequest: User sends a message ("remove blank rows")
# - ChatResponse: Immediate response with task ID for polling
# - TaskStatus: Status of async processing (queued, processing, done, failed)
# - ChatMessage: Individual message in conversation history
#
# Flow:
# 1. User sends ChatRequest -> gets ChatResponse with task_id
# 2. User polls GET /task/{task_id} -> gets TaskStatus
# 3. When status="done", response includes new node_id with transformed data
# =============================================================================

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class MessageRole(str, Enum):
    """
    Who sent the message in a conversation.

    - user: The human user
    - assistant: The AI system (includes agent reasoning)
    """
    USER = "user"
    ASSISTANT = "assistant"


class ChatMode(str, Enum):
    """
    Chat interaction mode.

    Controls whether transformations are previewed or executed.

    - plan: Strategist creates plan, shows code preview WITHOUT executing
            User can review the plan and code before committing
    - transform: Full pipeline executes: plan → code → execute → new version
                 Transformation is applied immediately
    """
    PLAN = "plan"
    TRANSFORM = "transform"


class TaskState(str, Enum):
    """
    Possible states for an async chat task.

    State machine:
        queued -> processing -> done
                            \-> failed

    - queued: Task is waiting in Redis queue
    - processing: Celery worker is running the agents
    - done: Transformation complete, new node created
    - failed: Something went wrong (error message in response)
    """
    QUEUED = "queued"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


class ChatRequest(BaseModel):
    """
    Schema for sending a chat message.

    The user provides a natural language instruction like:
    - "Remove all rows where age is blank"
    - "Convert the date column to YYYY-MM-DD format"
    - "Undo that last change"

    Modes:
    - plan: Preview the transformation without executing (default)
    - transform: Execute the transformation immediately

    Example (plan mode - preview):
        {
            "session_id": "550e8400-...",
            "message": "Drop rows where the email column is empty",
            "mode": "plan"
        }

    Example (transform mode - execute):
        {
            "session_id": "550e8400-...",
            "message": "Drop rows where the email column is empty",
            "mode": "transform"
        }
    """

    # Which session to apply this to
    session_id: UUID = Field(
        ...,
        description="Session ID to apply transformation to"
    )

    # The user's natural language instruction
    message: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        description="User's transformation request in natural language"
    )

    # Interaction mode: plan (preview) or transform (execute)
    mode: ChatMode = Field(
        default=ChatMode.PLAN,
        description="Mode: 'plan' to preview, 'transform' to execute"
    )


class ChatResponse(BaseModel):
    """
    Immediate response after submitting a chat message.

    Since AI processing is async, we return a task_id immediately.
    Client polls GET /task/{task_id} to check progress.

    Example:
        {
            "task_id": "task_abc123",
            "status": "queued",
            "message": "Your request is being processed",
            "session_id": "550e8400-..."
        }
    """

    # Unique identifier for this task (for polling)
    task_id: str = Field(
        ...,
        description="Task ID for polling status"
    )

    # Current status of the task
    status: TaskState = Field(
        default=TaskState.QUEUED,
        description="Current task status"
    )

    # Human-readable status message
    message: str = Field(
        default="Your request is being processed",
        description="Status message"
    )

    # Echo back the session ID for convenience
    session_id: UUID = Field(
        ...,
        description="Session ID"
    )


class TaskStatus(BaseModel):
    """
    Detailed status of an async chat task.

    Returned by GET /task/{task_id}.
    Includes progress info and results when complete.

    Example (in progress):
        {
            "task_id": "task_abc123",
            "status": "processing",
            "progress": "Generating transformation code...",
            "started_at": "2024-01-15T10:30:00Z"
        }

    Example (complete):
        {
            "task_id": "task_abc123",
            "status": "done",
            "progress": "Complete",
            "result": {
                "node_id": "660e8400-...",
                "transformation": "Dropped 150 rows where email was empty",
                "rows_affected": 150
            }
        }
    """

    # Task identifier
    task_id: str = Field(
        ...,
        description="Task ID"
    )

    # Current state
    status: TaskState = Field(
        ...,
        description="Current task status"
    )

    # Human-readable progress message
    # Shows what the agents are doing: "Analyzing request...", "Generating code...", etc.
    progress: str = Field(
        default="Waiting...",
        description="Progress message"
    )

    # When task started processing (NULL if still queued)
    started_at: datetime | None = Field(
        default=None,
        description="When processing started"
    )

    # When task completed (NULL if not done)
    completed_at: datetime | None = Field(
        default=None,
        description="When processing completed"
    )

    # Result data (only present when status=done)
    result: "TaskResult | None" = Field(
        default=None,
        description="Result data (when complete)"
    )

    # Error info (only present when status=failed)
    error: "TaskError | None" = Field(
        default=None,
        description="Error details (when failed)"
    )


class TaskResult(BaseModel):
    """
    Successful task result data.

    Contains the new node created by the transformation
    and details about what changed.

    Example:
        {
            "node_id": "660e8400-...",
            "transformation": "Dropped 150 rows where email was empty",
            "rows_before": 1000,
            "rows_after": 850,
            "rows_affected": 150,
            "assistant_message": "I removed 150 rows that had empty email fields."
        }
    """

    # The new node (version) created by this transformation
    node_id: UUID = Field(
        ...,
        description="ID of the new data version"
    )

    # What transformation was applied
    transformation: str = Field(
        ...,
        description="Description of transformation"
    )

    # Row counts for context
    rows_before: int = Field(
        ...,
        ge=0,
        description="Row count before transformation"
    )

    rows_after: int = Field(
        ...,
        ge=0,
        description="Row count after transformation"
    )

    rows_affected: int = Field(
        default=0,
        ge=0,
        description="Number of rows changed/removed"
    )

    # The AI's response message to show the user
    assistant_message: str = Field(
        ...,
        description="AI response message"
    )


class TaskError(BaseModel):
    """
    Error details when a task fails.

    Contains enough info to understand what went wrong
    without exposing internal system details.

    Example:
        {
            "code": "INVALID_COLUMN",
            "message": "Column 'eamil' not found. Did you mean 'email'?",
            "details": {"requested": "eamil", "available": ["email", "name", "age"]}
        }
    """

    # Error code for programmatic handling
    code: str = Field(
        ...,
        description="Error code"
    )

    # Human-readable error message
    message: str = Field(
        ...,
        description="Error message"
    )

    # Additional details (optional)
    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional error details"
    )


class PlanResponse(BaseModel):
    """
    Response for plan mode - shows what WILL happen without executing.

    Returned when mode="plan". Allows user to:
    - Review the interpreted plan
    - See the generated code (when Engineer is called)
    - Decide whether to proceed with transform mode

    Example:
        {
            "session_id": "550e8400-...",
            "mode": "plan",
            "plan": {
                "transformation_type": "drop_rows",
                "target_columns": [{"column_name": "email"}],
                "explanation": "Remove rows where email is null",
                "confidence": 0.95
            },
            "generated_code": "df = df.dropna(subset=['email'])",
            "estimated_impact": {
                "rows_affected": 50,
                "description": "Approximately 50 rows will be removed"
            },
            "assistant_message": "I'll remove rows where email is blank. About 50 rows will be affected.",
            "can_execute": true,
            "clarification_needed": null
        }
    """

    # Echo back session ID
    session_id: UUID = Field(
        ...,
        description="Session ID"
    )

    # Confirm mode
    mode: ChatMode = Field(
        default=ChatMode.PLAN,
        description="Mode (always 'plan' for this response)"
    )

    # The Technical Plan created by the Strategist
    plan: dict[str, Any] = Field(
        ...,
        description="The transformation plan from Strategist"
    )

    # Generated pandas code (from Engineer, when available)
    generated_code: str | None = Field(
        default=None,
        description="Generated Python/pandas code (when Engineer is called)"
    )

    # Estimated impact of the transformation
    estimated_impact: dict[str, Any] | None = Field(
        default=None,
        description="Estimated rows affected, columns changed, etc."
    )

    # AI's explanation message
    assistant_message: str = Field(
        ...,
        description="AI explanation of the plan"
    )

    # Whether this plan can be executed
    can_execute: bool = Field(
        default=True,
        description="Whether the plan can be executed (false if clarification needed)"
    )

    # If confidence is low, question for user
    clarification_needed: str | None = Field(
        default=None,
        description="Question for user if plan is ambiguous"
    )

    # How to execute this plan
    execute_hint: str = Field(
        default="To execute this plan, send the same message with mode='transform'",
        description="Instructions for executing the plan"
    )


class ChatMessage(BaseModel):
    """
    A single message in the conversation history.

    Returned as part of conversation history endpoint.

    Example:
        {
            "id": "770e8400-...",
            "role": "user",
            "content": "Remove rows where age is blank",
            "created_at": "2024-01-15T10:30:00Z",
            "node_id": "660e8400-..."
        }
    """

    # Unique message ID
    id: UUID = Field(
        ...,
        description="Message ID"
    )

    # Who sent this message
    role: MessageRole = Field(
        ...,
        description="Message sender (user or assistant)"
    )

    # The message text
    content: str = Field(
        ...,
        description="Message content"
    )

    # When the message was sent
    created_at: datetime = Field(
        ...,
        description="Timestamp"
    )

    # Which data version this message relates to
    node_id: UUID | None = Field(
        default=None,
        description="Related node ID"
    )

    # Optional metadata (agent reasoning, generated code, etc.)
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata"
    )

    class Config:
        """Pydantic configuration."""
        from_attributes = True


class ConversationHistory(BaseModel):
    """
    Full conversation history for a session.

    Returned by GET /history/{session_id}/chat.
    Includes all messages in chronological order.

    Example:
        {
            "session_id": "550e8400-...",
            "messages": [...],
            "total_messages": 10
        }
    """

    # Which session this is for
    session_id: UUID = Field(
        ...,
        description="Session ID"
    )

    # All messages in order
    messages: list[ChatMessage] = Field(
        default_factory=list,
        description="List of messages"
    )

    # Total count
    total_messages: int = Field(
        default=0,
        ge=0,
        description="Total number of messages"
    )


# Update forward references for nested models
TaskStatus.model_rebuild()
