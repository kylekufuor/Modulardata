# =============================================================================
# agents/strategist.py - Context Strategist Agent (Agent A)
# =============================================================================
# This module implements the Context Strategist, the first agent in the
# 3-agent data transformation pipeline.
#
# The Strategist's job:
# 1. Fetch conversation context from Supabase
# 2. Analyze the user's request in context of the data profile
# 3. Output a structured Technical Plan for the Engineer agent
#
# Design follows Anthropic's principles:
# - Start simple (direct OpenAI calls, not framework)
# - Feedback loop (gather context → create plan → verify)
# - Actionable errors (tell HOW to fix, not just WHAT failed)
#
# Usage:
#   from agents.strategist import StrategistAgent
#   agent = StrategistAgent()
#   plan = agent.create_plan(session_id, "remove rows where email is blank")
# =============================================================================

from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

from openai import OpenAI
from pydantic import ValidationError

from app.config import settings
from agents.models.technical_plan import TechnicalPlan
from agents.prompts.strategist_system import build_strategist_prompt
from lib.memory import (
    build_conversation_context,
    ConversationContext,
    format_messages_for_openai,
)
from lib.supabase_client import SupabaseClientError

# Set up logging for this module
logger = logging.getLogger(__name__)


# =============================================================================
# Exceptions
# =============================================================================

class StrategyError(Exception):
    """
    Error during strategy creation.

    Provides actionable error messages following Anthropic's principle:
    "Errors should tell HOW to fix, not just WHAT failed."

    Attributes:
        code: Error code for categorization
        message: Human-readable error message
        suggestion: Actionable suggestion for fixing the error
        details: Additional context for debugging
    """

    def __init__(
        self,
        message: str,
        code: str = "STRATEGY_ERROR",
        suggestion: str | None = None,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.suggestion = suggestion
        self.details = details or {}

    def __str__(self) -> str:
        result = f"[{self.code}] {self.message}"
        if self.suggestion:
            result += f" Suggestion: {self.suggestion}"
        return result


# =============================================================================
# Strategist Agent
# =============================================================================

class StrategistAgent:
    """
    Agent A: The Context Strategist.

    Transforms vague user requests into structured Technical Plans
    that the Engineer agent can execute.

    Example:
        agent = StrategistAgent()

        # Simple transformation request
        plan = agent.create_plan(
            session_id="550e8400-...",
            user_message="remove rows where email is blank"
        )

        # Plan is now a TechnicalPlan object ready for Engineer
        print(plan.transformation_type)  # "drop_rows"
        print(plan.explanation)  # "Remove all rows where email is null"

    Attributes:
        model: OpenAI model to use (default from settings)
        temperature: Generation temperature (default 0.2 for consistency)
        max_context_messages: Max conversation messages to include
    """

    def __init__(
        self,
        model: str | None = None,
        temperature: float | None = None,
        max_context_messages: int | None = None,
    ):
        """
        Initialize the Strategist Agent.

        Args:
            model: OpenAI model ID (default: settings.OPENAI_MODEL)
            temperature: Generation temperature (default: settings.STRATEGIST_TEMPERATURE)
            max_context_messages: Max messages to include (default: settings.STRATEGIST_MAX_MESSAGES)
        """
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        self.model = model or settings.OPENAI_MODEL
        self.temperature = temperature if temperature is not None else settings.STRATEGIST_TEMPERATURE
        self.max_context_messages = max_context_messages or settings.STRATEGIST_MAX_MESSAGES

        logger.info(f"StrategistAgent initialized with model={self.model}, temp={self.temperature}")

    # -------------------------------------------------------------------------
    # Main Entry Point
    # -------------------------------------------------------------------------

    def create_plan(
        self,
        session_id: str | UUID,
        user_message: str,
    ) -> TechnicalPlan:
        """
        Create a Technical Plan from a user message.

        This is the main entry point for the Strategist agent.
        It follows Anthropic's feedback loop pattern:
        1. Gather context (from Supabase)
        2. Create plan (via OpenAI)
        3. Validate result (via Pydantic)

        Args:
            session_id: The session UUID
            user_message: The user's transformation request

        Returns:
            TechnicalPlan ready for the Engineer agent

        Raises:
            StrategyError: If plan creation fails

        Example:
            plan = agent.create_plan(
                session_id="550e8400-...",
                user_message="remove rows where email is blank"
            )
        """
        session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id
        logger.info(f"Creating plan for session {session_id_str}: '{user_message[:50]}...'")

        # ---------------------------------------------------------------------
        # Step 1: Gather Context
        # ---------------------------------------------------------------------
        try:
            context = build_conversation_context(
                session_id=session_id_str,
                message_limit=self.max_context_messages,
                transformation_depth=5,  # Look back at recent transformations
            )
        except SupabaseClientError as e:
            raise StrategyError(
                message=f"Failed to fetch context: {e.message}",
                code="CONTEXT_FETCH_FAILED",
                suggestion=e.suggestion,
                details={"session_id": session_id_str}
            )

        # ---------------------------------------------------------------------
        # Step 2: Build Messages for OpenAI
        # ---------------------------------------------------------------------
        messages = self._build_messages(context, user_message)

        # ---------------------------------------------------------------------
        # Step 3: Call OpenAI
        # ---------------------------------------------------------------------
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                temperature=self.temperature,
                response_format={"type": "json_object"},  # Force JSON output
                messages=messages,
            )

            response_text = response.choices[0].message.content or ""
            logger.debug(f"OpenAI response: {response_text[:200]}...")

        except Exception as e:
            raise StrategyError(
                message=f"OpenAI API call failed: {e}",
                code="OPENAI_ERROR",
                suggestion="Check your OPENAI_API_KEY and network connection",
                details={"model": self.model}
            )

        # ---------------------------------------------------------------------
        # Step 4: Parse Response
        # ---------------------------------------------------------------------
        plan = self._parse_response(response_text)

        # ---------------------------------------------------------------------
        # Step 5: Post-Process (resolve references)
        # ---------------------------------------------------------------------
        plan = self._post_process_plan(plan, context)

        logger.info(f"Plan created: {plan.transformation_type} with confidence {plan.confidence}")
        return plan

    # -------------------------------------------------------------------------
    # Message Building
    # -------------------------------------------------------------------------

    def _build_messages(
        self,
        context: ConversationContext,
        user_message: str,
    ) -> list[dict[str, str]]:
        """
        Build the OpenAI messages array.

        Structure:
        1. System prompt with data profile and state
        2. Recent conversation history (for context)
        3. Current user message

        Args:
            context: ConversationContext from memory module
            user_message: The current user message

        Returns:
            List of message dicts for OpenAI API
        """
        messages = []

        # ---------------------------------------------------------------------
        # 1. System Prompt
        # ---------------------------------------------------------------------
        # Build profile summary for the prompt
        if context.current_profile:
            profile_summary = context.current_profile.to_text_summary(verbose=False)
        else:
            profile_summary = f"Dataset: {context.current_row_count} rows x {context.current_column_count} columns"

        # Get recent transformation descriptions
        recent_transformations = [
            t.transformation for t in context.recent_transformations
            if t.transformation
        ]

        # Build the full system prompt
        system_prompt = build_strategist_prompt(
            profile_summary=profile_summary,
            recent_transformations=recent_transformations,
            current_node_id=context.current_node_id,
            parent_node_id=context.parent_node_id,
        )

        messages.append({
            "role": "system",
            "content": system_prompt,
        })

        # ---------------------------------------------------------------------
        # 2. Conversation History (for referential context)
        # ---------------------------------------------------------------------
        # Include recent messages so agent can understand "that column", etc.
        if context.messages:
            history_messages = format_messages_for_openai(context.messages[-5:])
            messages.extend(history_messages)

        # ---------------------------------------------------------------------
        # 3. Current User Message
        # ---------------------------------------------------------------------
        messages.append({
            "role": "user",
            "content": user_message,
        })

        logger.debug(f"Built {len(messages)} messages for OpenAI")
        return messages

    # -------------------------------------------------------------------------
    # Response Parsing
    # -------------------------------------------------------------------------

    def _parse_response(self, response_text: str) -> TechnicalPlan:
        """
        Parse OpenAI response into a TechnicalPlan.

        Handles:
        - JSON parsing errors
        - Pydantic validation errors
        - Missing required fields

        Args:
            response_text: Raw response from OpenAI

        Returns:
            Validated TechnicalPlan object

        Raises:
            StrategyError: If parsing fails
        """
        # Try to parse JSON
        try:
            data = json.loads(response_text)
        except json.JSONDecodeError as e:
            raise StrategyError(
                message=f"Invalid JSON response from model: {e}",
                code="JSON_PARSE_ERROR",
                suggestion="The model didn't return valid JSON. Try rephrasing your request.",
                details={"raw_response": response_text[:500]}
            )

        # Try to validate with Pydantic
        try:
            plan = TechnicalPlan.model_validate(data)
            return plan
        except ValidationError as e:
            # Extract the specific validation errors
            errors = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            raise StrategyError(
                message=f"Invalid plan structure: {'; '.join(errors)}",
                code="VALIDATION_ERROR",
                suggestion="The model's response was valid JSON but missing required fields.",
                details={"validation_errors": e.errors(), "raw_data": data}
            )

    # -------------------------------------------------------------------------
    # Post-Processing
    # -------------------------------------------------------------------------

    def _post_process_plan(
        self,
        plan: TechnicalPlan,
        context: ConversationContext,
    ) -> TechnicalPlan:
        """
        Post-process the plan to resolve references.

        Handles:
        - UNDO: Ensure rollback_to_node_id is set
        - Column validation: Check columns exist in profile
        - Confidence adjustment: Lower if columns don't match

        Args:
            plan: The initial TechnicalPlan from parsing
            context: The conversation context

        Returns:
            Post-processed TechnicalPlan
        """
        # Handle UNDO operations
        if plan.is_undo():
            plan = self._resolve_undo(plan, context)

        # Validate target columns exist
        if plan.target_columns and context.current_profile:
            plan = self._validate_columns(plan, context)

        return plan

    def _resolve_undo(
        self,
        plan: TechnicalPlan,
        context: ConversationContext,
    ) -> TechnicalPlan:
        """
        Resolve the target node for UNDO operations.

        If the model didn't set rollback_to_node_id, we set it
        to the parent_node_id from context.

        Args:
            plan: TechnicalPlan with transformation_type=UNDO
            context: ConversationContext with parent_node_id

        Returns:
            Updated TechnicalPlan with rollback_to_node_id set
        """
        if not plan.rollback_to_node_id and context.parent_node_id:
            # Create a new plan with the rollback ID set
            plan_dict = plan.model_dump()
            plan_dict["rollback_to_node_id"] = context.parent_node_id
            return TechnicalPlan.model_validate(plan_dict)

        if not plan.rollback_to_node_id and not context.parent_node_id:
            # Can't undo - at the original node
            plan_dict = plan.model_dump()
            plan_dict["confidence"] = 0.3
            plan_dict["clarification_needed"] = "Cannot undo: you're at the original version of the data."
            return TechnicalPlan.model_validate(plan_dict)

        return plan

    def _validate_columns(
        self,
        plan: TechnicalPlan,
        context: ConversationContext,
    ) -> TechnicalPlan:
        """
        Validate that target columns exist in the profile.

        If columns don't match exactly, tries fuzzy matching and
        adjusts confidence accordingly.

        Args:
            plan: TechnicalPlan with target_columns
            context: ConversationContext with current_profile

        Returns:
            Updated TechnicalPlan (possibly with adjusted confidence)
        """
        if not context.current_profile:
            return plan

        profile_columns = context.get_column_names()
        profile_columns_lower = {c.lower(): c for c in profile_columns}

        plan_dict = plan.model_dump()
        needs_update = False
        missing_columns = []

        for i, target in enumerate(plan.target_columns):
            col_name = target.column_name

            # Check exact match
            if col_name in profile_columns:
                continue

            # Check case-insensitive match
            if col_name.lower() in profile_columns_lower:
                # Update to correct case
                plan_dict["target_columns"][i]["column_name"] = profile_columns_lower[col_name.lower()]
                needs_update = True
                logger.debug(f"Column '{col_name}' matched to '{profile_columns_lower[col_name.lower()]}'")
                continue

            # Column not found
            missing_columns.append(col_name)

        if missing_columns:
            # Lower confidence and ask for clarification
            plan_dict["confidence"] = min(plan_dict.get("confidence", 1.0), 0.5)

            # Try to suggest similar columns
            suggestions = self._find_similar_columns(missing_columns[0], profile_columns)
            if suggestions:
                plan_dict["clarification_needed"] = (
                    f"Column '{missing_columns[0]}' not found. "
                    f"Did you mean: {', '.join(suggestions)}?"
                )
            else:
                plan_dict["clarification_needed"] = (
                    f"Column '{missing_columns[0]}' not found. "
                    f"Available columns: {', '.join(profile_columns[:5])}..."
                )
            needs_update = True

        if needs_update:
            return TechnicalPlan.model_validate(plan_dict)

        return plan

    def _find_similar_columns(
        self,
        target: str,
        columns: list[str],
        max_suggestions: int = 3,
    ) -> list[str]:
        """
        Find columns similar to the target name.

        Uses simple substring matching and common patterns.

        Args:
            target: The column name to find
            columns: Available column names
            max_suggestions: Maximum suggestions to return

        Returns:
            List of similar column names
        """
        target_lower = target.lower()
        suggestions = []

        for col in columns:
            col_lower = col.lower()

            # Check for substring match
            if target_lower in col_lower or col_lower in target_lower:
                suggestions.append(col)
                continue

            # Check for common word overlap
            target_words = set(target_lower.replace("_", " ").replace("-", " ").split())
            col_words = set(col_lower.replace("_", " ").replace("-", " ").split())
            if target_words & col_words:
                suggestions.append(col)

        return suggestions[:max_suggestions]

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    def create_plan_with_context(
        self,
        context: ConversationContext,
        user_message: str,
    ) -> TechnicalPlan:
        """
        Create a plan using pre-built context.

        Useful when you've already fetched context and don't want
        to fetch again.

        Args:
            context: Pre-built ConversationContext
            user_message: The user's transformation request

        Returns:
            TechnicalPlan ready for Engineer
        """
        messages = self._build_messages(context, user_message)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                temperature=self.temperature,
                response_format={"type": "json_object"},
                messages=messages,
            )
            response_text = response.choices[0].message.content or ""
        except Exception as e:
            raise StrategyError(
                message=f"OpenAI API call failed: {e}",
                code="OPENAI_ERROR",
                suggestion="Check your OPENAI_API_KEY and network connection"
            )

        plan = self._parse_response(response_text)
        plan = self._post_process_plan(plan, context)

        return plan
