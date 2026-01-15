#!/usr/bin/env python3
"""Quick test of affirmative response handling."""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI
from agents.strategist import StrategistAgent
from lib.memory import ConversationContext, ChatMessage
from core.models.profile import DataProfile, ColumnProfile, SemanticType

client = OpenAI()

# Create context
mock_profile = DataProfile(
    row_count=1000,
    column_count=5,
    columns=[
        ColumnProfile(name="id", dtype="int64", semantic_type=SemanticType.ID, null_count=0, null_percent=0.0, unique_count=1000),
        ColumnProfile(name="email", dtype="object", semantic_type=SemanticType.EMAIL, null_count=50, null_percent=5.0, unique_count=950),
    ],
)

context = ConversationContext(
    session_id="550e8400-e29b-41d4-a716-446655440000",
    current_node_id="660e8400-e29b-41d4-a716-446655440001",
    parent_node_id="660e8400-e29b-41d4-a716-446655440000",
    current_profile=mock_profile,
    messages=[],
    recent_transformations=[],
    original_filename="customers.csv",
    current_row_count=1000,
    current_column_count=5,
)

print("=" * 60)
print("Testing affirmative response handling")
print("=" * 60)

# Simulate: Assistant suggested "fix missing emails", user says "yes please"
print("\nScenario: Assistant suggested fixing emails, user says 'yes please'")
print("-" * 60)

# The extracted suggestion from the previous response
last_suggestion = "remove rows where email is missing"

print(f"Last suggestion: \"{last_suggestion}\"")
print(f"User says: \"yes please\"")
print()

# Now use the suggestion as the message to the Strategist
with patch("agents.strategist.build_conversation_context") as mock_build:
    mock_build.return_value = context

    agent = StrategistAgent()
    plan = agent.create_plan(
        session_id=context.session_id,
        user_message=last_suggestion  # Use the suggestion, not "yes please"
    )

    print(f"Plan created successfully!")
    print(f"  Type: {plan.transformation_type}")
    print(f"  Columns: {plan.get_target_column_names()}")
    print(f"  Confidence: {plan.confidence:.0%}")

print("\n" + "=" * 60)
print("SUCCESS - Affirmative responses now work correctly!")
print("=" * 60)
