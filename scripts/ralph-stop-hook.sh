#!/bin/bash
#
# Ralph Loop Stop Hook
# This hook is called when Claude Code tries to exit.
# It checks if the completion promise was found and blocks exit if not.
#

STATE_FILE="${RALPH_STATE_FILE:-}"
TRANSCRIPT_FILE="$1"  # Claude Code passes the transcript file path

# If not in a ralph loop, allow exit
if [[ -z "$STATE_FILE" ]] || [[ ! -f "$STATE_FILE" ]]; then
    exit 0
fi

# Read state
COMPLETION_PROMISE=$(jq -r '.completion_promise' "$STATE_FILE")
MAX_ITERATIONS=$(jq -r '.max_iterations' "$STATE_FILE")
CURRENT_ITERATION=$(jq -r '.current_iteration' "$STATE_FILE")

# Check if completion promise found in transcript
if [[ -n "$TRANSCRIPT_FILE" ]] && [[ -f "$TRANSCRIPT_FILE" ]]; then
    if grep -q "$COMPLETION_PROMISE" "$TRANSCRIPT_FILE"; then
        # Task complete, allow exit
        echo "RALPH: Completion promise found. Task complete."
        exit 0
    fi
fi

# Check max iterations
if [[ $CURRENT_ITERATION -ge $MAX_ITERATIONS ]]; then
    echo "RALPH: Max iterations reached ($MAX_ITERATIONS). Allowing exit."
    exit 0
fi

# Block exit and signal continuation
echo "RALPH: Completion promise not found. Blocking exit for iteration $((CURRENT_ITERATION + 1))."
exit 1
