#!/bin/bash
#
# Ralph Loop - Autonomous iteration for Claude Code
# Named after Ralph Wiggum from The Simpsons
#
# Usage:
#   ./scripts/ralph-loop.sh "Your prompt here" --max-iterations 10 --completion-promise "DONE"
#
# The loop runs Claude Code repeatedly until:
#   1. The completion promise appears in output, OR
#   2. Max iterations reached
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
MAX_ITERATIONS=10
COMPLETION_PROMISE=""
PROMPT=""
STATE_FILE="/tmp/ralph-loop-state-$$.json"
LOG_FILE="/tmp/ralph-loop-log-$$.txt"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --max-iterations)
            MAX_ITERATIONS="$2"
            shift 2
            ;;
        --completion-promise)
            COMPLETION_PROMISE="$2"
            shift 2
            ;;
        --help|-h)
            echo "Ralph Loop - Autonomous iteration for Claude Code"
            echo ""
            echo "Usage: $0 \"<prompt>\" [options]"
            echo ""
            echo "Options:"
            echo "  --max-iterations N      Maximum iterations (default: 10)"
            echo "  --completion-promise S  String that signals completion"
            echo "  --help, -h              Show this help"
            echo ""
            echo "Example:"
            echo "  $0 \"Fix all TypeScript errors. Output <done>COMPLETE</done> when tsc passes.\" \\"
            echo "     --max-iterations 5 --completion-promise \"COMPLETE\""
            exit 0
            ;;
        *)
            if [[ -z "$PROMPT" ]]; then
                PROMPT="$1"
            fi
            shift
            ;;
    esac
done

# Validate inputs
if [[ -z "$PROMPT" ]]; then
    echo -e "${RED}Error: No prompt provided${NC}"
    echo "Usage: $0 \"<prompt>\" --completion-promise \"DONE\" --max-iterations 10"
    exit 1
fi

if [[ -z "$COMPLETION_PROMISE" ]]; then
    echo -e "${RED}Error: --completion-promise is required${NC}"
    echo "This string signals when the task is complete."
    exit 1
fi

# Banner
echo -e "${BLUE}"
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                      🔄 RALPH LOOP                            ║"
echo "║         Autonomous iteration for Claude Code                  ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

echo -e "${YELLOW}Configuration:${NC}"
echo "  Prompt: ${PROMPT:0:60}..."
echo "  Max iterations: $MAX_ITERATIONS"
echo "  Completion promise: $COMPLETION_PROMISE"
echo ""

# Initialize state
init_state() {
    cat > "$STATE_FILE" << EOF
{
    "prompt": $(echo "$PROMPT" | jq -Rs .),
    "max_iterations": $MAX_ITERATIONS,
    "completion_promise": "$COMPLETION_PROMISE",
    "current_iteration": 0,
    "status": "running",
    "started_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
    export RALPH_STATE_FILE="$STATE_FILE"
    export RALPH_LOG_FILE="$LOG_FILE"
}

# Check if completion promise found in output
check_completion() {
    if [[ -f "$LOG_FILE" ]] && grep -q "$COMPLETION_PROMISE" "$LOG_FILE"; then
        return 0
    fi
    return 1
}

# Update iteration count
increment_iteration() {
    local current=$(jq -r '.current_iteration' "$STATE_FILE")
    local new_count=$((current + 1))
    local tmp=$(mktemp)
    jq ".current_iteration = $new_count" "$STATE_FILE" > "$tmp" && mv "$tmp" "$STATE_FILE"
    echo $new_count
}

# Get current iteration
get_iteration() {
    jq -r '.current_iteration' "$STATE_FILE"
}

# Cleanup
cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"
    rm -f "$STATE_FILE" 2>/dev/null || true
    # Keep log file for inspection
    if [[ -f "$LOG_FILE" ]]; then
        echo -e "Log saved to: ${BLUE}$LOG_FILE${NC}"
    fi
}

trap cleanup EXIT

# Main loop
run_ralph_loop() {
    init_state

    local iteration=0
    local completed=false

    while [[ $iteration -lt $MAX_ITERATIONS ]]; do
        iteration=$(increment_iteration)

        echo ""
        echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
        echo -e "${GREEN}Iteration $iteration of $MAX_ITERATIONS${NC}"
        echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
        echo ""

        # Build the prompt for this iteration
        local iteration_prompt="$PROMPT"
        if [[ $iteration -gt 1 ]]; then
            iteration_prompt="[RALPH LOOP - Iteration $iteration/$MAX_ITERATIONS]

Previous iteration did not complete the task. The completion signal '$COMPLETION_PROMISE' was not found.

Continue working on the original task:

$PROMPT

When complete, output: <promise>$COMPLETION_PROMISE</promise>"
        fi

        # Run Claude Code and capture output
        echo -e "${YELLOW}Running Claude Code...${NC}"
        echo ""

        # Use script to capture output while still showing it
        # The -q flag quiets script's own messages
        if command -v script &> /dev/null; then
            # macOS/BSD script syntax
            script -q "$LOG_FILE" claude --print "$iteration_prompt" 2>&1 || true
        else
            # Fallback: just pipe to tee
            claude --print "$iteration_prompt" 2>&1 | tee "$LOG_FILE" || true
        fi

        # Check for completion
        if check_completion; then
            echo ""
            echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
            echo -e "${GREEN}✅ COMPLETION PROMISE FOUND!${NC}"
            echo -e "${GREEN}   Task completed in $iteration iteration(s)${NC}"
            echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
            completed=true
            break
        fi

        echo ""
        echo -e "${YELLOW}Completion promise not found. Continuing...${NC}"

        # Small delay between iterations
        sleep 2
    done

    if [[ "$completed" != "true" ]]; then
        echo ""
        echo -e "${RED}═══════════════════════════════════════════════════════════════${NC}"
        echo -e "${RED}⚠️  MAX ITERATIONS REACHED ($MAX_ITERATIONS)${NC}"
        echo -e "${RED}   Task may not be complete. Review the changes manually.${NC}"
        echo -e "${RED}═══════════════════════════════════════════════════════════════${NC}"
        exit 1
    fi
}

# Check dependencies
if ! command -v claude &> /dev/null; then
    echo -e "${RED}Error: 'claude' command not found${NC}"
    echo "Please ensure Claude Code CLI is installed and in your PATH"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo -e "${RED}Error: 'jq' command not found${NC}"
    echo "Please install jq: brew install jq"
    exit 1
fi

# Confirm before starting
echo -e "${YELLOW}⚠️  WARNING: This will run Claude Code autonomously up to $MAX_ITERATIONS times.${NC}"
echo -e "${YELLOW}   This can consume significant API credits.${NC}"
echo ""
read -p "Continue? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# Ensure we're in a git repo for safety
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo -e "${RED}Error: Not in a git repository${NC}"
    echo "Ralph loop should only run in git-tracked directories for safety."
    exit 1
fi

# Show current git status
echo ""
echo -e "${YELLOW}Current git status:${NC}"
git status --short
echo ""

read -p "Proceed with ralph loop? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# Run the loop
run_ralph_loop
