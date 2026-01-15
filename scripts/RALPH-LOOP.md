# Ralph Loop - Autonomous Iteration for Claude Code

Named after Ralph Wiggum from The Simpsons, this tool runs Claude Code repeatedly until a task is complete.

## Philosophy

> "Don't aim for perfect on first try. Let the loop refine the work."

Instead of carefully reviewing each step, you define success criteria upfront and let Claude iterate toward them. Failures become data.

## Quick Start

```bash
./scripts/ralph-loop.sh \
  "Fix all TypeScript errors in the project. Output <promise>BUILD_CLEAN</promise> when npm run build passes with no errors." \
  --max-iterations 10 \
  --completion-promise "BUILD_CLEAN"
```

## How It Works

1. You provide a prompt with clear completion criteria
2. Claude works on the task
3. Script checks if completion promise appeared in output
4. If not found and iterations remain, runs again with context
5. Repeats until success or max iterations

```
┌─────────────────────────────────────────────────────────┐
│                                                         │
│   ┌──────────┐    ┌──────────┐    ┌──────────────────┐ │
│   │  Prompt  │───►│  Claude  │───►│ Check Completion │ │
│   └──────────┘    └──────────┘    └────────┬─────────┘ │
│        ▲                                    │           │
│        │            ┌───────────┐           │           │
│        └────────────┤ Not Found ├───────────┘           │
│                     └───────────┘                       │
│                           │                             │
│                     ┌─────▼─────┐                       │
│                     │   Found   │──────► EXIT          │
│                     └───────────┘                       │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## Usage

```bash
./scripts/ralph-loop.sh "<prompt>" [options]

Options:
  --max-iterations N       Maximum iterations (default: 10)
  --completion-promise S   String that signals completion (REQUIRED)
  --help, -h              Show help
```

## Writing Good Prompts

### Include Clear Success Criteria

```bash
# Good - clear exit condition
"Fix all ESLint errors. Output <promise>LINT_CLEAN</promise> when 'npm run lint' exits with code 0."

# Bad - vague
"Fix the linting issues"
```

### Be Specific About Verification

```bash
# Good - tells Claude how to verify
"Implement the user API endpoints. Run 'pytest tests/test_user_api.py' to verify.
Output <promise>TESTS_PASS</promise> when all tests pass."

# Bad - no verification method
"Implement the user API"
```

### Include Iteration Context

The script automatically adds iteration context on subsequent runs, but you can enhance this:

```bash
"Migrate all components from class-based to functional React.
After each component, run 'npm run build' to verify it still compiles.
Keep track of progress. Output <promise>MIGRATION_DONE</promise> when all components are converted."
```

## Best Use Cases

| Task | Why It Works |
|------|--------------|
| Fix all lint/type errors | Clear pass/fail, mechanical fixes |
| Large refactors | Incremental progress, testable |
| Dependency upgrades | Clear completion criteria |
| API migrations | Systematic, verifiable |
| Test coverage | Measurable target |

## Cost Awareness

**Ralph loops consume API credits.** Each iteration is a full Claude conversation.

Estimate: ~$1-5 per iteration depending on context size.

A 10-iteration loop could cost $10-50+.

**Always:**
- Set reasonable `--max-iterations`
- Run in a git repo (can revert if needed)
- Start with smaller tasks to calibrate

## Safety Features

1. **Git requirement**: Won't run outside a git repository
2. **Confirmation prompts**: Asks before starting
3. **Max iterations**: Hard limit prevents runaway loops
4. **Logs preserved**: Output saved for review

## Examples

### Fix TypeScript Errors

```bash
./scripts/ralph-loop.sh \
  "Fix all TypeScript compilation errors. Run 'npm run build' after each fix to check progress. Output <promise>TSC_CLEAN</promise> when build succeeds with no type errors." \
  --max-iterations 8 \
  --completion-promise "TSC_CLEAN"
```

### Implement API Endpoints

```bash
./scripts/ralph-loop.sh \
  "Implement the remaining CRUD endpoints in app/api/routes.py based on the OpenAPI spec in docs/api.yaml. Run 'pytest tests/api/' after each endpoint. Output <promise>API_COMPLETE</promise> when all endpoints pass their tests." \
  --max-iterations 15 \
  --completion-promise "API_COMPLETE"
```

### Add Test Coverage

```bash
./scripts/ralph-loop.sh \
  "Add unit tests to achieve 80% coverage for src/services/. Run 'npm run test:coverage' to check. Current coverage shown in output. Output <promise>COVERAGE_MET</promise> when coverage >= 80%." \
  --max-iterations 12 \
  --completion-promise "COVERAGE_MET"
```

## Troubleshooting

### Loop exits immediately
- Check that `--completion-promise` doesn't appear in your prompt itself
- Ensure Claude is actually outputting the promise string

### Loop never completes
- Make the success criteria more achievable
- Check if the task is actually possible
- Review logs to see what's failing

### High costs
- Start with `--max-iterations 3` to test
- Use more specific prompts to reduce iterations needed
- Break large tasks into smaller sub-tasks

## Files

```
scripts/
├── ralph-loop.sh        # Main runner script
├── ralph-stop-hook.sh   # Hook for Claude Code integration
└── RALPH-LOOP.md        # This documentation

.claude/
└── settings.json        # Hook configuration
```
