# Anthropic Agent Best Practices Reference

This document consolidates key principles from Anthropic's engineering blog for building effective agents.
Reference this when designing and implementing agents for ModularData.

---

## 1. Context Engineering

### Right Altitude Approach
- System prompts should be **specific enough to guide behavior effectively, yet flexible enough to provide strong heuristics**
- Avoid overly rigid, hardcoded logic AND vague, high-level guidance

### Organization Strategy
Structure prompts into distinct sections using XML tags or Markdown headers:
```
<background_information>
<instructions>
## Tool guidance
## Output description
```

### Token Management
- **Treat context as finite** - find the smallest set of high-signal tokens
- **Compaction**: Summarize conversations approaching limits, preserve architectural decisions
- **Structured Note-Taking**: External notes for persistent memory, retrieved when needed
- **Sub-Agent Architectures**: Specialized agents return condensed summaries (1,000-2,000 tokens)

### What to Include vs Exclude

**Include**:
- Diverse, canonical examples (few-shot prompting)
- Clear, self-contained tool definitions with minimal functional overlap
- Metadata signals (file hierarchies, naming conventions)
- Critical context requiring immediate attention

**Exclude**:
- Exhaustive edge case lists in prompts
- Redundant tool outputs from deep message history
- Bloated tool sets causing ambiguous decision points

### Just-In-Time Retrieval
Rather than pre-loading all data, maintain lightweight identifiers and dynamically load information via tools during execution. Mirrors human cognition—retrieve on demand rather than memorizing everything.

---

## 2. Agent Architecture Patterns

### Start Simple
> "Finding the simplest solution possible, and only increasing complexity when needed"

### When to Use What

| Pattern | Best For |
|---------|----------|
| **Workflows** | Well-defined tasks with predictable subtasks |
| **Agents** | Open-ended problems where steps can't be predicted |

### Workflow Patterns
1. **Prompt Chaining**: Sequential LLM calls processing previous outputs
2. **Routing**: Classifying inputs to specialized downstream tasks
3. **Parallelization**: Running independent subtasks simultaneously
4. **Orchestrator-Workers**: Central LLM dynamically delegates to workers
5. **Evaluator-Optimizer**: Iterative refinement loops with feedback

### Agent Feedback Loop
```
Gather Context → Take Action → Verify Work → Repeat
```

---

## 3. Tool Design Principles

### Core Insight
> Invest in tools as much as in prompts. "Spent more time optimizing tools than the overall prompt."

### Design Guidelines

**Intentional Tools**:
- More tools don't always lead to better outcomes
- Avoid wrapping every API endpoint
- Target specific high-impact workflows

**Consolidate Functionality**:
- Combine multiple operations under one tool when logical
- Example: `schedule_event` (finds availability internally) vs separate `list_users`, `list_events`, `create_event`

**Use Namespacing**:
- Group related tools under common prefixes
- Example: `asana_search`, `asana_projects_search`

### ACI (Agent-Computer Interface)
> "Put yourself in the model's shoes"

Tool definitions should include:
- Example usage
- Edge cases
- Input format requirements
- Clear boundaries from other tools

### Poka-Yoke Principle
> "Change the arguments so that it is harder to make mistakes"

Example: Switching to absolute filepaths yielded "flawless" usage.

### Input/Output Design

**Inputs**:
- Name parameters clearly (`user_id` not `user`)
- Avoid cryptic identifiers

**Outputs**:
- Return high-signal information
- Use semantic identifiers over UUIDs
- Support `response_format` parameter (concise vs detailed)

### Error Handling
- Actionable error messages with specific guidance
- Include examples of correct format in errors
- When truncating, include instructions for better strategies

### Documentation
> "Write for new hires" - describe tools as you would to a team member

- Make implicit context explicit
- Document specialized query formats
- Even small refinements yield dramatic improvements

---

## 4. Long-Running Agent Harnesses

### Two-Part Architecture
1. **Initializer Agent**: Runs once to establish environment
2. **Coding Agent**: Executes in subsequent sessions, incremental progress

### State Management
- `progress.txt`: Documents what previous sessions accomplished
- Git history: Recoverable timeline of changes
- `init.sh`: Standardizes environment startup
- `feature_list.json`: Prevents premature completion

### Recovery Mechanisms
- Git commits for reverting bad changes
- Initial health checks catch bugs from previous sessions
- Structured progress notes enable quick context restoration

### Session Pattern
```
1. Get bearings (pwd, read progress file)
2. Review git log
3. Check feature requirements
4. Start development server
5. Test basic functionality
6. Begin work
```

---

## 5. Prompt Engineering

### Clarity Matters
- "Give the model enough tokens to think before it writes itself into a corner"
- Keep formats close to what model has seen naturally in text
- Eliminate formatting overhead (counting lines, string-escaping)

### Testing Approach
> "Run many example inputs in our workbench to see what mistakes the model makes, and iterate."

### Few-Shot Examples
Include diverse, canonical examples demonstrating expected behavior.

---

## 6. Three Core Principles

1. **Simplicity**: Maintain simple agent design
2. **Transparency**: Explicitly show planning steps
3. **Documentation & Testing**: Craft carefully for agent-computer interface

---

## 7. Framework Advice

> "Start by using LLM APIs directly: many patterns can be implemented in a few lines of code."

Use frameworks as helpers, not abstractions that obscure underlying logic.

> "Add complexity only when it demonstrably improves outcomes."

---

## 8. Verification Patterns

### Rules-Based Feedback
- Code linting
- Formal validation rules

### Visual Feedback
- Screenshots
- Renders for UI verification

### LLM as Judge
- Use another model to evaluate outputs against fuzzy criteria

---

## Quick Reference Checklist

When building an agent:

- [ ] Start with simplest possible solution
- [ ] Design tools with clear, unambiguous parameters
- [ ] Use XML tags or markdown headers in prompts
- [ ] Include few-shot examples
- [ ] Implement actionable error messages
- [ ] Support response format control (concise/detailed)
- [ ] Test with many example inputs
- [ ] Preserve context through structured notes
- [ ] Use Just-In-Time retrieval over pre-loading
- [ ] Document tools like for a new team member
