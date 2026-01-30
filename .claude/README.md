# Claude Prompt Management System

This directory contains reusable prompt artifacts for AI-assisted development tasks.

## Quick Start

```bash
# List available prompts
make claude-list

# Run a prompt (outputs to console for copy-paste)
make claude-run PROMPT=cleanup

# Run with direct execution (requires ANTHROPIC_API_KEY)
make claude-exec PROMPT=documentation
```

## Directory Structure

```
.claude/
├── README.md        # This file
├── config.json      # API configuration
├── prompts/         # Reusable prompt files
├── templates/       # Templates for new prompts
└── history/         # Execution logs (gitignored)
```

## Available Prompts

| Prompt | Description | Use Case |
|--------|-------------|----------|
| `cleanup` | Project cleanup specialist | Remove deprecated files, organize structure |
| `documentation` | Technical documentation specialist | Update READMEs, create docs |
| `code-review` | Code review specialist | Review PRs, suggest improvements |
| `data-pipeline` | Data pipeline debugger | Debug ingestion issues |
| `api-design` | API design specialist | Design new endpoints |

## Usage Examples

### Display Prompt (Copy-Paste to Claude)

```bash
# Basic usage - displays prompt content
make claude-run PROMPT=cleanup

# Include project context (git status, directory structure)
make claude-run PROMPT=documentation CONTEXT=1

# Save to file instead of stdout
./scripts/claude_run.sh cleanup --output cleanup_prompt.txt
```

### Execute via API

```bash
# Set your API key (one-time)
export ANTHROPIC_API_KEY='sk-ant-...'

# Execute prompt and get response
make claude-exec PROMPT=cleanup

# Or with the script directly
./scripts/claude_run.sh cleanup --exec
```

### Create New Prompts

```bash
# Create from template
make claude-new NAME=my-prompt

# Edit the new prompt
code .claude/prompts/my-prompt.md

# Test it
make claude-run PROMPT=my-prompt
```

## Prompt File Format

Each prompt file uses Markdown with YAML frontmatter:

```markdown
---
name: Prompt Name
version: 1.0.0
description: Short description
author: Your Name
created: 2026-01-30
tags: [cleanup, automation]
estimated_tokens: 500
use_case: When to use this prompt
---

# Role Definition

You are a [role] with [experience]...

# Context

[Project-specific context]

# Task

[What the prompt should accomplish]

# Constraints

[Limitations and rules]

# Deliverables

[Expected outputs]

# Output Format

[Template for expected output]
```

## Best Practices

### Writing Prompts

1. **Be specific about the role** - Include years of experience, domain expertise
2. **Provide project context** - Stack, key directories, conventions
3. **Define clear deliverables** - What exactly should be produced
4. **Include output format** - Template or example of expected output
5. **Set constraints** - What the prompt should NOT do

### Managing Prompts

1. **Version prompts** - Increment version on significant changes
2. **Test before committing** - Run prompts locally first
3. **Document usage** - Include example use cases in frontmatter
4. **Keep focused** - One prompt per specific task type
5. **Review outputs** - AI outputs always require human verification

### Organizing Prompts

- Use descriptive names: `data-pipeline.md` not `debug.md`
- Group related prompts with similar prefixes
- Archive old prompts to `history/` before deletion
- Keep prompts under 1000 tokens when possible

## Configuration

### config.json

The `config.json` file contains API settings (no secrets):

```json
{
  "api": {
    "model": "claude-sonnet-4-20250514",
    "max_tokens": 4096
  },
  "project_context": {
    "name": "Maryland Housing Atlas",
    "stack": ["python", "fastapi", "postgresql"]
  }
}
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | For --exec mode | Your Anthropic API key |

## Troubleshooting

### "Command not found: make claude-run"

Ensure you're in the project root directory and Makefile has been updated.

### "Prompt not found"

Check the prompt exists in `.claude/prompts/`:
```bash
ls -la .claude/prompts/
```

### "API key not set"

Set your Anthropic API key:
```bash
export ANTHROPIC_API_KEY='your-key-here'
```

### "Permission denied" on scripts

Make scripts executable:
```bash
chmod +x scripts/claude_run.sh scripts/claude_list.sh
```

## Security Notes

- **Never commit API keys** - Use environment variables only
- **Review AI outputs** - Don't blindly apply suggested changes
- **History is gitignored** - Execution logs stay local
- **Prompts are public** - Don't include sensitive project details

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2026-01-30 | Initial release with 5 prompts |

---

**Maintained by:** Maryland Atlas Team
