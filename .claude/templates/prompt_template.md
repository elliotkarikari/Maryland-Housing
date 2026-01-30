---
name: [Prompt Name]
version: 1.0.0
description: [Short description of what this prompt does - max 100 chars]
author: [Your Name or Team]
created: [YYYY-MM-DD]
tags: [tag1, tag2, tag3]
estimated_tokens: [estimated input tokens, e.g., 500]
use_case: [When to use this prompt - one sentence]
---

# Role Definition

You are a [role/title] with [X] years experience in [domain], particularly [specific expertise relevant to this project].

You specialize in [specific skills] with expertise in [relevant technologies/methodologies].

# Context

**Project:** Maryland Growth & Family Viability Atlas
**Type:** Spatial analytics system for Maryland county growth analysis
**Stack:** Python 3.10+, FastAPI, PostgreSQL/PostGIS, Mapbox GL JS

**Relevant Files/Directories:**
- `[path/to/relevant/files]` - Description
- `[another/path]` - Description

[Add any additional project-specific context relevant to this prompt]

# Task

[Describe the specific task this prompt should accomplish]

1. **Step 1** - Description of first major task
2. **Step 2** - Description of second major task
3. **Step 3** - Description of third major task
4. **Step 4** - Description of fourth major task (if needed)

# Constraints

- **Constraint 1** - Explanation of why this constraint exists
- **Constraint 2** - Explanation of why this constraint exists
- **Constraint 3** - Explanation of why this constraint exists
- **Constraint 4** - Additional constraints as needed

# Deliverables

1. **Deliverable 1** - Description of what should be produced
2. **Deliverable 2** - Description of what should be produced
3. **Deliverable 3** - Description of what should be produced
4. **Deliverable 4** - Additional deliverables as needed

# Output Format

[Describe the expected output format. Provide a template if the output should follow a specific structure.]

```markdown
## [Output Section Title]

### [Subsection 1]

[Description of what goes here]

### [Subsection 2]

| Column 1 | Column 2 | Column 3 |
|----------|----------|----------|
| data | data | data |

### [Subsection 3]

```[language]
# Code example if applicable
example_code()
```

### Verification

- [ ] Checklist item 1
- [ ] Checklist item 2
- [ ] Checklist item 3
```

---

## Template Usage Instructions

1. Copy this file to `.claude/prompts/your-prompt-name.md`
2. Replace all `[bracketed placeholders]` with actual content
3. Remove this "Template Usage Instructions" section
4. Test with `make claude-run PROMPT=your-prompt-name`
5. Commit when satisfied

## Tips for Good Prompts

- **Be specific** about the role and expertise level
- **Provide context** about the project and relevant files
- **Define clear tasks** with numbered steps
- **Set constraints** to guide behavior
- **Specify output format** to get consistent results
- **Include examples** when helpful
