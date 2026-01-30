#!/usr/bin/env bash
#
# claude_list.sh - List all available Claude prompts with details
#
# Usage:
#   ./scripts/claude_list.sh
#   make claude-list
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PROMPTS_DIR="$PROJECT_ROOT/.claude/prompts"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

echo -e "${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}           Claude Prompts - Maryland Housing Atlas                  ${NC}"
echo -e "${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
echo ""

if [[ ! -d "$PROMPTS_DIR" ]]; then
    echo -e "${RED}[ERROR]${NC} No prompts directory found at: $PROMPTS_DIR"
    echo ""
    echo "Create the directory structure with:"
    echo "  mkdir -p .claude/prompts"
    exit 1
fi

# Count prompts
prompt_count=0
shopt -s nullglob
for f in "$PROMPTS_DIR"/*.md; do
    [[ -f "$f" ]] && prompt_count=$((prompt_count + 1))
done
shopt -u nullglob

if [[ $prompt_count -eq 0 ]]; then
    echo -e "${YELLOW}[WARNING]${NC} No prompts found in $PROMPTS_DIR"
    echo ""
    echo "Create a new prompt with:"
    echo "  make claude-new NAME=my-prompt"
    exit 0
fi

echo -e "${BLUE}Found $prompt_count prompt(s):${NC}"
echo ""

# List each prompt
index=0
for f in "$PROMPTS_DIR"/*.md; do
    if [[ -f "$f" ]]; then
        index=$((index + 1))
        name=$(basename "$f" .md)

        # Extract metadata from YAML frontmatter
        prompt_name=$(grep "^name:" "$f" 2>/dev/null | sed 's/^name: //' | head -1 || echo "$name")
        version=$(grep "^version:" "$f" 2>/dev/null | sed 's/^version: //' | head -1 || echo "1.0.0")
        description=$(grep "^description:" "$f" 2>/dev/null | sed 's/^description: //' | head -1 || echo "No description")
        tags=$(grep "^tags:" "$f" 2>/dev/null | sed 's/^tags: //' | head -1 || echo "[]")
        tokens=$(grep "^estimated_tokens:" "$f" 2>/dev/null | sed 's/^estimated_tokens: //' | head -1 || echo "?")
        use_case=$(grep "^use_case:" "$f" 2>/dev/null | sed 's/^use_case: //' | head -1 || echo "")

        # Display prompt info
        echo -e "${GREEN}[$index] $name${NC} ${MAGENTA}v$version${NC}"
        echo -e "    ${YELLOW}Name:${NC}        $prompt_name"
        echo -e "    ${YELLOW}Description:${NC} $description"
        echo -e "    ${YELLOW}Tags:${NC}        $tags"
        echo -e "    ${YELLOW}Est. Tokens:${NC} $tokens"
        if [[ -n "$use_case" ]]; then
            echo -e "    ${YELLOW}Use Case:${NC}    $use_case"
        fi
        echo ""
    fi
done

echo -e "${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}Quick Commands:${NC}"
echo ""
echo -e "  ${GREEN}Display prompt:${NC}    make claude-run PROMPT=<name>"
echo -e "  ${GREEN}Execute via API:${NC}   make claude-run PROMPT=<name> EXEC=1"
echo -e "  ${GREEN}With context:${NC}      make claude-run PROMPT=<name> CONTEXT=1"
echo -e "  ${GREEN}Create new:${NC}        make claude-new NAME=<name>"
echo ""
echo -e "${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
