#!/usr/bin/env bash
#
# claude_run.sh - Execute Claude prompts from .claude/prompts directory
#
# Usage:
#   ./scripts/claude_run.sh <prompt_name> [options]
#
# Options:
#   --exec       Actually call Claude API (requires ANTHROPIC_API_KEY)
#   --copy       Copy prompt to clipboard (macOS/Linux)
#   --output     Save to file instead of stdout
#   --context    Include project context files
#   --dry-run    Show what would be sent without executing
#   --help       Show this help message
#
# Examples:
#   ./scripts/claude_run.sh cleanup              # Display prompt
#   ./scripts/claude_run.sh cleanup --exec       # Execute via API
#   ./scripts/claude_run.sh documentation --copy # Copy to clipboard

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CLAUDE_DIR="$PROJECT_ROOT/.claude"
PROMPTS_DIR="$CLAUDE_DIR/prompts"
CONFIG_FILE="$CLAUDE_DIR/config.json"
HISTORY_DIR="$CLAUDE_DIR/history"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Print colored output
print_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_header() { echo -e "${CYAN}$1${NC}"; }

# Show help
show_help() {
    cat << 'EOF'
Claude Prompt Runner - Maryland Housing Atlas

USAGE:
    claude_run.sh <prompt_name> [options]

ARGUMENTS:
    prompt_name    Name of prompt file (without .md extension)

OPTIONS:
    --exec         Execute prompt via Claude API (requires ANTHROPIC_API_KEY)
    --copy         Copy prompt to clipboard
    --output FILE  Save output to specified file
    --context      Include project context (git status, directory structure)
    --dry-run      Show prompt without executing
    --list, -l     List all available prompts
    --help, -h     Show this help message

EXAMPLES:
    # Display cleanup prompt (for copy-paste to Claude web)
    ./scripts/claude_run.sh cleanup

    # Execute documentation prompt via API
    ./scripts/claude_run.sh documentation --exec

    # Copy code-review prompt to clipboard
    ./scripts/claude_run.sh code-review --copy

    # Save prompt with context to file
    ./scripts/claude_run.sh data-pipeline --context --output prompt.txt

    # List all available prompts
    ./scripts/claude_run.sh --list

ENVIRONMENT:
    ANTHROPIC_API_KEY    Required for --exec mode (get from console.anthropic.com)

PROMPT LOCATION:
    Prompts are stored in: .claude/prompts/
    Create new prompts with: make claude-new NAME=my-prompt

EOF
}

# List available prompts
list_prompts() {
    print_header "═══════════════════════════════════════════════════════════════"
    print_header "              Available Claude Prompts                          "
    print_header "═══════════════════════════════════════════════════════════════"
    echo ""

    if [[ ! -d "$PROMPTS_DIR" ]]; then
        print_error "No prompts directory found at: $PROMPTS_DIR"
        exit 1
    fi

    local count=0
    for f in "$PROMPTS_DIR"/*.md; do
        if [[ -f "$f" ]]; then
            count=$((count + 1))
            local name=$(basename "$f" .md)

            # Extract metadata from YAML frontmatter
            local prompt_name=$(grep "^name:" "$f" 2>/dev/null | sed 's/^name: //' | head -1 || echo "$name")
            local version=$(grep "^version:" "$f" 2>/dev/null | sed 's/^version: //' | head -1 || echo "1.0.0")
            local description=$(grep "^description:" "$f" 2>/dev/null | sed 's/^description: //' | head -1 || echo "No description")
            local tags=$(grep "^tags:" "$f" 2>/dev/null | sed 's/^tags: //' | head -1 || echo "[]")

            echo -e "${GREEN}$count. $name${NC} (v$version)"
            echo -e "   ${YELLOW}Description:${NC} $description"
            echo -e "   ${YELLOW}Tags:${NC} $tags"
            echo -e "   ${YELLOW}Run:${NC} make claude-run PROMPT=$name"
            echo ""
        fi
    done

    if [[ $count -eq 0 ]]; then
        print_warning "No prompts found in $PROMPTS_DIR"
        echo "Create one with: make claude-new NAME=my-prompt"
    fi

    print_header "═══════════════════════════════════════════════════════════════"
    echo "Usage: make claude-run PROMPT=<name>"
    echo "       make claude-run PROMPT=<name> EXEC=1  (for API execution)"
    print_header "═══════════════════════════════════════════════════════════════"
}

# Extract prompt content (skip YAML frontmatter)
extract_prompt() {
    local file="$1"
    # Skip everything between --- markers (YAML frontmatter)
    # Start outputting after the second ---
    awk '
        /^---$/ {
            count++
            if (count == 2) {
                getline  # Skip the closing ---
                found = 1
            }
            next
        }
        found { print }
    ' "$file"
}

# Extract metadata from frontmatter
extract_metadata() {
    local file="$1"
    local key="$2"
    grep "^$key:" "$file" 2>/dev/null | sed "s/^$key: //" | head -1
}

# Build project context
build_context() {
    cat << EOF

---

# Project Context (Auto-generated)

## Current Working Directory
\`\`\`
$PROJECT_ROOT
\`\`\`

## Directory Structure (Top 2 Levels)
\`\`\`
$(find "$PROJECT_ROOT" -maxdepth 2 -type d \
    -not -path '*/\.*' \
    -not -path '*/__pycache__*' \
    -not -path '*/node_modules*' \
    -not -path '*/.venv*' \
    -not -path '*/venv*' \
    2>/dev/null | head -40 | sed "s|$PROJECT_ROOT|.|g")
\`\`\`

## Git Status
\`\`\`
$(cd "$PROJECT_ROOT" && git status --short 2>/dev/null | head -25 || echo "Not a git repository")
\`\`\`

## Recent Commits (Last 5)
\`\`\`
$(cd "$PROJECT_ROOT" && git log --oneline -5 2>/dev/null || echo "No git history")
\`\`\`

## Key Files Modified Recently
\`\`\`
$(cd "$PROJECT_ROOT" && git diff --name-only HEAD~5 2>/dev/null | head -15 || echo "N/A")
\`\`\`

---

EOF
}

# Execute prompt via Claude API
execute_prompt() {
    local prompt="$1"
    local model="${2:-claude-sonnet-4-20250514}"

    if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
        print_error "ANTHROPIC_API_KEY environment variable not set"
        echo ""
        echo "To set it:"
        echo "  export ANTHROPIC_API_KEY='your-key-here'"
        echo ""
        echo "Get your API key from: https://console.anthropic.com/"
        exit 1
    fi

    # Check for required tools
    if ! command -v curl &> /dev/null; then
        print_error "curl is required but not installed"
        exit 1
    fi

    if ! command -v jq &> /dev/null; then
        print_warning "jq not installed - output will be raw JSON"
    fi

    print_info "Executing prompt via Claude API..."
    print_info "Model: $model"
    print_info "Estimated tokens: $(echo "$prompt" | wc -w | tr -d ' ') words"
    echo ""

    # Build JSON payload (escape the prompt properly)
    local payload
    payload=$(jq -n \
        --arg model "$model" \
        --arg prompt "$prompt" \
        '{
            model: $model,
            max_tokens: 4096,
            messages: [{
                role: "user",
                content: $prompt
            }]
        }')

    # Call API
    local response
    response=$(curl -s -X POST "https://api.anthropic.com/v1/messages" \
        -H "Content-Type: application/json" \
        -H "x-api-key: $ANTHROPIC_API_KEY" \
        -H "anthropic-version: 2023-06-01" \
        -d "$payload")

    # Extract and display response
    if command -v jq &> /dev/null; then
        local content
        content=$(echo "$response" | jq -r '.content[0].text // .error.message // "Unknown error"')
        echo "$content"
    else
        echo "$response"
    fi
}

# Copy to clipboard
copy_to_clipboard() {
    local content="$1"

    if command -v pbcopy &> /dev/null; then
        # macOS
        echo "$content" | pbcopy
        print_success "Prompt copied to clipboard (macOS pbcopy)"
    elif command -v xclip &> /dev/null; then
        # Linux with xclip
        echo "$content" | xclip -selection clipboard
        print_success "Prompt copied to clipboard (xclip)"
    elif command -v xsel &> /dev/null; then
        # Linux with xsel
        echo "$content" | xsel --clipboard --input
        print_success "Prompt copied to clipboard (xsel)"
    elif command -v clip.exe &> /dev/null; then
        # WSL
        echo "$content" | clip.exe
        print_success "Prompt copied to clipboard (WSL clip.exe)"
    else
        print_warning "No clipboard tool found (tried: pbcopy, xclip, xsel, clip.exe)"
        print_info "Install xclip: sudo apt install xclip"
        return 1
    fi
}

# Save to history
save_history() {
    local prompt_name="$1"
    local mode="$2"  # display, exec, copy
    local timestamp
    timestamp=$(date +%Y%m%d_%H%M%S)
    local history_file="$HISTORY_DIR/${prompt_name}_${timestamp}.log"

    mkdir -p "$HISTORY_DIR"

    cat << EOF > "$history_file"
# Claude Prompt Execution Log
# ===========================
# Prompt: $prompt_name
# Mode: $mode
# Timestamp: $(date -Iseconds 2>/dev/null || date)
# User: $(whoami)
# Working Directory: $(pwd)
# ===========================

EOF

    echo "$history_file"
}

# Main function
main() {
    local prompt_name=""
    local do_exec=false
    local do_copy=false
    local do_context=false
    local dry_run=false
    local output_file=""

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --exec)
                do_exec=true
                shift
                ;;
            --copy)
                do_copy=true
                shift
                ;;
            --context)
                do_context=true
                shift
                ;;
            --dry-run)
                dry_run=true
                shift
                ;;
            --output)
                if [[ -n "${2:-}" ]]; then
                    output_file="$2"
                    shift 2
                else
                    print_error "--output requires a filename"
                    exit 1
                fi
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            --list|-l)
                list_prompts
                exit 0
                ;;
            -*)
                print_error "Unknown option: $1"
                echo ""
                show_help
                exit 1
                ;;
            *)
                if [[ -z "$prompt_name" ]]; then
                    prompt_name="$1"
                else
                    print_error "Unexpected argument: $1"
                    exit 1
                fi
                shift
                ;;
        esac
    done

    # Check prompt name provided
    if [[ -z "$prompt_name" ]]; then
        print_error "No prompt name provided"
        echo ""
        echo "Usage: $(basename "$0") <prompt_name> [options]"
        echo ""
        echo "Available prompts:"
        list_prompts
        exit 1
    fi

    # Find prompt file
    local prompt_file="$PROMPTS_DIR/${prompt_name}.md"

    if [[ ! -f "$prompt_file" ]]; then
        print_error "Prompt not found: $prompt_name"
        echo ""
        echo "Looking in: $PROMPTS_DIR"
        echo ""
        echo "Available prompts:"
        list_prompts
        exit 1
    fi

    # Extract metadata
    local name version description
    name=$(extract_metadata "$prompt_file" "name")
    version=$(extract_metadata "$prompt_file" "version")
    description=$(extract_metadata "$prompt_file" "description")

    print_header "═══════════════════════════════════════════════════════════════"
    print_info "Prompt: ${name:-$prompt_name} (v${version:-1.0.0})"
    print_info "Description: ${description:-No description}"
    print_header "═══════════════════════════════════════════════════════════════"
    echo ""

    # Build full prompt
    local full_prompt
    full_prompt=$(extract_prompt "$prompt_file")

    if [[ "$do_context" == true ]]; then
        print_info "Including project context..."
        full_prompt="${full_prompt}$(build_context)"
    fi

    # Determine mode
    local mode="display"
    [[ "$do_exec" == true ]] && mode="exec"
    [[ "$do_copy" == true ]] && mode="copy"

    # Handle dry run
    if [[ "$dry_run" == true ]]; then
        print_info "Dry run - showing prompt that would be sent:"
        echo ""
        print_header "────────────────────────────────────────────────────────────────"
        echo "$full_prompt"
        print_header "────────────────────────────────────────────────────────────────"
        echo ""
        print_info "Word count: $(echo "$full_prompt" | wc -w | tr -d ' ')"
        print_info "Character count: $(echo "$full_prompt" | wc -c | tr -d ' ')"
        exit 0
    fi

    # Handle output modes
    if [[ "$do_exec" == true ]]; then
        # Execute via API
        local result
        result=$(execute_prompt "$full_prompt")

        if [[ -n "$output_file" ]]; then
            echo "$result" > "$output_file"
            print_success "Output saved to: $output_file"
        else
            echo ""
            print_header "═══════════════════════════════════════════════════════════════"
            print_header "                        API Response                            "
            print_header "═══════════════════════════════════════════════════════════════"
            echo ""
            echo "$result"
        fi

        # Save to history
        local history_file
        history_file=$(save_history "$prompt_name" "exec")
        echo "$result" >> "$history_file"
        print_info "Execution logged to: $history_file"

    elif [[ "$do_copy" == true ]]; then
        # Copy to clipboard
        if copy_to_clipboard "$full_prompt"; then
            print_info "Paste into Claude web interface or API client"
        else
            # Fallback: output to console
            echo "$full_prompt"
        fi

        # Save to history
        save_history "$prompt_name" "copy" > /dev/null

    else
        # Just output the prompt
        if [[ -n "$output_file" ]]; then
            echo "$full_prompt" > "$output_file"
            print_success "Prompt saved to: $output_file"
        else
            echo "$full_prompt"
        fi

        # Save to history
        save_history "$prompt_name" "display" > /dev/null

        echo ""
        print_header "═══════════════════════════════════════════════════════════════"
        print_info "Copy the above prompt and paste into Claude"
        print_info "Or run with --exec to execute via API"
        print_info "Or run with --copy to copy to clipboard"
        print_header "═══════════════════════════════════════════════════════════════"
    fi
}

# Run main
main "$@"
