#!/usr/bin/env bash
set -euo pipefail
trap 'echo "HOOK CRASH: $0 line $LINENO" >&2; exit 0' ERR

# PreToolUse(Bash) hook: Block dangerous commands.
# Exit codes: 0 = allow, 2 = block

INPUT=$(cat)

# Extract the command — try jq, fall back to bash regex
if command -v jq &>/dev/null; then
  CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty' 2>/dev/null)
else
  if [[ "$INPUT" =~ \"command\":\"(([^\"\\]|\\.)*)\" ]]; then
    CMD="${BASH_REMATCH[1]}"
  else
    exit 0
  fi
fi

[ -z "${CMD:-}" ] && exit 0

# --- Destructive filesystem commands ---
if echo "$CMD" | grep -qE 'rm\s+(-[a-zA-Z]*f|-[a-zA-Z]*r){2}.*(/|\*|~)'; then
  echo "BLOCKED: Destructive rm -rf on root/home/glob path." >&2
  exit 2
fi

if echo "$CMD" | grep -qE '^\s*rm\s+-rf\s+/\s*$'; then
  echo "BLOCKED: rm -rf / is never allowed." >&2
  exit 2
fi

# --- Force push / hard reset ---
if echo "$CMD" | grep -qiE 'git\s+push\s+.*--force|git\s+push\s+-f\b'; then
  echo "BLOCKED: Force push. Use --force-with-lease or get user approval." >&2
  exit 2
fi

if echo "$CMD" | grep -qiE 'git\s+reset\s+--hard'; then
  echo "BLOCKED: git reset --hard discards work. Get user approval first." >&2
  exit 2
fi

# --- Production database ---
if echo "$CMD" | grep -qiE 'DROP\s+(DATABASE|TABLE|SCHEMA)\s'; then
  echo "BLOCKED: DROP DATABASE/TABLE/SCHEMA requires user approval." >&2
  exit 2
fi

if echo "$CMD" | grep -qiE '(production|prod)\s*.*database|DATABASE_URL=.*prod'; then
  echo "BLOCKED: Refusing to run commands against production database." >&2
  exit 2
fi

# --- Docker destructive ---
if echo "$CMD" | grep -qiE 'docker\s+(system\s+prune|rm\s+-f|rmi\s+-f)'; then
  echo "BLOCKED: Docker destructive operation requires user approval." >&2
  exit 2
fi

# --- Pipe to shell ---
if echo "$CMD" | grep -qE 'curl\s.*\|\s*(ba)?sh'; then
  echo "BLOCKED: Piping curl to shell is dangerous. Download and review first." >&2
  exit 2
fi

exit 0
