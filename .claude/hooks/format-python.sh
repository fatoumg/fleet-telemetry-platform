#!/usr/bin/env bash
set -euo pipefail
trap 'echo "HOOK CRASH: $0 line $LINENO" >&2; exit 0' ERR

# PostToolUse(Edit|Write|MultiEdit) hook: format the edited file with ruff.
#
# Guarded on the .py extension on purpose. This repo edits far more .md, .sql and .yml than
# Python, and ruff exits non-zero on a file it cannot parse -- an unguarded hook would report a
# failure after every documentation edit and train you to ignore hook output.
#
# Always exits 0: a formatter that blocks the edit it just formatted is worse than no formatter.

INPUT=$(cat)

if command -v jq &>/dev/null; then
  FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty' 2>/dev/null)
else
  if [[ "$INPUT" =~ \"file_path\":\"(([^\"\\]|\\.)*)\" ]]; then
    FILE="${BASH_REMATCH[1]}"
  else
    exit 0
  fi
fi

[ -z "${FILE:-}" ] && exit 0
[[ "$FILE" == *.py ]] || exit 0
[ -f "$FILE" ] || exit 0

command -v ruff &>/dev/null || exit 0

ruff format "$FILE" || true
exit 0
