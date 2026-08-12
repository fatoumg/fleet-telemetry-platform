#!/usr/bin/env bash
set -euo pipefail
trap 'echo "HOOK CRASH: $0 line $LINENO" >&2; exit 0' ERR

# PostToolUse(Edit|Write|MultiEdit) hook: lint the edited file with ruff.
#
# Same .py guard and same exit-0 discipline as format-python.sh. Findings go to stderr so they
# surface as feedback without failing the edit -- ruff check is advisory here; pre-commit and CI
# are the gates that actually block.
#
# Runs AFTER format-python.sh, so what it reports is the formatted file, not the file as written.

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

ruff check "$FILE" >&2 || true
exit 0
