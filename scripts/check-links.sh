#!/usr/bin/env bash
# Fails if a relative Markdown link in the given paths points at a file that does
# not exist. http/https/mailto links are NOT fetched: this is a repository
# integrity check, not a network check (Global Constraint 17 — no external calls).
set -euo pipefail
cd "$(dirname "$0")/.."
status=0
while IFS= read -r -d '' f; do
  dir=$(dirname "$f")
  while IFS= read -r link; do
    case "$link" in http://*|https://*|mailto:*|"") continue ;; esac
    target="${link%%#*}"
    [ -z "$target" ] && continue
    if [ ! -e "$dir/$target" ] && [ ! -e "$target" ]; then
      echo "broken link in $f -> $link" >&2
      status=1
    fi
  done < <(grep -oE '\]\([^)#][^)]*\)' "$f" | sed -E 's/^\]\(//; s/\)$//')
done < <(find "$@" -name '*.md' -type f -print0)
exit "$status"
