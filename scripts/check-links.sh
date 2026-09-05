#!/usr/bin/env bash
# Fails if a relative Markdown link in the given paths points at a file that does
# not exist. http/https/mailto links are NOT fetched: this is a repository
# integrity check, not a network check (Global Constraint 17 — no external calls).
#
# TWO THINGS HERE ARE FIXES, NOT STYLE.
#
# 1. THE ARGUMENT LIST IS REQUIRED, AND ENUMERATING NOTHING IS A FAILURE.
#    Run bare — `./scripts/check-links.sh`, the obvious invocation, and the one
#    `just links` does not use — `"$@"` was empty, so `find -name '*.md' …` ran
#    with no path, BSD find errored `illegal option -- n`, nothing was
#    enumerated, and the script EXITED 0. `set -euo pipefail` cannot help: the
#    failing `find` was inside a process substitution feeding a `while` loop,
#    which `set -e` does not observe. A check that reports success having
#    examined nothing is worse than no check, and it is this build's recurring
#    defect in miniature.
#
# 2. IT SAYS HOW MANY FILES IT CHECKED. A green run over zero files and a green
#    run over twenty were previously indistinguishable from the output.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "$#" -eq 0 ]; then
  cat >&2 <<'USAGE'
usage: scripts/check-links.sh <path> [path...]

At least one path is REQUIRED. This script checks the paths it is given and
nothing else; with no arguments it would have nothing to check, and reporting
success for that is exactly the failure mode it exists to prevent.

`just links` passes the repository's own list.
USAGE
  exit 2
fi

# The whole file list is materialised BEFORE the loop, so a `find` failure is
# observed by `set -e` here rather than swallowed inside a process
# substitution. `-print0` + `mapfile -d ''` keeps paths with spaces intact.
files=()
while IFS= read -r -d '' f; do files+=("$f"); done < <(find "$@" -name '*.md' -type f -print0)

if [ "${#files[@]}" -eq 0 ]; then
  echo "FAIL: no Markdown file matched $* — a link check that enumerated nothing is not a pass" >&2
  exit 1
fi

status=0
links=0
for f in "${files[@]}"; do
  dir=$(dirname "$f")
  while IFS= read -r link; do
    case "$link" in http://*|https://*|mailto:*|"") continue ;; esac
    target="${link%%#*}"
    [ -z "$target" ] && continue
    links=$((links+1))
    if [ ! -e "$dir/$target" ] && [ ! -e "$target" ]; then
      echo "broken link in $f -> $link" >&2
      status=1
    fi
  done < <(grep -oE '\]\([^)#][^)]*\)' "$f" | sed -E 's/^\]\(//; s/\)$//')
done

echo "checked ${#files[@]} Markdown file(s), $links relative link(s)"
exit "$status"
