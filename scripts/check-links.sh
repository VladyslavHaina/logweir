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

# FX-47: a `#fragment` must name a heading or an explicit anchor of the
# Markdown file it points at (the same file for a bare `#fragment`). Headings
# become anchors by GitHub's rule: the heading's text, lower-cased, keeping
# letters, marks, numbers, `_`, `-` and spaces, each space a `-`, and a repeat
# suffixed `-1`, `-2`, … An `id="…"` or `name="…"` in an HTML tag counts too.
# Only `#` headings are read (this tree has no underlined ones), and a fragment
# into a file that is not Markdown is not checked.
if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL: python3 is needed to check link fragments" >&2
  exit 1
fi
python3 - "${files[@]}" <<'PY' || status=1
import html, os, re, sys, unicodedata
from urllib.parse import unquote

LINK = re.compile(r"\]\(([^)]*)\)")
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
ATX = re.compile(r"^ {0,3}#{1,6}(?:[ \t]+(.*?))?(?:[ \t]+#+)?[ \t]*$")
EXPLICIT = re.compile(r"<[A-Za-z][^>]*?\s(?:id|name)\s*=\s*[\"']([^\"']+)[\"']")

def text(heading):
    spans = []
    def code(m):
        spans.append(m.group(2).strip())
        return "\0%d\0" % (len(spans) - 1)
    s = re.sub(r"(`+)(.+?)\1", code, heading)
    s = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", s)
    s = re.sub(r"!?\[([^\]]*)\]\[[^\]]*\]", r"\1", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"\\(.)", r"\1", s)
    s = re.sub(r"(?<![0-9A-Za-z])_+|_+(?![0-9A-Za-z])", "", s)
    s = re.sub(r"\0(\d+)\0", lambda m: spans[int(m.group(1))], s)
    return html.unescape(s).strip()

def slug(heading):
    kept = (c for c in text(heading).lower()
            if c in " -" or unicodedata.category(c)[0] in "LMN" or unicodedata.category(c) == "Pc")
    return "".join(kept).replace(" ", "-")

anchors_of = {}
def anchors(path):
    if path not in anchors_of:
        found, seen, fence = set(), {}, None
        def add(base):
            name, n = base, seen.get(base, 0)
            while name in seen:
                n += 1
                name = "%s-%d" % (base, n)
            seen[base] = n
            seen.setdefault(name, 0)
            found.add(name)
        with open(path, encoding="utf-8") as handle:
            for line in handle.read().splitlines():
                opened = FENCE.match(line)
                if fence:
                    if opened and opened.group(1)[0] == fence[0] and len(opened.group(1)) >= len(fence):
                        fence = None
                    continue
                if opened:
                    fence = opened.group(1)
                    continue
                found.update(EXPLICIT.findall(line))
                atx = ATX.match(line)
                if atx:
                    add(slug(atx.group(1) or ""))
        anchors_of[path] = found
    return anchors_of[path]

bad = checked = 0
for source in sys.argv[1:]:
    with open(source, encoding="utf-8") as handle:
        links = LINK.findall(handle.read())
    for link in links:
        link = link.strip().split(" ")[0]
        if "#" not in link or re.match(r"[a-z]+:", link):
            continue
        target, fragment = link.split("#", 1)
        if not fragment:
            continue
        if not target:
            path = source
        else:
            path = os.path.normpath(os.path.join(os.path.dirname(source), unquote(target)))
            if not os.path.exists(path):
                path = os.path.normpath(unquote(target))
        if not path.endswith(".md") or not os.path.isfile(path):
            continue
        checked += 1
        if unquote(fragment) not in anchors(path):
            print("broken fragment in %s -> %s (no such heading or anchor in %s)" % (source, link, path), file=sys.stderr)
            bad += 1
print("checked %d fragment(s) into Markdown files" % checked)
sys.exit(1 if bad else 0)
PY
exit "$status"
