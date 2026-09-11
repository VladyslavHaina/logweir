#!/usr/bin/env bash
# THE UI's NO-EXTERNAL-RESOURCE GATE. Joined to `just lint`.
#
# WHAT THIS PROVES, exactly and only: that no file the browser loads out of the
# UI directory names a resource outside that directory, that none of them
# carries a credential or stores one, and that every module specifier is
# relative. It proves this by reading the shipped bytes, because the shipped
# bytes ARE the sources: there is no bundler, no minifier, no source map and no
# build output that could differ from what this gate scans.
#
# WHAT THIS DOES NOT PROVE: that the page is safe to run. It is not a sandbox
# check. The page is served by `kubectl proxy`, which attaches the viewer's own
# kubeconfig credential to every request it forwards, so the page runs with the
# viewer's entire cluster authority. That residual is documented in
# `ui/README.md` and in `docs/kubernetes.md`, and it is the reason this gate
# exists at all: a page holding that much authority must not be able to pull a
# byte of code or a font from anywhere but the directory it was served from.
#
# WHY A LINT GATE AND NOT A RUNTIME CHECK. STANDING RULE 7: a lint gate never
# reaches the network. Nothing here fetches, resolves or pings anything; it
# reads files and compares bytes.
#
# THE THREE RULES.
#
#   1. No external resource of any kind. No content delivery network, no
#      remotely hosted font, no analytics, no icon pulled from elsewhere, no
#      source map, no bare module specifier. The literal byte sequences that
#      say "this line names something outside the directory" are the array
#      FORBIDDEN below -- a gate has to hold its own needles, so they appear
#      there, as data, and this prose does not repeat them. Two of them are
#      URL schemes with their colon; four are protocol-relative attribute
#      openings in both quote styles; the rest name a stylesheet import, two
#      resource hints, a source-map annotation and a remote font declaration.
#      A module specifier is checked separately: it must begin with a
#      single-dot or double-dot path segment.
#
#   2. No credential in the page. The five byte sequences in CREDENTIAL below
#      are a request header carrying authorisation, its scheme keyword, the two
#      browser-storage writes and the cookie accessor. The page holds no
#      credential of any kind and it stores nothing.
#
#   3. Enumerating nothing is not a pass. If the walk finds no file the gate
#      FAILS, naming the root -- the `scripts/check-links.sh:42-45` precedent.
#      That is also what makes a directory rename visible: a gate whose root
#      moved would otherwise go green having read nothing.
#
# SCOPE, AND WHY IT HAS EXACTLY TWO HOLES. The walk covers every file under the
# root EXCEPT `*.md` and EXCEPT the `tests/` subtree.
#   - A Markdown file is documentation, not an asset the browser loads. The
#     README has to print the serving command and the hardened kubeconfig
#     alternative, both of which contain a server address; a rule that forbade
#     the bytes there would make the document unwritable. Markdown integrity is
#     `scripts/check-links.sh`'s job.
#   - A test is data, not an asset. `ui/tests/api.spec.js` proves that the path
#     builder REFUSES an absolute URL, which it cannot do without containing
#     one. The release artefact excludes that directory, so nothing in it is
#     ever served.
# Inside that scope there is no exemption of any kind: no allowlist, no
# per-file escape, no comment that turns the rule off for a line. The moment a
# gate grows one, the thing it proves becomes "everything except what somebody
# decided not to look at".
#
# AND IT IS PLAIN ASCII, DELIBERATELY. An earlier draft of the plan wrote a
# zero-width space inside one of these tokens so a document could mention a
# scheme without tripping its own rule. That is a hole nobody can see in a
# diff. There is no invisible codepoint anywhere in this file, the prose above
# names no scheme, and `the_ui_sources_are_ascii_only` fails the build if one
# appears under the UI root.
#
# `LOGWEIR_UI_ROOT` exists so the tests can point this at a temp tree instead
# of the real one -- the `ROOT="${LOGWEIR_ROOT:-...}"` overlay convention
# `scripts/check-one-signer.sh:92` already uses. It is what makes the RED side
# of this gate testable without writing a forbidden byte into the shipped tree.
#
# No exit code is read through a pipe anywhere below (STANDING RULE 20).
set -euo pipefail
cd "$(dirname "$0")/.."

UI_ROOT="${LOGWEIR_UI_ROOT:-ui}"
# A trailing slash would make every `-path` pattern below miss.
while [ "${UI_ROOT}" != "/" ] && [ "${UI_ROOT%/}" != "${UI_ROOT}" ]; do
  UI_ROOT="${UI_ROOT%/}"
done

# Rule 1: eleven literal byte sequences. A line containing any of them names
# something outside the directory the page was served from.
FORBIDDEN=(
  'http:'
  'https:'
  'src="//'
  "src='//"
  'href="//'
  "href='//"
  '@import url('
  'rel="preconnect"'
  'rel="dns-prefetch"'
  '//# sourceMappingURL='
  '@font-face'
)

# Rule 2: five literal byte sequences. A line containing any of them puts a
# credential in the page or stores something in the browser.
CREDENTIAL=(
  'Authorization'
  'Bearer'
  'localStorage.setItem'
  'sessionStorage.setItem'
  'document.cookie'
)

if [ ! -d "$UI_ROOT" ]; then
  echo "FAIL: $UI_ROOT is not a directory, so this gate enumerated nothing -- a check that" >&2
  echo "      enumerated nothing is not a pass. If the UI directory was renamed, this script's" >&2
  echo "      root (\$LOGWEIR_UI_ROOT, default 'ui') has to move with it." >&2
  exit 1
fi

# The file list is materialised BEFORE the loop, so a `find` failure is seen
# here rather than swallowed inside a process substitution feeding a `while`.
files=()
while IFS= read -r -d '' f; do
  files+=("$f")
done < <(find "$UI_ROOT" -type f ! -name '*.md' ! -path "$UI_ROOT/tests/*" -print0)

if [ "${#files[@]}" -eq 0 ]; then
  echo "FAIL: no file under $UI_ROOT was scanned -- this gate enumerated nothing, and a check" >&2
  echo "      that enumerated nothing is not a pass." >&2
  exit 1
fi

status=0
lines_scanned=0

# Extracts the module specifier a line declares, or the empty string. Handles
# `import ... from "x"`, `export ... from "x"`, a side-effect `import "x"` and
# a dynamic `import("x")`, in either quote style.
specifier_of() {
  local raw="$1"
  local trimmed="${raw#"${raw%%[![:space:]]*}"}"
  local after=""

  case "$trimmed" in
    import*|export*)
      case "$raw" in
        *" from "*) after="${raw#*" from "}" ;;
        *"import\""*|*"import'"*) after="${trimmed#import}" ;;
      esac
      ;;
  esac
  if [ -z "$after" ]; then
    case "$raw" in
      *"import("*) after="${raw#*import(}" ;;
    esac
  fi
  if [ -z "$after" ]; then
    printf '%s' ""
    return 0
  fi

  # One quote style, so the first pair delimits the specifier.
  local normalised="${after//\'/\"}"
  case "$normalised" in
    *'"'*) : ;;
    *) printf '%s' ""; return 0 ;;
  esac
  local rest="${normalised#*\"}"
  printf '%s' "${rest%%\"*}"
}

for f in "${files[@]}"; do
  lineno=0
  while IFS= read -r line || [ -n "$line" ]; do
    lineno=$((lineno + 1))
    lines_scanned=$((lines_scanned + 1))

    for token in "${FORBIDDEN[@]}"; do
      case "$line" in
        *"$token"*)
          echo "FAIL $f:$lineno: names a resource outside the UI directory (rule 1)" >&2
          echo "     the line carries the byte sequence: $token" >&2
          status=1
          ;;
      esac
    done

    for token in "${CREDENTIAL[@]}"; do
      case "$line" in
        *"$token"*)
          echo "FAIL $f:$lineno: puts a credential in the page or stores something (rule 2)" >&2
          echo "     the line carries the byte sequence: $token" >&2
          status=1
          ;;
      esac
    done

    spec="$(specifier_of "$line")"
    if [ -n "$spec" ]; then
      case "$spec" in
        ./*|../*) : ;;
        *)
          echo "FAIL $f:$lineno: module specifier '$spec' is not relative (rule 1)" >&2
          echo "     every specifier under the UI root begins './' or '../'; a bare specifier" >&2
          echo "     needs a resolver this page does not have and must not acquire." >&2
          status=1
          ;;
      esac
    fi
  done < "$f"
done

echo "== ui offline gate: ${#files[@]} file(s), $lines_scanned line(s) under $UI_ROOT =="
echo "   ${#FORBIDDEN[@]} external-resource sequences, ${#CREDENTIAL[@]} credential sequences,"
echo "   and every module specifier required to begin './' or '../'."
echo "   Out of scope, by design: '*.md' and '$UI_ROOT/tests/'. Nothing else is exempt."
exit "$status"
