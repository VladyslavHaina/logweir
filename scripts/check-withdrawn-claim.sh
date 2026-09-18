#!/usr/bin/env bash
# G-SIGN, SECOND HALF — THE CORPUS GREP.
#
# WHAT THIS PROVES: that no shipped surface in this repository restates the
# stronger claim about signing that was made once, was RED on the tree it was
# asserted about, and was WITHDRAWN. `scripts/check-one-signer.sh:10-19`
# forbids restating it, in those words; this script is what makes that
# instruction enforceable instead of advisory, on every surface rather than in
# one file's comments.
#
# WHY A GREP AND NOT A REVIEW. The defect class here is not a bug in code, it
# is a documented guarantee the code does not deliver — and the only reason
# anyone knows it is a defect class is that it already happened once. A prose
# claim has no test, no type and no compiler: a sentence added to a README in a
# hurry, or to a UI string, or to a CI step's `name:`, ships as a promise.
# Global Constraint 27 states the true, narrowed position — no control-plane
# CRATE LINKS the signer, and the CAPABILITY is unbroken while a component
# holds Job CRUD over the signing key's namespace (residual O1, accepted, O0
# default (a)) — and nothing on any surface may say otherwise.
#
# THE PHRASE LIST IS FIXED AND CASE-INSENSITIVE, AND THAT IS A DELIBERATE
# LIMIT. It catches the sentence and its near paraphrases, not every possible
# rewording; a grep that tried to catch every rewording would be a grep that
# fires on the honest, narrowed statements this repository is REQUIRED to make.
# A new paraphrase found in review is added to the list here, in the same
# commit as the surface it was found on.
#
# THE SECOND CLAIM THIS GATE NOW CARRIES (D3 W9, review `d3w9` H3). The
# retention worker's enforcement record and its per-point tombstones are
# **create-only and unsigned in this build**, and `docs/stability.md` says so in
# those terms together with the reason (signing would make
# `logweir-retention` link `crates/logweir-evidence`, whose reaching set
# `scripts/check-one-signer.sh` holds to `{logweir, e2e}` — a decision about the
# SIGNER, taken in two files a deletion feature does not own). That passage also
# writes the rule this gate now enforces: *no surface may describe the record as
# signed until one of the two named remedies lands*.
#
# It is the same defect class and the same remedy: a documented guarantee the
# code does not deliver, caught by grep because prose has no compiler. Four
# shipped surfaces carried it on the branch that introduced the worker — the
# CRD's own `doc` string, served by `kubectl explain` and shipped in
# `config/crd/`, the chart's CRD copies, all seven rendered chart outputs and
# `logweir.yaml`; two paragraphs of `docs/kubernetes.md`; and a
# `RetentionPolicy` status condition message. The gate was green, because its
# phrase list did not carry the wording.
#
# THE MATCH IS WRAP-INSENSITIVE, AND THAT WAS A REAL HOLE (review `d3w9` R2).
# This was `grep -rniF`, which is LINE-BASED, and this corpus hard-wraps prose
# at a median of 76 columns — so a phrase that happened to break across two
# lines was invisible. It was not a gap in one phrase: EVERY phrase below was
# escapable the same way, including the original withdrawn signing claim this
# gate was built for, and escaping it took nothing more deliberate than a
# paragraph reflow. One survived here for a whole review round.
#
# So the match now runs over a whitespace-normalised join of each file, with a
# leading comment marker stripped from every continuation line so a Rust doc
# comment and a shell comment wrap the same way a paragraph does. The walk is
# `scripts/withdrawn-claim-scan.py`, in a FILE and not a heredoc, for the reason
# `scripts/check-no-archive-write.sh` records: a heredoc body containing
# backticks inside a command substitution is mis-parsed by bash, and a gate that
# reports success having run nothing is this repository's signature defect. It
# prints `path:lineno:text`, byte for byte what `grep -rn` printed, so the
# reporting loop below is unchanged.
#
# COSTS NOTHING AND REACHES NOTHING: no network, no Docker, no `.engine/`, no
# cargo, no toolchain. It reads files, with python3.
set -euo pipefail

# `LOGWEIR_ROOT` exists so the tests can point this at a temp overlay; it
# defaults to the repository root, exactly like `scripts/check-one-signer.sh:58`
# and `scripts/check-pure-core.sh`.
ROOT="${LOGWEIR_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
# THE SCAN COMES FROM WHERE THIS SCRIPT LIVES, NOT FROM $ROOT. `LOGWEIR_ROOT`
# points this gate at a fixture corpus, and a fixture corpus is a thing to be
# SCANNED, not a place to find the scanner: resolving the tool there would make
# every overlay test either carry a copy of it or silently exercise nothing.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

# The withdrawn claim and its paraphrases. Fixed strings (`grep -F`), matched
# case-insensitively.
PHRASES=(
  "provably cannot sign"
  "cannot sign, provably"
  "the control plane cannot sign"
  "control plane provably cannot"
  "no component of the control plane can sign"
  "weirkeeper cannot sign"
  # The retention record (review `d3w9` H3). Each of these is a wording that
  # was actually on a shipped surface, not a hypothetical: the honest
  # replacement is "create-only and unsigned in this build".
  "only with a signed record"
  "attributable signed record"
  "signed-key intent tombstone"
  "signed-key record"
  "signed retention record"
  "signed tombstone"
  "the record is signed"
  "signed enforcement record"
)

# EXACTLY TWO EXEMPT PATHS, AS LITERALS — NEVER A PATTERN. These two files
# exist to FORBID the claim and therefore have to quote it. A pattern
# (`scripts/*`, `*check*`) would exempt every future file that happened to
# match, which is how a grep stops meaning anything; the comparison below is
# string equality and nothing else. Two separate variables rather than a list,
# so "exactly two" is mechanically readable:
# `crates/logweir/tests/withdrawn_claim.rs::the_exemption_list_is_exactly_two_paths`
# asserts there is no third.
EXEMPT_1="scripts/check-one-signer.sh"
EXEMPT_2="scripts/check-withdrawn-claim.sh"

is_exempt() {
  [ "$1" = "$EXEMPT_1" ] || [ "$1" = "$EXEMPT_2" ]
}

# EVERY SHIPPED SURFACE. The five root documents, the documentation, the
# shipped configuration and examples, the UI, the CI workflows, the guard
# scripts, and every `*.rs` under `crates/`.
#
# `scripts/` IS WALKED, and that is why the exemption above exists at all: the
# two gate scripts are the only files in this tree that legitimately quote the
# withdrawn claim, and a scan that skipped their directory would make the
# exemption decorative. `scripts/check-one-signer.sh:16` binds "this script's
# output, its comments, or the CI step that runs it" — so the CI step is
# walked too, under `.github/workflows/`.
#
# Absent roots are SKIPPED rather than erroring: `config/` and `ui/` do not
# exist in this tree yet, and a gate that refuses to run is a gate that does
# not run.
FILE_SURFACES=(
  README.md
  SECURITY.md
  CONTRIBUTING.md
  MAINTAINERS.md
  TRADEMARKS.md
)
DIR_SURFACES=(
  docs
  config
  examples
  ui
  .github/workflows
  scripts
)

# The scan needs python3. REFUSE, naming it, rather than falling through: a
# guard that skips its only check and still exits 0 is a check that cannot fail.
SCAN="$SCRIPT_DIR/withdrawn-claim-scan.py"
if ! command -v python3 >/dev/null 2>&1; then
  echo "check-withdrawn-claim: REFUSING to run — \`python3\` is not on PATH." >&2
  echo "  The corpus scan is wrap-insensitive and is written in python3; a" >&2
  echo "  line-based grep fall-back would silently reinstate the hole this" >&2
  echo "  gate was fixed to close (review d3w9 R2). Install it and re-run." >&2
  exit 1
fi
if [ ! -f "$SCAN" ]; then
  echo "check-withdrawn-claim: REFUSING to run — $SCAN is missing, so this" >&2
  echo "  gate scanned nothing, and a gate that scans nothing passes forever." >&2
  exit 1
fi

# The phrase list, handed to the scan as a file: one phrase per line, so a
# phrase containing a space, a backtick or a slash needs no quoting dance.
phrase_file="$(mktemp)"
trap 'rm -f "$phrase_file"' EXIT
for phrase in "${PHRASES[@]}"; do
  printf '%s\n' "$phrase" >> "$phrase_file"
done

# Every file of every surface, enumerated here rather than by `grep -r` so the
# scan sees exactly what the greps saw. `find`'s status is read on its own line;
# nothing here reads an exit code through a pipe.
targets=()
for f in "${FILE_SURFACES[@]}"; do
  [ -f "$f" ] && targets+=("$f")
done
for d in "${DIR_SURFACES[@]}"; do
  [ -d "$d" ] || continue
  while IFS= read -r found; do
    [ -n "$found" ] && targets+=("$found")
  done < <(find "$d" -type f 2>/dev/null | LC_ALL=C sort)
done
if [ -d crates ]; then
  while IFS= read -r found; do
    [ -n "$found" ] && targets+=("$found")
  done < <(find crates -type f -name '*.rs' 2>/dev/null | LC_ALL=C sort)
fi

if [ "${#targets[@]}" -eq 0 ]; then
  echo "FAIL: no surface file was found at all; this gate asserted nothing" >&2
  exit 1
fi

hits_file="$(mktemp)"
trap 'rm -f "$phrase_file" "$hits_file"' EXIT
python3 "$SCAN" "$phrase_file" "${targets[@]}" > "$hits_file"
scan_status=$?
if [ "$scan_status" -ne 0 ]; then
  echo "FAIL: the corpus scan exited $scan_status; it asserted nothing" >&2
  exit 1
fi
hits="$(cat "$hits_file")"

echo "== corpus scan: no shipped surface restates the withdrawn claim about signing =="
echo "   ${#PHRASES[@]} phrases over ${#targets[@]} file(s), matched ACROSS LINE WRAPS;"
echo "   exempt, as literal paths: $EXEMPT_1, $EXEMPT_2"

fail=0
while IFS= read -r line; do
  [ -n "$line" ] || continue
  # The scan's output is `path:line:text` — the shape `grep -rn` printed; the
  # path can contain no colon in this tree, and the surfaces are all relative
  # to $ROOT.
  path="${line%%:*}"
  rest="${line#*:}"
  lineno="${rest%%:*}"
  text="${rest#*:}"
  if is_exempt "$path"; then
    continue
  fi
  echo "FAIL: $path:$lineno restates the withdrawn claim about signing:" >&2
  echo "  $text" >&2
  fail=1
done <<< "$hits"

if [ "$fail" -eq 0 ]; then
  echo "ok: no shipped surface restates it"
else
  echo "" >&2
  echo "  The stronger claim was made once, was RED on the tree it was asserted" >&2
  echo "  about, and was withdrawn; scripts/check-one-signer.sh:10-19 forbids" >&2
  echo "  restating it. What IS true is Global Constraint 27's narrowed claim:" >&2
  echo "  no control-plane CRATE LINKS the signer, and the CAPABILITY to sign is" >&2
  echo "  unbroken while a component holds Job CRUD over the signing key's" >&2
  echo "  namespace. Say that instead. Only two paths are exempt, by literal" >&2
  echo "  path, because they exist to forbid the claim: $EXEMPT_1 and" >&2
  echo "  $EXEMPT_2." >&2
fi

exit "$fail"
