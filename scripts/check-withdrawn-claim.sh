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
# COSTS NOTHING AND REACHES NOTHING: no network, no Docker, no `.engine/`, no
# cargo, no toolchain. It reads files.
set -euo pipefail

# `LOGWEIR_ROOT` exists so the tests can point this at a temp overlay; it
# defaults to the repository root, exactly like `scripts/check-one-signer.sh:58`
# and `scripts/check-pure-core.sh`.
ROOT="${LOGWEIR_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
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

grep_args=()
for phrase in "${PHRASES[@]}"; do
  grep_args+=(-e "$phrase")
done

# One `grep` invocation per surface group, its status handled on its own line:
# `grep` exits 1 for "no match", which is the GREEN case here, so a bare
# pipeline under `set -e` would abort the script on success. Nothing here reads
# an exit code through a pipe.
hits=""
collect() {
  local out
  out="$(grep -rniF "${grep_args[@]}" "$@" 2>/dev/null || true)"
  if [ -n "$out" ]; then
    hits="$hits$out"$'\n'
  fi
}

present_files=()
for f in "${FILE_SURFACES[@]}"; do
  [ -f "$f" ] && present_files+=("$f")
done
if [ "${#present_files[@]}" -gt 0 ]; then
  collect "${present_files[@]}"
fi

for d in "${DIR_SURFACES[@]}"; do
  if [ -d "$d" ]; then
    collect "$d"
  fi
done

if [ -d crates ]; then
  collect --include='*.rs' crates
fi

echo "== corpus grep: no shipped surface restates the withdrawn claim about signing =="
echo "   ${#PHRASES[@]} phrases; exempt, as literal paths: $EXEMPT_1, $EXEMPT_2"

fail=0
while IFS= read -r line; do
  [ -n "$line" ] || continue
  # `grep -rn` output is `path:line:text`; the path can contain no colon in
  # this tree, and the surfaces are all relative to $ROOT.
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
