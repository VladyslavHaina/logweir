#!/usr/bin/env bash
# THE BRACKET-ANCHORED UNVERIFIED-LABEL GATE. Joined to `just lint`.
#
# Spec §16 clause 9 — "every mark in the shipped docs is labelled in place,
# with what would verify it" — is the one clause whose subject is the SHAPE OF
# THE PROJECT'S OWN CLAIMS, and it is the one a tag is most likely to ship
# broken, because until this script nothing mechanical read a bracket.
#
# WHAT THIS PROVES, exactly and only: that every mark in the tree carries a
# description, that the description is long enough to say something, and that
# the line carrying it does not also assert the thing is true. IT DOES NOT AND
# CANNOT CHECK THAT A DESCRIPTION IS ACCURATE. "needs an MSK cluster" and
# "needs a unicorn" are the same sentence to this script. A mark whose
# description is wrong is a defect this gate is blind to; a mark with NO
# description is one it refuses.
#
# THE SUBJECT IS THE BRACKET TOKEN, NEVER THE BARE WORD (critique D H11(b)).
# A left bracket immediately followed by the word is a MARK. The bare word on
# its own is ordinary prose about unverifiedness -- `justfile`'s comment,
# `scripts/check-dod.sh`'s own "printed as UNVERIFIED rather than silently
# skipped", `phase7_verify.rs`'s doc comment, the UI's `UNVERIFIED` constant and
# the two workflow headers. Twenty-five such lines exist on the tree the day
# this lands. A gate on the bare word turns every one of them into a permanent
# false red, which is the failure mode `workflow_lint.rs` already had to be
# narrowed out of once, so they are INVENTORIED AND NEVER FAILED ON.
#
# ------------------------------------------------------------------ the rules
#
# RULE 0 -- QUOTATIONS ARE INVENTORIED, NEVER GATED. (Controller ruling; this
#   rule is not in the task brief and is recorded here because it changes what
#   the gate refuses.) An occurrence written as the BARE token wrapped in
#   backticks -- `[UNVERIFIED]` -- is a QUOTATION of a mark used as a noun in
#   prose, not a mark: "Spec §15's sixth `[UNVERIFIED]` mark", "read its
#   `[UNVERIFIED]` mark in", "the NetworkPolicy's `[UNVERIFIED]` mark". NINE
#   such quotations exist on the tree the day this lands, in
#   `docs/kubernetes.md`, `docs/install.md`, `docs/stability.md`,
#   `scripts/render-install.sh`, `e2e/compose/docker-compose.yml`,
#   `e2e/tests/guards.rs` and `e2e/tests/scram.rs`. A gate that fails every one
#   of them is a gate nobody can land. Rules 1 and 2 are not applied to a
#   quotation; it is printed under its own heading, with file and line, so it
#   is visible and counted rather than invisible.
#
#   THE RULE IS THE BARE FORM AND ONLY THE BARE FORM, and that bound is
#   load-bearing: this repository's real marks are usually written INSIDE
#   backticks too (`docs/stability.md:499`, `docs/support-matrix.md:70`,
#   `docs/kubernetes.md:1567` and three more). Exempting every backticked
#   occurrence would leave sixteen of the tree's twenty-one occurrences
#   unjudged and gate five -- a gate that lets a mark through because it sits in
#   backticks is no gate. So: backticked AND empty is a quotation; anything
#   else carrying the token is a mark and gets all four rules below.
#
# RULE 1 -- CLOSED. After the token the bracket must carry an em dash or an
#   ASCII hyphen and then a description of AT LEAST THREE whitespace-separated
#   WORDS AND AT LEAST TWELVE CHARACTERS. Both halves are stated because
#   neither alone is the rule: "not tested" is twelve characters and two words,
#   and spec §15's own mark-5 text opens with an eighteen-character three-word
#   clause that a twenty-character threshold would have rejected. A mark with
#   no description, an empty one, or a shorter one is exit 1, naming the file
#   and the line.
#
# RULE 2 -- NOT CONTRADICTED. The line must not also assert the thing is true.
#   Implemented as: DELETE the bracket token and its `[VERIFIED` sibling from
#   the line FIRST, then reject if what remains contains any of the four
#   LOWERCASE, WHOLE-WORD, CASE-SENSITIVE tokens `verified`, `proves`,
#   `confirms`, `we know`. The pre-strip and the case-sensitivity are both
#   load-bearing. Matched case-insensitively the rule fires on the token
#   itself -- the word it is built from ENDS in "verified" -- so every mark in
#   the tree goes red at once; and this repository writes `[VERIFIED …]` all
#   over its own documents. Whole-word matching is what keeps the legitimate
#   "confirmed by the first adopter run" at `docs/stability.md` from being a
#   red, and what keeps "unverified" from matching "verified".
#
# RULE 3 -- INVENTORIED. Every mark this script ACCEPTED is printed with its
#   file, its line and its description, so the count is visible on every `lint`
#   run -- the `check-deps-count.sh` practice of printing the number long
#   before it fails.
#
# RULE 4 -- MENTIONS, REPORTED NOT GATED. Occurrences of the bare word NOT
#   preceded by a left bracket are printed under their own heading with file
#   and line, and are never failures.
#
# --------------------------------------------------------------- what it walks
#
# Every file under the repository root except `target/`, `.git/` and
# `Cargo.lock`, which is the walk the task brief specifies. BINARY FILES ARE
# SKIPPED (`grep -I`): grep reports a binary hit as "Binary file … matches"
# with no line number, and a gate whose parser silently drops such a line is
# worse than one that never saw it. Untracked local artefact directories
# (`.e2e/`, `.engine/`) are NOT excluded, deliberately -- the walk is the
# shipped tree plus whatever is beside it, and neither carries the token today.
#
# NO CARGO, NO PYTHON, NO DOCKER, NO NETWORK. One filesystem walk. Its wall
# clock is recorded in the task report and is well under the 2 s budget, so
# Global Constraint 22's 120 s suite budget is untouched by it.
#
# Membership in `just lint` is what makes this a gate rather than a script:
# `.github/workflows/ci.yml` has never executed on any commit, so a workflow
# step would be documentation. `crates/logweir/tests/label_gate.rs`'s
# `just_lint_runs_the_label_gate` keeps the membership honest.
#
# `LOGWEIR_ROOT` exists so a fixture can point this at a temp tree, exactly as
# `scripts/check-one-signer.sh` does.
#
# Run: just lint  (or: bash scripts/check-unverified-labels.sh)
set -uo pipefail

ROOT="${LOGWEIR_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT" || {
  echo "check-unverified-labels.sh: cannot enter $ROOT" >&2
  exit 1
}

echo "== the label gate: every mark closed in place =="
echo "   root: $ROOT"

scan="$(mktemp "${TMPDIR:-/tmp}/logweir-labels.XXXXXX")"
trap 'rm -f "$scan"' EXIT

# ONE WALK. The exit status is read on the next line, from the command itself
# and never through a pipe (STANDING RULE 20). grep exits 1 when nothing
# matches, which is a legitimate tree state (an empty overlay), so only 2 and
# above is a walk failure.
grep -rIn --exclude-dir=target --exclude-dir=.git --exclude=Cargo.lock \
  -e 'UNVERIFIED' . >"$scan"
walk=$?
if [ "$walk" -gt 1 ]; then
  echo "FAIL: the tree walk itself failed (grep exit $walk) -- nothing was checked" >&2
  exit 1
fi

awk '
function wordhit(s, w,   re) {
  # Whole-word and case-sensitive. Word characters are letters, digits and the
  # underscore, so "unverified" does not contain the token "verified" and a
  # Rust identifier is not split in the middle.
  re = "(^|[^A-Za-z0-9_])" w "([^A-Za-z0-9_]|$)"
  return match(s, re) > 0
}
BEGIN {
  # The bracket is kept APART from the word everywhere in this program, so
  # this gate does not report its own source as a mark it then has to judge.
  LB   = "["
  WORD = "UNVERIFIED"
  RE_T = "\\" LB WORD
  RE_V = "\\" LB "VERIFIED"
  BT   = sprintf("%c", 96)     # a backtick, kept out of the shell quoting
  marks = 0; quoted = 0; mentions = 0; failed = 0
}
{
  # grep -n prints file:line:text. Filenames in this tree carry no colon.
  p1 = index($0, ":"); if (p1 == 0) next
  file = substr($0, 1, p1 - 1)
  tail = substr($0, p1 + 1)
  p2 = index(tail, ":"); if (p2 == 0) next
  lineno = substr(tail, 1, p2 - 1)
  text = substr(tail, p2 + 1)

  trimmed = text
  sub(/^[ \t]+/, "", trimmed)

  # Every occurrence on the line is classified, not just the first: one line
  # may carry two. A mark that passes rule 1 is held in `pend` rather than
  # counted straight away, because rule 2 is a property of the whole LINE and
  # is only decidable once the line has been read to its end -- a mark on a
  # contradicted line must not be reported as accepted and as a failure at the
  # same time.
  pos = 1
  has_mark = 0
  np = 0
  while (1) {
    k = index(substr(text, pos), WORD)
    if (k == 0) break
    at = pos + k - 1                      # 1-based index of the W
    pos = at + length(WORD)

    before = (at > 1) ? substr(text, at - 1, 1) : ""
    if (before != LB) {
      mentions++
      mention[mentions] = file ":" lineno "  " trimmed
      continue
    }

    # Bracketed. RULE 0: the bare form inside backticks is a quotation.
    backticked = (at > 2) && (substr(text, at - 2, 1) == BT)
    bare_close = (substr(text, at + length(WORD), 2) == "]" BT)
    if (backticked && bare_close) {
      quoted++
      quote[quoted] = file ":" lineno "  " trimmed
      continue
    }

    has_mark = 1

    # RULE 1. The description is what follows the token up to the closing
    # bracket, or to end of line when the mark wraps onto the next one.
    rest = substr(text, at + length(WORD))
    shut = index(rest, "]")
    inner = (shut > 0) ? substr(rest, 1, shut - 1) : rest

    desc = inner
    if (!sub(/^[ \t]*(—|-)[ \t]*/, "", desc)) {
      printf "FAIL %s:%s\n", file, lineno
      printf "     rule 1 (closed): the mark carries no dash and no description.\n"
      printf "     A mark with nothing behind it is a claim, not a label: write\n"
      printf "     the token, an em dash, then the sentence that would verify it.\n"
      printf "     line: %s\n", trimmed
      failed++
      continue
    }
    sub(/[ \t]+$/, "", desc)
    n = split(desc, parts, /[ \t]+/)
    if (n < 3 || length(desc) < 12) {
      printf "FAIL %s:%s\n", file, lineno
      printf "     rule 1 (closed): the description is %d word(s) and %d character(s);\n", n, length(desc)
      printf "     the threshold is at least three words AND at least twelve characters.\n"
      printf "     description: %s\n", desc
      printf "     line: %s\n", trimmed
      failed++
      continue
    }

    np++
    pend[np] = file ":" lineno "  " desc
  }

  # RULE 2, once per line, and only for a line that carries a real mark.
  hit = ""
  if (has_mark) {
    t = text
    gsub(RE_T, "", t)
    gsub(RE_V, "", t)
    if (wordhit(t, "verified"))      hit = "verified"
    else if (wordhit(t, "proves"))   hit = "proves"
    else if (wordhit(t, "confirms")) hit = "confirms"
    else if (wordhit(t, "we know"))  hit = "we know"
    if (hit != "") {
      printf "FAIL %s:%s\n", file, lineno
      printf "     rule 2 (not contradicted): the line carries a mark AND the word\n"
      printf "     \"%s\". A label and a claim on one line is one of them being false.\n", hit
      printf "     line: %s\n", trimmed
      failed++
    }
  }
  if (hit == "") {
    for (j = 1; j <= np; j++) { marks++; accepted[marks] = pend[j] }
  }
}
END {
  printf "\n-- marks accepted (rule 3: every one, with its description) --\n"
  for (i = 1; i <= marks; i++) printf "   %s\n", accepted[i]
  printf "   %d mark(s) accepted\n", marks

  printf "\n-- quoted marks (rule 0: the bare token in backticks, a quotation) --\n"
  for (i = 1; i <= quoted; i++) printf "   %s\n", quote[i]
  printf "   %d quoted mark(s); neither rule 1 nor rule 2 applies to these\n", quoted

  printf "\n-- bare-word mentions (rule 4: reported, never a failure) --\n"
  for (i = 1; i <= mentions; i++) printf "   %s\n", mention[i]
  printf "   %d mention(s)\n", mentions

  printf "\n"
  if (failed > 0) {
    printf "FAIL: %d line(s) carry a mark that is not labelled in place, or that is\n", failed
    printf "      contradicted on its own line. Spec §16 clause 9 is the requirement;\n"
    printf "      the lines are named above.\n"
    exit 1
  }
  printf "ok: %d mark(s), every one carrying a description and uncontradicted;\n", marks
  printf "    %d quoted; %d bare-word mention(s). This checks the SHAPE of a\n", quoted, mentions
  printf "    description, never its truth.\n"
  exit 0
}
' "$scan"
rc=$?
exit "$rc"
