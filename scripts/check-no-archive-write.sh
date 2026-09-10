#!/usr/bin/env bash
# G-RET — THE CAPABILITY GATE FOR THE RETENTION PATH. Task 19.
#
# WHAT THIS PROVES, exactly and only: no source file under
# `crates/weirkeeper/src/` NAMES a way to write to, or delete from, an object
# store. Not "this run did not delete anything" — that is what a behavioural
# test can observe, and a behavioural test only ever sees the calls one
# particular run happened to make. The question a guard has to answer about a
# deletion is not "did it?" but "CAN it?", so this is a capability check,
# shaped like `scripts/check-one-signer.sh`'s check 3 and run beside it in
# `just lint`.
#
# WHAT THIS DOES NOT PROVE: that the control plane cannot reach the archive at
# all. It holds a read-only handle by design — `Store::read_only_from_url`,
# whose `read_only` flag makes every put method refuse before it checks
# anything else — and it reads manifests through it to produce the retention
# report. The claim is about WRITES.
#
# WHY THE RETENTION REPORT IS A REPORT. Deleting from an archive is the one
# operation whose first defect is unrecoverable. Global Constraint 6 — "Logweir
# writes only under its own `logweir/` prefix… `PutMode::Create` everywhere" —
# stands unamended, and no Logweir component in tag 1 holds any delete
# capability against object storage. Logweir prints the `aws s3 rm` / `mc rm`
# commands; an operator runs them; the adopter's own bucket lifecycle policy
# does the deleting.
#
# ---------------------------------------------------------------------------
# THE TWO THINGS THAT MAKE THIS GREP PRECISE, AND WHY IT HAS NO EXEMPTIONS
# ---------------------------------------------------------------------------
#
# 1. COMMENT AND DOC-COMMENT LINES ARE STRIPPED FIRST — the same `//` / `///`
#    filter `check-one-signer.sh` already uses for its own source grep. This
#    design REQUIRES doc comments that talk about deleting: Task 17's
#    reconciler note has to say the orphan case "does not delete, does not
#    repair", and this task's own `docs/kubernetes.md` sentence is "no Logweir
#    component in tag 1 holds any delete capability against object storage".
#
# 2. THE DELETE TOKEN IS RECEIVER-ANCHORED, AND IS NEVER THE BARE WORD
#    `delete`. `delete` is a Kubernetes verb this controller legitimately
#    holds on Jobs and ConfigMaps (`api.delete(…)` is an ordinary, correct
#    call) and an ordinary English word besides. A gate that fires on its
#    first run against a CORRECT implementation gets an exemption added or a
#    token removed, and either outcome deletes the part of the grep that
#    catches a real object-store delete. So the token matches a `Store` /
#    `ObjectStore`-shaped receiver — `store.delete(`, `archive.delete(`,
#    `self.inner.delete(` — and nothing else.
#
# THERE IS NO PATH EXEMPTION LIST, and adding one would be the defect above
# arriving through the other door. The comment strip and the receiver anchor
# are what make the grep precise enough not to need one, and
# `crates/weirkeeper/tests/retention.rs::the_gate_has_no_path_exemptions`
# fails if an exemption array appears here or if the delete token loses its
# anchor.
#
# ---------------------------------------------------------------------------
# USAGE
# ---------------------------------------------------------------------------
#
#   scripts/check-no-archive-write.sh [ROOT]
#
# ROOT defaults to `crates/weirkeeper/src`, which is what `just lint` runs.
# The argument exists so
# `the_gate_passes_a_doc_comment_that_says_delete` can point the SAME
# implementation at a two-file fixture and observe that the comment strip and
# the receiver anchor really behave as described — one implementation, two
# entry points, the same argument `check-one-signer.sh`'s module header makes
# about `answer()`. It is not an exemption: it cannot make the default run
# skip anything.
#
# Exit 0 when no source line names a write; exit 1, naming every hit, when one
# does. STANDING RULE 20: nothing here is piped whose status is load-bearing.
set -u

ROOT="${1:-crates/weirkeeper/src}"

if [ ! -d "$ROOT" ]; then
  echo "FAIL: $ROOT is not a directory — this gate scanned nothing, and a gate that scans" >&2
  echo "  nothing passes forever. If the crate moved, move this path in the same commit." >&2
  exit 1
fi

# THE TOKENS. One per line, in this heredoc, so the test that asserts the
# delete token is receiver-anchored can read them out of this file. Each is an
# extended regular expression.
#
#   Store::from_url(    the WRITABLE constructor. The read-only one is
#                       `Store::read_only_from_url(` and is deliberately not a
#                       token — it is what this path is supposed to use.
#   put_create_only(    the one write method `Store` exposes.
#   PutMode             `object_store`'s put mode, i.e. a raw put being built.
#   \.put(  \.put_opts( `object_store`'s own put methods, reached directly.
#   …\.delete(          a delete on a Store/ObjectStore-shaped receiver.
TOKENS="$(cat <<'EOF'
Store::from_url\(
put_create_only\(
PutMode
\.put\(
\.put_opts\(
(store|archive|inner|handle|Store|ObjectStore)[A-Za-z0-9_]*\.delete\(
EOF
)"

echo "== source grep: nothing under $ROOT names an object-store write =="

fail=0
hits=""
while IFS= read -r token; do
  [ -n "$token" ] || continue
  # POSIX character classes only. BSD grep on macOS silently matches NOTHING
  # for the GNU `\s` / `\b` extensions, which would turn a genuine violation
  # into a clean run — the failure mode `scripts/check-pure-core.sh` records.
  #
  # The two `grep -v`s are the comment strip: a line whose first non-space
  # character is `//` (which covers `///` and `//!`) is prose, not code.
  found="$(grep -rnE "$token" "$ROOT" --include='*.rs' 2>/dev/null \
           | grep -v '^[^:]*:[0-9]*:[[:space:]]*//' \
           | grep -v '^[^:]*:[0-9]*:[[:space:]]*///' || true)"
  if [ -n "$found" ]; then
    fail=1
    hits="$hits
FAIL: the token /$token/ appears in code:
$found"
  fi
done <<EOF
$TOKENS
EOF

if [ "$fail" -ne 0 ]; then
  printf '%s\n' "$hits" >&2
  echo "" >&2
  echo "G-RET: the retention path holds no writable archive handle, and this crate must not" >&2
  echo "  name one. Global Constraint 6 stands unamended: Logweir writes only under its own" >&2
  echo "  \`logweir/\` prefix, and no Logweir component in tag 1 holds any object-store delete" >&2
  echo "  capability. Retention REPORTS what it would remove — it renders the \`aws s3 rm\` /" >&2
  echo "  \`mc rm\` command into BackupSchedule.status.retentionReport and runs nothing." >&2
  echo "  If a genuinely new write path is being introduced, that is a Global Constraint 6" >&2
  echo "  amendment and a spec change, not an edit to this script." >&2
  exit 1
fi

# The self-check: a gate that walked no files would report a clean tree
# forever. Reported rather than merely compared, so the number is in the log.
scanned="$(grep -rlE '.' "$ROOT" --include='*.rs' 2>/dev/null | wc -l | tr -d ' ')"
if [ "$scanned" -lt 1 ]; then
  echo "FAIL: the gate found no .rs file under $ROOT — it asserted nothing" >&2
  exit 1
fi
echo "ok: $scanned .rs file(s) under $ROOT, none naming an object-store write or delete"
exit 0
