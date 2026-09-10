#!/usr/bin/env bash
# G-RET — THE CAPABILITY GATE FOR THE RETENTION PATH. Task 19.
#
# WHAT THIS PROVES, exactly and only: no source file under
# `crates/weirkeeper/src/` NAMES a way to write to an object store or to delete
# from one on a `Store`-shaped receiver, AND no source file under
# `crates/logweir-store/src/` names an object-store delete at all or defines a
# `delete` method. Not "this run did not delete anything" — that is what a
# behavioural test can observe, and a behavioural test only ever sees the calls
# one particular run happened to make. The question a guard has to answer about
# a deletion is not "did it?" but "CAN it?", so this is a capability check,
# shaped like `scripts/check-one-signer.sh`'s check 3 and run beside it in
# `just lint`.
#
# WHY TWO ROOTS, AND WHY THEIR TOKEN LISTS DIFFER (Task 19 review, finding
# G-RET (c)). The first version of this gate scanned `crates/weirkeeper/src`
# alone, and the reviewer showed by planting that a `.delete(` added to
# `crates/logweir-store/src/lib.rs` left it at rc 0 — i.e. the gate did not
# watch the ONE crate where an object-store delete would actually be written,
# because that is the crate that holds the handle. So:
#
#   * `crates/weirkeeper/src` — the control plane. Kubernetes deletes are
#     LEGITIMATE here (`api.delete(…)` on a Job or a ConfigMap), so the delete
#     token is receiver-anchored, and the writable constructor and the put
#     family are forbidden outright.
#   * `crates/logweir-store/src` — the store crate. It contains no Kubernetes
#     client at all, so ANY `.delete(`, any `delete_objects(` /
#     `delete_stream(`, and any `fn delete…` DEFINITION is a violation and
#     needs no receiver anchor. The put family is NOT forbidden here: this is
#     the crate that implements `put_create_only`, the one write Global
#     Constraint 6 allows.
#
# WHAT THIS DOES NOT PROVE: that the control plane cannot reach the archive at
# all. It holds a read-only handle by design — `Store::read_only_from_url`,
# whose `read_only` flag makes every put method refuse before it checks
# anything else — and it reads manifests through it to produce the retention
# report. The claim is about WRITES.
#
# AND IT IS A TRIPWIRE ON SPELLINGS, NOT A PROOF. It reads source text, so a
# delete issued as a raw HTTP DELETE through some other crate would pass it.
# What makes that unwritable is not this script: `Store` exposes no delete
# method, its `inner: Arc<dyn ObjectStore>` is private so `ObjectStore::delete`
# is unreachable from any other crate, and `crates/weirkeeper/Cargo.toml`
# declares no `object_store`, `reqwest` or `hyper` edge — which Global
# Constraint 38 and `scripts/check-deps-count.sh` are what govern. This gate is
# the cheap, fast half that catches the idiomatic spelling on the way in.
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
# THE THREE THINGS THAT MAKE THIS GREP PRECISE, AND WHY IT HAS NO EXEMPTIONS
# ---------------------------------------------------------------------------
#
# 1. COMMENT AND DOC-COMMENT LINES ARE STRIPPED FIRST — the same `//` / `///`
#    filter `check-one-signer.sh` already uses for its own source grep. This
#    design REQUIRES doc comments that talk about deleting: Task 17's
#    reconciler note has to say the orphan case "does not delete, does not
#    repair", and this task's own `docs/kubernetes.md` sentence is "no Logweir
#    component in tag 1 holds any delete capability against object storage".
#
# 2. METHOD CHAINS ARE JOINED BEFORE THE GREP RUNS. `cargo fmt` routinely
#    breaks a chain so the receiver and the method land on different lines:
#
#        store
#            .delete("k")
#
#    A receiver-anchored regex needs both on ONE line, so the reviewer's plant
#    of exactly that shape passed the first version of this gate. The awk pass
#    below therefore emits one LOGICAL line per record: a comment-stripped
#    source line with every immediately following continuation line (first
#    non-space character `.`) appended to it, reported at the receiver's own
#    line number. This is the only reason the `.delete(` anchor is safe to
#    keep.
#
# 3. IN THE CONTROL PLANE THE DELETE TOKEN IS RECEIVER-ANCHORED, AND IS NEVER
#    THE BARE WORD `delete`. `delete` is a Kubernetes verb this controller
#    legitimately holds on Jobs and ConfigMaps (`api.delete(…)` is an
#    ordinary, correct call) and an ordinary English word besides. A gate that
#    fires on its first run against a CORRECT implementation gets an exemption
#    added or a token removed, and either outcome deletes the part of the grep
#    that catches a real object-store delete. So in `weirkeeper` the token
#    matches an identifier CONTAINING a store-shaped word — `store.delete(`,
#    `my_store.delete(`, `archive.delete(`, `self.inner.delete(`,
#    `obj.delete(`, `object_store.delete(` — or a receiver that is exactly the
#    abbreviation `s` or `st` (`s.delete(`, `st.delete(`, all four of which the
#    reviewer planted), plus `object_store`'s own bulk methods
#    `delete_objects(` and `delete_stream(`, whose names no Kubernetes client
#    has. In the STORE crate there is no Kubernetes client to protect, so the
#    anchor is dropped and a bare `.delete(` — or a `fn delete…` definition —
#    is a violation on its own.
#
# THERE IS NO PATH EXEMPTION LIST, and adding one would be the defect above
# arriving through the other door. The comment strip, the chain join and the
# receiver anchor are what make the grep precise enough not to need one, and
# `crates/weirkeeper/tests/retention.rs::the_gate_has_no_path_exemptions`
# fails if an exemption array appears here, if the control plane's delete token
# loses its anchor, or if the store crate's list loses its unanchored one.
#
# ---------------------------------------------------------------------------
# USAGE
# ---------------------------------------------------------------------------
#
#   scripts/check-no-archive-write.sh [CONTROL_PLANE_ROOT [STORE_ROOT]]
#
# The roots default to `crates/weirkeeper/src` and `crates/logweir-store/src`,
# which is what `just lint` runs. The arguments exist so
# `the_gate_passes_a_doc_comment_that_says_delete` can point the SAME
# implementation at a two-file fixture and observe that the comment strip, the
# chain join and the receiver anchor really behave as described — one
# implementation, two entry points, the same argument
# `check-one-signer.sh`'s module header makes about `answer()`. They are not
# exemptions: they cannot make the default run skip anything, and
# `the_gate_scans_the_store_crate_by_default` plants a `.delete(` in the real
# `crates/logweir-store/src` and asserts the DEFAULT, argument-free run exits 1.
#
# Exit 0 when no source line names a write; exit 1, naming every hit, when one
# does. STANDING RULE 20: nothing here is piped whose status is load-bearing.
set -u

ROOT_CONTROL_PLANE="${1:-crates/weirkeeper/src}"
ROOT_STORE="${2:-crates/logweir-store/src}"

# THE TOKENS. One per line, in these heredocs, so the test that asserts the
# control plane's delete token is receiver-anchored — and that the store
# crate's is deliberately not — can read them out of this file. Each is an
# extended regular expression, matched against the JOINED logical lines.
#
# The control plane: the write surface, plus a receiver-anchored delete.
#
#   Store::from_url(    the WRITABLE constructor. The read-only one is
#                       `Store::read_only_from_url(` and is deliberately not a
#                       token — it is what this path is supposed to use.
#   put_create_only(    the one write method `Store` exposes.
#   PutMode             `object_store`'s put mode, i.e. a raw put being built.
#   \.put(  \.put_opts( `object_store`'s own put methods, reached directly.
#   …\.delete(          a delete on a store-shaped receiver: an identifier
#                       CONTAINING store/archive/inner/handle/blob/bucket/obj,
#                       or one that IS the abbreviation `s` or `st`.
#   delete_objects(     `object_store`'s bulk delete, by name — no Kubernetes
#   delete_stream(      client has either, so neither needs an anchor.
TOKENS_CONTROL_PLANE="$(cat <<'EOF'
Store::from_url\(
put_create_only\(
PutMode
\.put\(
\.put_opts\(
[A-Za-z0-9_]*(store|Store|archive|inner|handle|blob|bucket|obj|ObjectStore)[A-Za-z0-9_]*\.delete\(
(^|[^A-Za-z0-9_])(s|st)\.delete\(
delete_objects\(
delete_stream\(
EOF
)"

# The store crate: deletes only, and UNANCHORED on purpose — this crate holds
# no Kubernetes client, so there is no legitimate `.delete(` in it, and it is
# the crate where a delete method would be ADDED. `fn[[:space:]]+delete`
# catches the DEFINITION, which is the shape the review's plant took.
#
# The put family is absent here deliberately: `put_create_only` is DEFINED in
# this crate, and it is the one write Global Constraint 6 allows.
TOKENS_STORE="$(cat <<'EOF'
\.delete\(
delete_objects\(
delete_stream\(
fn[[:space:]]+delete
EOF
)"

fail=0
hits=""

# One "logical line" per record — `<file>:<lineno>:<text>` — with comment lines
# blanked and method-chain continuations joined onto the receiver's line. See
# note 2 in the header: without this, `cargo fmt` splitting a chain hides a
# receiver-anchored delete from the grep.
logical_lines() {
  local root="$1" f
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    awk -v file="$f" '
      function flush() { if (have) { printf "%s:%d:%s\n", file, n, t; have = 0 } }
      {
        s = $0
        sub(/^[ \t]+/, "", s)
        # A line whose first non-space character is `//` (which covers `///`
        # and `//!`) is prose, not code.
        if (s ~ /^\/\//) { s = "" }
        # A continuation of the chain above: append with NO separator, because
        # rustfmt puts the `.method(` at the start of the line and the receiver
        # at the end of the previous one.
        if (have && s ~ /^\./) { t = t s; next }
        flush()
        t = s; n = FNR; have = 1
      }
      END { flush() }
    ' "$f"
  done < <(find "$root" -name '*.rs' -type f 2>/dev/null | LC_ALL=C sort)
}

# Grep one root's joined lines for one root's token list.
scan() {
  local root="$1" tokens="$2" what="$3"
  local lines token found files

  if [ ! -d "$root" ]; then
    echo "FAIL: $root is not a directory — this gate scanned nothing there, and a gate that" >&2
    echo "  scans nothing passes forever. If the crate moved, move this path in the same commit." >&2
    fail=1
    return
  fi

  echo "== source grep: nothing under $root names $what =="
  lines="$(logical_lines "$root")"

  # The self-check: a gate that walked no files would report a clean tree
  # forever, and the joining pass above is one more place that could silently
  # produce nothing. Counted on the JOINED output, not on the raw files, so a
  # broken preprocessor is caught too. Reported rather than merely compared, so
  # the number is in the log.
  files="$(printf '%s\n' "$lines" | grep -c '^..*:[0-9][0-9]*:' || true)"
  if [ -z "$lines" ] || [ "$files" -lt 1 ]; then
    echo "FAIL: the gate produced no source line under $root — it asserted nothing" >&2
    fail=1
    return
  fi

  while IFS= read -r token; do
    [ -n "$token" ] || continue
    # POSIX character classes only. BSD grep on macOS silently matches NOTHING
    # for the GNU `\s` / `\b` extensions, which would turn a genuine violation
    # into a clean run — the failure mode `scripts/check-pure-core.sh` records.
    found="$(printf '%s\n' "$lines" | grep -E "^[^:]*:[0-9]*:.*${token}" || true)"
    if [ -n "$found" ]; then
      fail=1
      hits="$hits
FAIL: the token /$token/ appears in code under $root:
$found"
    fi
  done <<EOF
$tokens
EOF

  echo "ok: $files source line(s) under $root, none naming $what"
}

scan "$ROOT_CONTROL_PLANE" "$TOKENS_CONTROL_PLANE" \
  "an object-store write, or a delete on a store-shaped receiver"
scan "$ROOT_STORE" "$TOKENS_STORE" \
  "an object-store delete, or a delete method definition"

if [ "$fail" -ne 0 ]; then
  printf '%s\n' "$hits" >&2
  echo "" >&2
  echo "G-RET: the retention path holds no writable archive handle, and neither the control" >&2
  echo "  plane nor the store crate may name one. Global Constraint 6 stands unamended:" >&2
  echo "  Logweir writes only under its own \`logweir/\` prefix, and no Logweir component in" >&2
  echo "  tag 1 holds any object-store delete capability. Retention REPORTS what it would" >&2
  echo "  remove — it renders the \`aws s3 rm\` / \`mc rm\` command into" >&2
  echo "  BackupSchedule.status.retentionReport and runs nothing." >&2
  echo "  If a genuinely new write path is being introduced, that is a Global Constraint 6" >&2
  echo "  amendment and a spec change, not an edit to this script." >&2
  exit 1
fi

exit 0
