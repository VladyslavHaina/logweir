#!/usr/bin/env bash
# THE UI's BEHAVIOUR GATE. Interface register I24. Joined to `just lint`, after
# `check-ui-offline.sh`.
#
# WHAT THIS RUNS. `ui/tests/*.js` under node's own test runner. The page
# modules are pure functions from a JSON object to an HTML string -- no DOM, no
# network, no clock -- so every rule the pages carry is asserted by string
# containment against a checked-in fixture in the shape the API server returns.
# There is no npm, no bundler, no browser, no DOM shim and no `package.json`
# for node to read (Global Constraints 17 and 21); node is a test runner here
# and nothing else. This gate builds nothing and reaches no network
# (STANDING RULE 7).
#
# WHY THIS GATE EXISTS AT ALL. The first draft put the behavioural suite at
# `ui/tests/run.sh`: not under `scripts/`, not matching `check-*.sh`, in no
# `just` recipe and absent from `just gate`'s list -- invisible to the gate
# that exists to catch omissions. Every behavioural assertion in the UI would
# have been verified once by its own implementer and never again.
#
# AND WHY IT FAILS INSTEAD OF DEGRADING. There is no node-absent fallback and
# there must not be one. Rust cannot evaluate `renderBackupDetail(fixture)`. A
# "text check" over the spec file could assert that an assertion was WRITTEN;
# it cannot assert that it HOLDS. So on a machine without node, a degrading
# gate would report green while every page mutant survived -- which is exactly
# the failure mode STANDING RULE 21 names: a guard whose mutant passes is worse
# than no guard, because the ledger records it as closed. Node absent, node too
# old, no release binary, or a run that asserted nothing: each is an exit 1
# naming what was found.
#
# THE ARGUMENT IS A QUOTED GLOB, AND THAT IS LOAD-BEARING (plan erratum E21a).
# `node --test ui/tests/` passes a DIRECTORY, which node 22 and later execute
# as a module: on this host's node the run dies with `Cannot find module`. The
# glob is quoted so node does its own matching rather than the shell's, which
# keeps the line identical whatever directory it is invoked from.
#
# AND A GREEN RUN THAT ASSERTED NOTHING IS A FAILURE (plan erratum E21f). A
# bare `node --test` from the repository root reports `tests 0` and exits 0 --
# a gate that enumerated nothing, which is the `check-links.sh:42-45` and
# `check-ui-offline.sh` rule in a third place. This script therefore reads
# node's own summary count and refuses a zero. The TAP reporter is pinned so
# that summary line has one spelling whatever the terminal looks like.
#
# NO EXIT CODE IS READ THROUGH A PIPE (STANDING RULE 20). node's status is
# captured from the command itself and re-raised at the end; the only pipeline
# below reads TEXT out of the captured output.
#
# KNOWN SHAPE THIS GATE AND `check-ui-offline.sh` DO NOT SEE (plan erratum
# E21f, L2): a MULTI-LINE bare import --
#   import {
#     x,
#   } from "preact";
# -- evades `check-ui-offline.sh`'s `specifier_of` and `ui_lint.rs`'s, because
# both read one physical line. It is bounded rather than open: any external URL
# on any line still trips the `http:`/`https:` token scan, and there is no
# resolver in this tree for a bare specifier to reach, so such an import fails
# at load time in the browser and in node. Widening the scanner to a
# multi-line parser is not worth the parser; this note is the disclosure.
#
# The `LOGWEIR_BIN` convention is `scripts/demo-approve.sh:29-33`'s. Unlike
# that script this gate NEVER BUILDS: STANDING RULE 5 guarantees the binary.
set -euo pipefail
cd "$(dirname "$0")/.."
command -v node >/dev/null 2>&1 || {
  echo "check-ui-behaviour: node is not on PATH. The UI behaviour suite needs node >= 20.0.0 \
(STANDING RULE 5, fifth prerequisite). This gate never degrades to a text check: a text check can \
assert that an assertion was written and cannot assert that it holds." >&2; exit 1; }
v="$(node --version)"; major="${v#v}"; major="${major%%.*}"
[ "$major" -ge 20 ] 2>/dev/null || {
  echo "check-ui-behaviour: node $v is below v20.0.0; crypto.subtle is on globalThis only from \
node 19 and the suite needs 20. This gate never degrades to a text check." >&2; exit 1; }
LOGWEIR_BIN="${LOGWEIR_BIN:-target/release/logweir}"
[ -x "$LOGWEIR_BIN" ] || {
  echo "check-ui-behaviour: no release logweir binary at $LOGWEIR_BIN. STANDING RULE 5's fourth \
prerequisite: cargo build --release -p logweir." >&2; exit 1; }

set +e
out="$(LOGWEIR_BIN="$LOGWEIR_BIN" node --test --test-reporter=tap 'ui/tests/*.js' 2>&1)"
rc=$?
set -e
printf '%s\n' "$out"

if [ "$rc" -ne 0 ]; then
  echo "check-ui-behaviour: the UI behaviour suite failed (node exited $rc)." >&2
  exit "$rc"
fi

count="$(printf '%s\n' "$out" | sed -n 's/^# tests \{1,\}\([0-9][0-9]*\).*$/\1/p' | tail -1)"
if [ -z "$count" ]; then
  echo "check-ui-behaviour: node reported no test count at all, so this gate cannot tell a \
suite that passed from a suite that never ran. Expected a '# tests <n>' summary line from \
node --test --test-reporter=tap." >&2
  exit 1
fi
if [ "$count" -eq 0 ]; then
  echo "check-ui-behaviour: the suite reported 0 tests. A run that asserted nothing is not a \
pass -- it is what a moved or mis-globbed ui/tests/ looks like from inside this gate. The \
argument is the quoted glob 'ui/tests/*.js'; check that the directory and its *.spec.js files \
are where the gate expects them." >&2
  exit 1
fi

# THE PLAN GOLDEN'S THIRD ARM (Task 27, interface register I20).
#
# `ui/tests/pages.spec.js` byte-compares the emitter's output with the
# committed golden, and `crates/logweir/tests/ui_lint.rs` deserialises that
# golden into `logweir_core::spec::RestoreSpec` -- the runner's own type, which
# is the only assertion that catches an invented shape. This arm closes the
# remaining gap between them: it RE-RUNS the emitter into a temporary file and
# diffs it against the bytes in the tree, so a golden regenerated on one
# machine and a source edit committed without regenerating it cannot both look
# green.
#
# It lives here and not in `cargo test` because Global Constraint 22 keeps the
# default `cargo test` suite free of process-spawning gates, and node is not a
# `cargo test` prerequisite. Both halves of I20 are still present in `cargo
# test`: the Rust half needs no node at all.
#
# THE EXIT STATUS IS READ FROM `diff` ITSELF (STANDING RULE 20). Nothing is
# piped, and the temporary file is removed whether the diff passed or not.
golden="ui/tests/fixtures/plan.golden.yaml"
[ -f "$golden" ] || {
  echo "check-ui-behaviour: $golden is missing. It is the checked-in restore plan document \
the UI emits, the one serde_yaml deserialises into RestoreSpec in ui_lint.rs, and the \
one this gate regenerates and diffs." >&2; exit 1; }

tmp="$(mktemp -t logweir-plan-golden)"
set +e
node ui/tests/emit-plan.js > "$tmp"
emit_rc=$?
set -e
if [ "$emit_rc" -ne 0 ]; then
  rm -f "$tmp"
  echo "check-ui-behaviour: node ui/tests/emit-plan.js exited $emit_rc. The emitter is \
ui/plan.js's renderPlanBytes over ui/tests/fixtures/plan-fields.json; a throw here is a plan \
this page could not render at all." >&2
  exit "$emit_rc"
fi

set +e
diff -u "$golden" "$tmp"
diff_rc=$?
set -e
rm -f "$tmp"
if [ "$diff_rc" -ne 0 ]; then
  echo "check-ui-behaviour: the committed plan golden and the emitter disagree (diff exited \
$diff_rc). These bytes are what Restore.spec.planBytes carries and what the controller writes \
into the runner's plan ConfigMap verbatim, so a drift here is a document the runner may not \
parse. Regenerate with: node ui/tests/emit-plan.js > $golden -- and note that regenerating it \
is not the same as making it correct: ui_lint.rs deserialises the regenerated bytes into \
logweir_core::spec::RestoreSpec from Rust." >&2
  exit "$diff_rc"
fi
echo "== plan golden: $golden is byte-identical to node ui/tests/emit-plan.js =="

echo "== ui behaviour gate: $count test(s) under ui/tests/, node $v, LOGWEIR_BIN=$LOGWEIR_BIN =="
exit 0
