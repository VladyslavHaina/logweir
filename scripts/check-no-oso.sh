#!/usr/bin/env bash
# Global Constraint 2 (spec §7.1). A name grep proves the wrong thing on its
# own — a text match on a type name says nothing about linkage — so linkage is
# proved by cargo, and the grep is narrowed to the two forms that actually
# create a link. The vendored structs, xtask and docs legitimately NAME the
# upstream types (SegmentMetadata, DryRunReport) and are excluded.
set -euo pipefail

fail=0

echo "== cargo tree: no consumer of kafka-backup-core =="
tree_err=$(cargo tree --workspace --invert kafka-backup-core 2>&1 >/dev/null) && tree_rc=0 || tree_rc=$?
if [ "$tree_rc" -eq 0 ]; then
  echo "FAIL: kafka-backup-core is present in the workspace dependency graph"
  cargo tree --workspace --invert kafka-backup-core || true
  fail=1
elif printf '%s' "$tree_err" | grep -q 'did not match any packages'; then
  echo "ok: kafka-backup-core not in the graph"
else
  echo "FAIL: cargo tree errored for a reason other than the package being absent:"
  printf '%s\n' "$tree_err"
  fail=1
fi

echo "== cargo metadata: no workspace crate declares it, under any feature/target =="
if ! meta=$(cargo metadata --format-version 1 --all-features 2>&1); then
  echo "FAIL: cargo metadata errored (the workspace does not load):"
  printf '%s\n' "$meta"
  fail=1
elif printf '%s' "$meta" | grep -q '"name":"kafka-backup-core"'; then
  echo "FAIL: kafka-backup-core appears in cargo metadata --all-features"
  fail=1
else
  echo "ok: absent from metadata"
fi

echo "== narrow linkage grep =="
# POSIX character classes, not the GNU `\s` extension: this repository is
# developed on macOS, whose BSD ERE grep silently matches nothing for `\s`
# and would report `ok: no linkage` on a genuine violation.
if grep -rnE '^[[:space:]]*(use|extern crate)[[:space:]]+kafka_backup_core' crates/ \
     --include='*.rs' \
     | grep -v '^crates/logweir-engine-oso/src/vendored/' ; then
  echo "FAIL: a source file links kafka_backup_core"
  fail=1
else
  echo "ok: no linkage"
fi

# Global Constraint 3: only three engine subcommands are reachable from shipped code.
# Subcommand *tokens* only — never the binary name and never a flag. Per global
# ruling GR8, `.engine/kafka-backup --version` (Task 13) is permitted and must
# not trip this check.
if grep -rnE '"(backup|list|restore-status|offset|evidence-verify|validation evidence-verify)"' crates/ \
     --include='*.rs' | grep -v '^crates/[^:]*:.*// harness-only' ; then
  echo "FAIL: a kafka-backup subcommand outside {restore, validate-restore, validation run} is named under crates/"
  fail=1
else
  echo "ok: no out-of-contract engine subcommand under crates/"
fi

exit "$fail"
