#!/usr/bin/env bash
# `just mvp-demo` — ONE command from a source topic to a verified
# point-in-time restore into a NEW topic and a signed receipt. Task 12; Phase
# A's exit criterion.
#
# WHAT THIS IS, AND HOW IT DIFFERS FROM `scripts/demo.sh`
#
#   `scripts/demo.sh` is the v0.1 drill demo. It takes its archive FROM THE
#   HARNESS — `LOGWEIR_SEED_REFRESH_FIXTURES=0 ./scripts/e2e-seed.sh`, whose
#   backup step is `kafka-backup backup --config /config/backup-drill.yaml`,
#   the pinned engine invoked directly — and it restores into a SCRATCH
#   cluster behind a marker topic, at no point in time. Everything it proves
#   is true and none of it is the product taking a backup.
#
#   This script drives the PRODUCT end to end:
#
#       produce -> logweir backup run -> receipt verified by BOTH readers
#               -> logweir drill approve -> logweir restore run
#                  (target.mode: newTopic, restore.point_in_time)
#               -> scorecard verified by BOTH readers
#
#   and prints one summary line naming the new topic, its record count read
#   off the broker, the measured RTO and RPO, and the two evidence keys.
#
# EXIT CODES ARE READ DIRECTLY, NEVER THROUGH A PIPE (STANDING RULE 20).
#
#   `cmd | grep` reports GREP's status, so a pipe on a line whose exit code is
#   load-bearing silently throws that code away. Every such command here is
#   written in one shape:
#
#       set +e
#       logweir … > "$OUT/something.stdout"
#       rc=$?
#       set -e
#       [ "$rc" -eq 0 ] || die "…"
#
#   — the command on its own line (redirecting to a FILE rather than piping to
#   a reader), its status read on the very next line, and `set -e` relaxed
#   across exactly those two lines so a failure prints `rc=<n>` and a sentence
#   instead of vanishing. `crates/logweir/tests/mvp_demo_lint.rs::mvp_demo_masks_no_exit_code`
#   runs the I29 tokeniser (`crates/logweir/tests/support/exit_code_lint.rs`)
#   over this file in the DEFAULT test suite and fails naming any line that
#   breaks the shape.
#
#   That tokeniser's guard is a LITERAL first-word rule — `kubectl`, `curl`,
#   `logweir`, `docker`, `just` — so this script puts the binary's directory on
#   `$PATH` and writes the bare word `logweir`. A `"$LOGWEIR_BIN" …` spelling
#   would be invisible to the lint, which is the opposite of what a
#   demonstration script wants.
#
# THE BINARY IT RUNS: `target/debug/logweir`, via `$PATH`.
#
#   Plan erratum E9: the e2e harness runs the DEBUG binary, so the row in
#   `e2e/tests/mvp_demo.rs` that shells this script and the rest of the e2e
#   suite must be testing the same bytes. `just mvp-demo` runs
#   `cargo build -p logweir` first, exactly as `just pitr` does. Set
#   `LOGWEIR_BIN=/path/to/logweir` to run a different build.
#
# WHAT IT WRITES: `.demo/mvp/` only, which is gitignored (`.demo/`).
#
# Zero cloud spend (Global Constraint 17): a local `docker compose` stack and
# nothing else.
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=.demo/mvp
COMPOSE_FILE=e2e/compose/docker-compose.yml
BOOTSTRAP_INNET=kafka-broker-1:9094
ARCHIVE_BUCKET=kafka-backups

# The demo's archive lives under its OWN backup_id, which is what
# `examples/backup.yaml` already carries, and is SWEPT AT BOTH ENDS. Two
# reasons, and the second is the load-bearing one:
#
#   1. a second `backup` into a colliding backup_id does not accumulate
#      (`scripts/e2e-seed.sh:81-91` measured it: a re-run left the manifest
#      describing 2048 records while the broker held 6000);
#   2. `harness::corrupt_a_non_oldest_segment` picks its victim by listing the
#      archive bucket and taking the LAST key in sort order, and `mvp-demo`
#      sorts AFTER `drill-demo` — so an archive left here would be quarantined
#      in place of the drill's on a later `just e2e` run, and `full_drill`'s
#      corruption row would then watch an intact archive exit 0. That was
#      measured once already, for Task 4's `t4real-…` prefix. (Task 12 also
#      scopes that helper to the prefix it is given, which removes the hazard
#      structurally; the sweep stays, because a shared bucket with one run's
#      leftovers in it is its own problem.)
#
# THE DEMO NEVER TOUCHES `logweir.scratch` OR THE SEEDED `drill-demo` ARCHIVE.
# It restores in `target.mode: newTopic`, which needs no marker topic, and it
# reads and writes only under the two prefixes below.
BACKUP_ID=mvp-demo
RECEIPT_PREFIX=logweir/

die() { echo "mvp-demo: $*" >&2; exit 1; }
step() { echo; echo "==> $*"; }

# ---------------------------------------------------------------------------
# Tools, at second zero rather than four minutes in.
# ---------------------------------------------------------------------------
for tool in docker openssl awk; do
  command -v "$tool" >/dev/null 2>&1 || die "\`$tool\` is not on \$PATH. Requires: docker, openssl, awk, python3 (with \`cryptography\`)."
done

# The Python auditor verifier runs at steps 4 and 7. Its one dependency is
# checked HERE, because discovering it at the end wastes the whole run — and
# because skipping the second reader when it cannot import would let this
# script print a green result having run one of the two checks it claims to.
PYTHON="${LOGWEIR_PYTHON:-python3}"
command -v "$PYTHON" >/dev/null 2>&1 || die "\`$PYTHON\` is not on \$PATH; set LOGWEIR_PYTHON to an interpreter that has \`cryptography\`."
"$PYTHON" -c 'import cryptography' >/dev/null 2>&1 || die "$PYTHON cannot import \`cryptography\`, which docs/verify_scorecard.py needs: \`pip install cryptography\`, or point LOGWEIR_PYTHON at an interpreter that has it. It is checked now rather than at step 7 — and it is NOT skipped, because the independent second reader is the point."

# The bare word `logweir` is what the exit-code lint guards, so the binary goes
# on $PATH through a shim directory of exactly one entry rather than $PATH
# gaining all of target/debug.
LOGWEIR_BIN="${LOGWEIR_BIN:-$PWD/target/debug/logweir}"
[ -x "$LOGWEIR_BIN" ] || die "no logweir binary at $LOGWEIR_BIN — run \`cargo build -p logweir\` (\`just mvp-demo\` does it for you), or set LOGWEIR_BIN."
mkdir -p "$OUT/bin"
ln -sf "$LOGWEIR_BIN" "$OUT/bin/logweir"
PATH="$PWD/$OUT/bin:$PATH"
export PATH

# ---------------------------------------------------------------------------
# 1/8 PREFLIGHT. Refuse a stack that is not up, and refuse a DIRTY one.
# ---------------------------------------------------------------------------
step "1/8 preflight: the compose stack is up, healthy, and the source topics are empty"

set +e
docker compose -f "$COMPOSE_FILE" ps --format json > "$OUT/ps.json" 2> "$OUT/ps.stderr"
rc=$?
set -e
if [ "$rc" -ne 0 ]; then
  cat "$OUT/ps.stderr" >&2
  die "\`docker compose ps\` exited $rc. Is Docker running? Bring the stack up with: just e2e-up"
fi

# Parsed by python3 rather than grepped: compose v2 emits one JSON object per
# line (older builds emit one array), and both shapes have to read the same.
set +e
"$PYTHON" - "$OUT/ps.json" kafka-broker-1 minio <<'PY'
import json, sys

path, wanted = sys.argv[1], sys.argv[2:]
raw = open(path).read().strip()
rows = []
if raw.startswith("["):
    rows = json.loads(raw)
else:
    for line in raw.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))

by_service = {r.get("Service"): r for r in rows}
missing = [s for s in wanted if s not in by_service]
if missing:
    sys.exit(f"the compose stack is not up: no container for {', '.join(missing)}")

unhealthy = []
for s in wanted:
    r = by_service[s]
    state, health = r.get("State", "?"), (r.get("Health") or "")
    if state != "running" or health != "healthy":
        unhealthy.append(f"{s} is {state}/{health or 'no-health'}")
if unhealthy:
    sys.exit("the compose stack is not healthy: " + "; ".join(unhealthy))

print("    kafka-broker-1 and minio are both running/healthy")
PY
rc=$?
set -e
[ "$rc" -eq 0 ] || die "refusing to run against a stack that is not up and healthy. Bring a FRESH one up with:

    just e2e-down && just e2e-up
"

# `topic-setup` is the cp-kafka CLI image; --entrypoint swaps in whichever tool
# is wanted. KAFKA_OPTS is blanked because the image sets JMX flags meant for a
# long-running broker, not a one-shot CLI. The offsets land in a FILE and are
# summed from it afterwards: `… | awk` would put the exit code of `awk` where
# the broker's belongs.
end_offsets() {  # topic -> total records currently on the broker
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint kafka-get-offsets -e KAFKA_OPTS= topic-setup --bootstrap-server "$BOOTSTRAP_INNET" --topic "$1" > "$OUT/offsets-$1.txt" 2> "$OUT/offsets-$1.stderr"
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || die "could not read end offsets for topic $1 (rc=$rc); see $OUT/offsets-$1.stderr"
  awk -F: '{ s += $3 } END { print s+0 }' "$OUT/offsets-$1.txt"
}

# A SECOND run of this script on the same stack stops HERE, before anything is
# produced and before any archive exists — which is the whole point of checking
# it first. The voice is `scripts/e2e-seed.sh:81-91`'s, because it is the same
# fact: a second backup into a colliding backup_id does not accumulate, and a
# partial archive that a restore happily reads from is this repository's
# recurring false pass.
for topic in orders payments; do
  existing=$(end_offsets "$topic")
  [ "$existing" -eq 0 ] || die "topic ${topic} already holds ${existing} records; this demo wants a FRESH stack:

    just e2e-down && just e2e-up && just mvp-demo

A second backup into backup_id ${BACKUP_ID} does not accumulate and would leave a partial archive describing fewer records than the broker holds."
done
echo "    orders and payments both hold 0 records"

# ---------------------------------------------------------------------------
# Sweep, at the START. And from a trap, so EVERY exit path sweeps too.
# ---------------------------------------------------------------------------
# `docker compose run --rm` starts a FRESH container each time, so an `mc alias
# set` in one invocation is gone by the next; the alias comes from MC_HOST_local
# on the minio-setup service (see docker-compose.yml).
sweep_archive() {
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint mc minio-setup rm --recursive --force "local/$ARCHIVE_BUCKET/$BACKUP_ID/" > "$OUT/sweep-archive.log" 2>&1
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || echo "    (nothing to sweep under $ARCHIVE_BUCKET/$BACKUP_ID/)"
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint mc minio-setup rm --recursive --force "local/$ARCHIVE_BUCKET/${RECEIPT_PREFIX}backups/$BACKUP_ID/" > "$OUT/sweep-receipts.log" 2>&1
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || echo "    (nothing to sweep under $ARCHIVE_BUCKET/${RECEIPT_PREFIX}backups/$BACKUP_ID/)"
}

echo "    sweeping any earlier run's archive out of the shared bucket"
sweep_archive
# `trap … EXIT` is the shell's `SweptArchive` (e2e/tests/pitr_boundary.rs): a
# run that dies at step 5 must not leave `mvp-demo/…` behind for the next
# `just e2e` to trip over. It runs on the success path too — a demo archive is
# not evidence, and the receipt and scorecard it produced are still on disk
# under .demo/mvp/.
#
# THE START-OF-RUN SWEEP IS UNCONDITIONAL. `LOGWEIR_MVP_DEMO_KEEP_ARCHIVE=1`
# suppresses only the one below, and exactly one caller sets it: the e2e row
# `e2e/tests/mvp_demo.rs`, which has to read this run's MANIFEST back to
# compute `expected_restored_count`'s bound (interface I5) and then sweeps the
# bucket itself from a `Drop` guard — which survives a panicking assertion,
# where a shell `trap` in a process that has already exited cannot help.
if [ "${LOGWEIR_MVP_DEMO_KEEP_ARCHIVE:-0}" = "1" ]; then
  echo "    LOGWEIR_MVP_DEMO_KEEP_ARCHIVE=1: the end-of-run sweep is the caller's"
else
  trap 'echo; echo "==> sweeping the demo archive out of the shared bucket"; sweep_archive' EXIT
fi

# ---------------------------------------------------------------------------
# 2/8 PRODUCE.
# ---------------------------------------------------------------------------
step "2/8 producing 1000 records into each of orders and payments"

RECORDS_PER_TOPIC="${RECORDS_PER_TOPIC:-1000}"

# awk generates the range directly rather than `seq 1 $N | awk`: BSD seq
# (macOS, where this repository is developed) COUNTS DOWN when first > last, so
# `seq 1 0` prints "1\n0" and a zero-record request would silently produce two
# records.
for topic in orders payments; do
  awk -v t="$topic" -v n="$RECORDS_PER_TOPIC" 'BEGIN { for (i = 1; i <= n; i++) printf "%s-%06d:{\"id\":%d,\"topic\":\"%s\"}\n", t, i, i, t }' > "$OUT/records-$topic.txt"
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint kafka-console-producer -e KAFKA_OPTS= topic-setup --bootstrap-server "$BOOTSTRAP_INNET" --topic "$topic" --property "parse.key=true" --property "key.separator=:" < "$OUT/records-$topic.txt"
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || die "producing into $topic exited $rc"
done

# The producer exits 0 whether or not the broker accepted anything, so the end
# offsets are read back OFF THE BROKER and required to be non-zero. This is
# also the number the receipt is checked against, measured rather than assumed
# from RECORDS_PER_TOPIC.
BROKER_RECORDS=0
for topic in orders payments; do
  t_records=$(end_offsets "$topic")
  [ "$t_records" -gt 0 ] || die "topic ${topic} holds 0 records after producing; the archive would be empty"
  echo "    ${topic}: ${t_records} records on the broker"
  BROKER_RECORDS=$((BROKER_RECORDS + t_records))
done

# THE RECOVERY POINT, and the sample window, computed here — after the records
# exist — and printed. Plan errata E6/E7: every epoch instant in this project is
# computed with `python3 -c 'import datetime as d; …'` rather than typed, because
# a hand-written epoch millisecond was wrong by eight days once already.
#
# `point_in_time` is five seconds AFTER the last record so every record
# produced above is at or before it (the boundary is INCLUSIVE — guard G-PITR,
# e2e/tests/pitr_boundary.rs). It must also be strictly ABOVE the archive's
# floor, or phase 0 refuses with exit 3 naming both integers (erratum E7(a)),
# which it is: the floor is the first record's timestamp.
#
# `sample.window_start` reaches an hour back, which is before the first record
# and after nothing else — these topics held zero records a minute ago. That
# puts every manifest segment WHOLLY inside the sample window, so phase 7's
# `claimed` is the sample size rather than a straddling segment's whole record
# count (Task 10b).
read -r PIT_RFC3339 PIT_STAMP PIT_MS SAMPLE_START_RFC3339 <<EOF
$("$PYTHON" -c 'import datetime as d
now = d.datetime.now(d.timezone.utc).replace(microsecond=0)
pit = now + d.timedelta(seconds=5)
start = pit - d.timedelta(hours=1)
print(pit.strftime("%Y-%m-%dT%H:%M:%SZ"), pit.strftime("%Y%m%dT%H%M%SZ"),
      int(pit.timestamp() * 1000), start.strftime("%Y-%m-%dT%H:%M:%SZ"))')
EOF
[ -n "${PIT_RFC3339:-}" ] || die "could not compute the recovery point"
echo "    recovery point: $PIT_RFC3339  (epoch ms $PIT_MS)"
echo "    sample window:  $SAMPLE_START_RFC3339 .. $PIT_RFC3339, 25 records per partition"

# ---------------------------------------------------------------------------
# Keys and the allowlist, before the first `logweir` command.
# ---------------------------------------------------------------------------
# TWO DIFFERENT KEYS, minted with the two `openssl` commands the install docs
# carry: the one that APPROVES a plan is never the one that SIGNS the result.
if [ ! -f "$OUT/signing.pem" ]; then
  openssl ecparam -genkey -name prime256v1 -noout > "$OUT/signing.der.pem"
  openssl pkcs8 -topk8 -nocrypt -in "$OUT/signing.der.pem" -out "$OUT/signing.pem"
  openssl ecparam -genkey -name prime256v1 -noout > "$OUT/approver.der.pem"
  openssl pkcs8 -topk8 -nocrypt -in "$OUT/approver.der.pem" -out "$OUT/approver.pem"
  rm -f "$OUT/signing.der.pem" "$OUT/approver.der.pem"
fi
openssl ec -in "$OUT/signing.pem"  -pubout -out "$OUT/signing.pub.pem"  2>/dev/null
openssl ec -in "$OUT/approver.pem" -pubout -out "$OUT/approver.pub.pem" 2>/dev/null

# SAID OUT LOUD, because the code will not say it: `SigningKey::load_or_generate`
# (crates/logweir-evidence/src/keys.rs) MINTS SILENTLY against an empty key
# path. So a run whose `--signing-key` points at nothing still produces a
# perfectly valid signature — over a key that nothing attests, that no roster
# names, and that no auditor has ever seen. The two keys below were minted on
# this machine, minutes ago, by this script. They prove the document has not
# been edited since it was signed. They prove nothing whatsoever about WHO
# signed it. See docs/keys.md and docs/verify-a-scorecard.md.
echo "    keys: $OUT/signing.pem (signs) and $OUT/approver.pem (approves) — two DIFFERENT keys"
echo "          WARNING: freshly minted on this machine and attested by nothing."
echo "          A signature over a key nobody has published proves integrity, not provenance."

# The allowlist is the RESTORE-TARGET allowlist, and GC18(c) rail 4 refuses a
# SOURCE cluster that appears in it: a cluster cannot be both the source of an
# archive and a permitted scratch target. So the live broker must be ABSENT
# from it for `backup run` to be admitted at all — and `target.mode: newTopic`
# requires no membership either way (difference 2 of the four; see
# examples/restore.yaml). `scripts/demo.sh` derives the opposite file for the
# opposite reason: a `mode: scratch` drill needs the live id PRESENT.
printf '{"allowed_cluster_ids":["SCRATCH-CLUSTER-NOT-THE-SOURCE"],"source_cluster_id":null}\n' > "$OUT/allowed-clusters.json"

# The engine ROUTE is probed, never assumed, and it is PRINTED — a demo that
# quietly swapped its engine would be showing you a result about something
# other than what it claims to run. Upstream publishes the image for
# linux/amd64 only (permitted by global ruling GR6), so on the darwin/arm64
# laptop this repository is developed on the extracted ELF cannot exec at all
# and the same digest-pinned image is used under --platform linux/amd64.
export LOGWEIR_E2E_ENGINE_MOUNT="$PWD/$OUT/tmp"
mkdir -p "$LOGWEIR_E2E_ENGINE_MOUNT"
if .engine/kafka-backup --version >/dev/null 2>&1; then
  export LOGWEIR_ENGINE_BIN="$PWD/.engine/kafka-backup"
  echo "    engine route: NATIVE $LOGWEIR_ENGINE_BIN"
else
  export LOGWEIR_ENGINE_BIN="$PWD/e2e/fixtures/engine-docker.sh"
  echo "    engine route: CONTAINER via $LOGWEIR_ENGINE_BIN (.engine/kafka-backup is a linux/amd64 ELF and cannot exec here)"
fi
LOGWEIR_ENGINE_VERSION=$("$LOGWEIR_ENGINE_BIN" --version)
LOGWEIR_ENGINE_VERSION=${LOGWEIR_ENGINE_VERSION##* }
LOGWEIR_ENGINE_DIGEST=$(tr -d '[:space:]' < third_party/kafka-backup-binary.digest)
export LOGWEIR_ENGINE_VERSION LOGWEIR_ENGINE_DIGEST
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1
export TMPDIR="$LOGWEIR_E2E_ENGINE_MOUNT"
echo "    engine: $LOGWEIR_ENGINE_VERSION $LOGWEIR_ENGINE_DIGEST"

# ---------------------------------------------------------------------------
# 3/8 BACK UP — the PRODUCT taking the backup, not the harness.
# ---------------------------------------------------------------------------
step "3/8 backing up orders and payments with \`logweir backup run\`"

# examples/backup.yaml is the HOST-SIDE shape: `logweir` and the engine both
# run on the host here, so the compose SERVICE name for MinIO would not
# resolve — only the published localhost port does (critique A F23).
set +e
logweir backup run --spec examples/backup.yaml --allowed-clusters "$OUT/allowed-clusters.json" --signing-key "$OUT/signing.pem" --triggered-by mvp-demo --receipt-out "$OUT/receipt.json" > "$OUT/backup.stdout"
rc=$?
set -e
echo "    rc=$rc  (logweir backup run)"
if [ "$rc" -ne 0 ]; then
  tail -40 "$OUT/backup.stdout" >&2
  die "\`logweir backup run\` exited $rc; full output in $OUT/backup.stdout"
fi
tail -2 "$OUT/backup.stdout"

# --receipt-out <path> writes the DSSE sidecar beside it with the extension
# replaced by .sig (interface I6, Task 5b). Both halves are required below:
# a document without its signature is not evidence.
[ -s "$OUT/receipt.json" ] || die "no receipt at $OUT/receipt.json"
[ -s "$OUT/receipt.sig" ]  || die "no sidecar at $OUT/receipt.sig — --receipt-out writes the pair"

# ---------------------------------------------------------------------------
# 4/8 VERIFY THE RECEIPT, WITH BOTH READERS.
# ---------------------------------------------------------------------------
step "4/8 verifying the backup receipt with BOTH readers"

set +e
logweir drill verify --payload-type backup-receipt --scorecard "$OUT/receipt.json" --signature "$OUT/receipt.sig" --public-key "$OUT/signing.pub.pem"
rc=$?
set -e
echo "    rc=$rc  (logweir drill verify --payload-type backup-receipt)"
[ "$rc" -eq 0 ] || die "the Rust reader refused the receipt (rc=$rc)"

# The second reader shares NO CODE with Logweir — that is the entire point of
# there being two. A demo that ran one of them would be demonstrating half of
# the product's claim while printing the word "verified".
set +e
"$PYTHON" docs/verify_scorecard.py --payload-type backup-receipt "$OUT/receipt.json" "$OUT/receipt.sig" "$OUT/signing.pub.pem"
rc=$?
set -e
echo "    rc=$rc  (python3 docs/verify_scorecard.py --payload-type backup-receipt)"
[ "$rc" -eq 0 ] || die "the independent Python reader refused the receipt (rc=$rc)"

# ---------------------------------------------------------------------------
# 5/8 THE RESTORE PLAN, AND AN APPROVAL OVER ITS EXACT BYTES.
# ---------------------------------------------------------------------------
step "5/8 binding the recovery point into the restore plan, then approving those exact bytes"

# examples/restore.yaml IS the plan document (RestoreSpec, interface I20) and
# is used verbatim except for FOUR fields, each of which is deployment-specific
# and cannot be carried in a checked-in example:
#
#   source.storage.prefix   this run's archive, not the seeded drill-demo one
#   restore.point_in_time   the recovery point, five seconds after the records
#   sample.window_start     an hour back — before the first record
#   sample.window_end       the recovery point
#
# The `prefix` rewrite is RANGE-SCOPED to the `source:` block. There are two
# other `prefix:` keys in the document — `target.topic_mapping_prefix` (a
# different key name, so it cannot match) and `evidence.prefix: logweir/`
# (the same key name, in a block Global Constraint 6 pins) — and an unscoped
# substitution would rewrite the second and break GC6's `logweir/` root.
sed -e "/^source:/,/^target:/ s|^\( *prefix:\).*|\1 $BACKUP_ID|" \
    -e "s|^\( *point_in_time:\).*|\1 \"$PIT_RFC3339\"|" \
    -e "s|^\( *window_start:\).*|\1 \"$SAMPLE_START_RFC3339\"|" \
    -e "s|^\( *window_end:\).*|\1   \"$PIT_RFC3339\"|" \
    examples/restore.yaml > "$OUT/restore.yaml"

# CHECKED, not assumed: a sed that matched nothing exits 0 and leaves the
# illustrative dates in place, which would be refused at phase 0 as a window
# over an archive that did not exist yet — a correct refusal and a baffling
# first experience.
grep -q "prefix: $BACKUP_ID" "$OUT/restore.yaml" || die "failed to bind the archive prefix into $OUT/restore.yaml"
grep -q "point_in_time: \"$PIT_RFC3339\"" "$OUT/restore.yaml" || die "failed to bind the recovery point into $OUT/restore.yaml"
grep -q "window_start: \"$SAMPLE_START_RFC3339\"" "$OUT/restore.yaml" || die "failed to bind the sample window start into $OUT/restore.yaml"
grep -q "prefix: logweir/" "$OUT/restore.yaml" || die "the evidence prefix was rewritten; Global Constraint 6 pins it at logweir/"
echo "    plan: $OUT/restore.yaml  (four fields bound; everything else is examples/restore.yaml verbatim)"

# `drill approve` hashes `fs::read_to_string(&args.spec)` EXACTLY
# (crates/logweir/src/approve.rs), so the bytes approved are the bytes on disk
# — which is why it approves the rendered plan and not the example. Approving
# one document and running another is precisely what `plan_hash` exists to
# catch, and it would be refused at phase 1 with exit 3.
set +e
logweir drill approve --spec "$OUT/restore.yaml" --key "$OUT/approver.pem" --approver mvp-demo --ticket DEMO-1 --out "$OUT/approval.json"
rc=$?
set -e
echo "    rc=$rc  (logweir drill approve)"
[ "$rc" -eq 0 ] || die "\`logweir drill approve\` exited $rc"

# ---------------------------------------------------------------------------
# 6/8 RESTORE INTO A NEW TOPIC, AT A POINT IN TIME.
# ---------------------------------------------------------------------------
step "6/8 restoring to $PIT_RFC3339 into brand-new topics (target.mode: newTopic)"

# What `newTopic` means here, in one sentence: the records land in topics that
# DID NOT EXIST, on the same cluster they came from, and phase 9 tears NOTHING
# down — deleting the restored topic would delete the recovery.
set +e
logweir restore run --spec "$OUT/restore.yaml" --approval "$OUT/approval.json" --approver-key "$OUT/approver.pub.pem" --allowed-clusters "$OUT/allowed-clusters.json" --signing-key "$OUT/signing.pem" --triggered-by mvp-demo --out "$OUT/scorecard.json" --offset-report-out "$OUT/offsets.json" > "$OUT/restore.stdout"
rc=$?
set -e
echo "    rc=$rc  (logweir restore run)"
if [ "$rc" -ne 0 ]; then
  tail -40 "$OUT/restore.stdout" >&2
  die "\`logweir restore run\` exited $rc; full output in $OUT/restore.stdout"
fi

[ -s "$OUT/scorecard.json" ] || die "no scorecard at $OUT/scorecard.json"
[ -s "$OUT/scorecard.sig" ]  || die "no sidecar at $OUT/scorecard.sig — --out writes the pair"

# ---------------------------------------------------------------------------
# 7/8 SHOW IT, VERIFY IT TWICE, AND NAME THE EVIDENCE.
# ---------------------------------------------------------------------------
step "7/8 showing the scorecard, then verifying it with BOTH readers"

set +e
logweir drill show "$OUT/scorecard.json"
rc=$?
set -e
echo "    rc=$rc  (logweir drill show)"
[ "$rc" -eq 0 ] || die "\`logweir drill show\` exited $rc"

set +e
logweir drill verify --scorecard "$OUT/scorecard.json" --signature "$OUT/scorecard.sig" --public-key "$OUT/signing.pub.pem"
rc=$?
set -e
echo "    rc=$rc  (logweir drill verify)"
[ "$rc" -eq 0 ] || die "the Rust reader refused the scorecard (rc=$rc)"

set +e
"$PYTHON" docs/verify_scorecard.py "$OUT/scorecard.json" "$OUT/scorecard.sig" "$OUT/signing.pub.pem"
rc=$?
set -e
echo "    rc=$rc  (python3 docs/verify_scorecard.py)"
[ "$rc" -eq 0 ] || die "the independent Python reader refused the scorecard (rc=$rc)"

# The evidence object keys the runner emitted, interface I8: `scorecard-key=`,
# then `sidecar-key=`, then `offset-report-key=` — the third present exactly
# when the engine wrote an offset report. They are the FINAL stdout lines of
# the run and they are what an auditor fetches; a demo that printed a local
# path would be showing you the copy, not the evidence.
echo
echo "    the runner's evidence keys (interface I8):"
grep -E '^(scorecard|sidecar|offset-report)-key=' "$OUT/restore.stdout" > "$OUT/evidence-keys.txt" || true
[ -s "$OUT/evidence-keys.txt" ] || die "the runner printed no evidence keys; I8 says it must"
sed 's/^/      /' "$OUT/evidence-keys.txt"

SCORECARD_KEY=$(sed -n 's/^scorecard-key=//p' "$OUT/evidence-keys.txt")
RECEIPT_KEY=$(sed -n 's/^receipt-key=//p' "$OUT/backup.stdout")
[ -n "$SCORECARD_KEY" ] || die "no scorecard-key= line in $OUT/restore.stdout"
[ -n "$RECEIPT_KEY" ]   || die "no receipt-key= line in $OUT/backup.stdout"

echo
echo "    the exact command an auditor runs, over the bytes they fetched:"
echo
echo "      python3 docs/verify_scorecard.py \\"
echo "        scorecard.json scorecard.sig signing.pub.pem"
echo
echo "      python3 docs/verify_scorecard.py --payload-type backup-receipt \\"
echo "        receipt.json receipt.sig signing.pub.pem"
echo
echo "    It imports nothing from Logweir and needs no Rust toolchain: one file,"
echo "    docs/verify_scorecard.py, and the \`cryptography\` package."

# ---------------------------------------------------------------------------
# 8/8 THE SUMMARY.
# ---------------------------------------------------------------------------
step "8/8 summary"

RESTORED_TOPICS=""
RESTORED_RECORDS=0
for topic in orders payments; do
  new_topic="restore-${PIT_STAMP}-${topic}"
  n=$(end_offsets "$new_topic")
  [ "$n" -gt 0 ] || die "restored topic $new_topic holds 0 records"
  echo "    restored topic      $new_topic  ->  $n records (read off the broker)"
  RESTORED_RECORDS=$((RESTORED_RECORDS + n))
  if [ -z "$RESTORED_TOPICS" ]; then RESTORED_TOPICS="$new_topic"; else RESTORED_TOPICS="$RESTORED_TOPICS,$new_topic"; fi
done

read -r OUTCOME RTO RPO <<EOF
$("$PYTHON" -c 'import json,sys
sc = json.load(open(sys.argv[1]))
m = sc.get("measured", {})
print(sc.get("outcome"), m.get("rto_seconds"), m.get("rpo_seconds"))' "$OUT/scorecard.json")
EOF

echo "    source records      $BROKER_RECORDS (orders + payments, read off the broker)"
echo "    recovery point      $PIT_RFC3339"
echo "    outcome             $OUTCOME"
echo "    measured rto        ${RTO}s"
echo "    measured rpo        ${RPO}s"
echo "    receipt key         $RECEIPT_KEY"
echo "    scorecard key       $SCORECARD_KEY"
echo
echo "mvp-demo: $OUTCOME restored $RESTORED_RECORDS records into $RESTORED_TOPICS at $PIT_RFC3339; rto=${RTO}s rpo=${RPO}s; receipt-key=$RECEIPT_KEY scorecard-key=$SCORECARD_KEY"
echo
echo "The scorecard and its signature are at $OUT/scorecard.json and $OUT/scorecard.sig;"
echo "the backup receipt is at $OUT/receipt.json and $OUT/receipt.sig. Both were verified"
echo "by two independent readers above. Tear the stack down with: just e2e-down"
