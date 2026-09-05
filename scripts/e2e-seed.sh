#!/usr/bin/env bash
# Seed the compose stack: produce records, take a backup with the pinned OSO
# CLI, and refresh the fixtures that must come from a REAL 0.21.0 archive
# rather than from Logweir's own encoder.
#
# Global Constraint 3 binds the `logweir` BINARY to {restore, validate-restore,
# validation run}. This is a seed harness, not the binary, and GC3 names
# scripts/e2e-seed.sh explicitly as a place `backup` may be invoked;
# scripts/check-no-oso.sh enforces the runtime half by scanning crates/.
#
# Every step below is fail-closed on purpose. "The command exited 0" is not
# evidence that an archive exists: a backup of empty topics also exits 0, and a
# drill that restores nothing and reports success is this project's recurring
# defect. So each stage asserts a COUNT or a HASH, not an exit status.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose -f e2e/compose/docker-compose.yml"
RECORDS_PER_TOPIC="${RECORDS_PER_TOPIC:-1000}"
ARCHIVE="local/kafka-backups/drill-demo"

die() { echo "e2e-seed: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 0. The digest must be resolved AND must agree with the one source of truth.
# ---------------------------------------------------------------------------
# Fail closed rather than seed against an unresolved digest placeholder.
[ -f e2e/compose/.env ] || die "e2e/compose/.env is missing; run \`just engine\` first"
# An `if`, not `grep -q … && die`: under `set -e` the latter makes the whole
# guard evaluate to 1 on the HAPPY path (grep finds nothing), which is exempt
# from `set -e` today only by a subtle clause in its rules. Say it plainly.
if grep -q REPLACE_WITH_PINNED_DIGEST e2e/compose/.env; then
  die "e2e/compose/.env still holds REPLACE_WITH_PINNED_DIGEST; run \`just engine\` first"
fi

# .env is GENERATED from third_party/kafka-backup-binary.digest by
# scripts/extract-engine.sh. If the two ever disagree, the archive this script
# writes was produced by a different engine build than the one the workspace
# vendors and tests against — a silent provenance break, and exactly the thing
# Global Constraint 7's digest pin exists to prevent. Compare them here, where
# it is cheap, rather than discovering it in a scorecard.
env_digest=$(sed -n 's/^OSO_DIGEST=//p' e2e/compose/.env | tr -d '[:space:]')
pin_digest=$(tr -d '[:space:]' < third_party/kafka-backup-binary.digest)
[ -n "$env_digest" ] || die "e2e/compose/.env defines no OSO_DIGEST"
[ "$env_digest" = "$pin_digest" ] \
  || die "digest drift: e2e/compose/.env has ${env_digest} but third_party/kafka-backup-binary.digest has ${pin_digest}; re-run \`just engine\`"
echo "==> engine pinned at ${pin_digest} (.env and third_party/ agree)"

# ---------------------------------------------------------------------------
# 1. Produce.
# ---------------------------------------------------------------------------
# `topic-setup` is the cp-kafka CLI image; --entrypoint swaps in whichever tool
# we need. KAFKA_OPTS is blanked because the image sets JMX flags meant for a
# long-running broker, not a one-shot CLI.
end_offsets() {  # topic -> total records currently on the broker
  $COMPOSE run --rm -T --entrypoint kafka-get-offsets -e KAFKA_OPTS= topic-setup \
      --bootstrap-server kafka-broker-1:9094 --topic "$1" 2>/dev/null \
    | awk -F: '{ s += $3 } END { print s+0 }'
}

# This script seeds a FRESH stack, and says so before it does any work rather
# than after. A second `backup` into the same backup_id does NOT accumulate:
# measured on this stack, a re-run left the manifest describing 2048 records
# while the broker held 6000 — a partial archive that a drill would happily
# restore from and call a success. That is the exact false-pass this repo keeps
# hitting, so refuse here instead of producing 1000 more records into a state
# the archive cannot represent.
for topic in orders payments; do
  existing=$(end_offsets "$topic")
  [ "$existing" -eq 0 ] || die "topic ${topic} already holds ${existing} records; seed a FRESH stack (\`just e2e-down && just e2e-up\`) — a second backup into backup_id drill-demo does not accumulate and would leave a partial archive"
done

echo "==> producing ${RECORDS_PER_TOPIC} records into each of orders and payments"
for topic in orders payments; do
  # awk generates the range directly rather than `seq 1 $N | awk`: BSD seq
  # (macOS, where this repo is developed) COUNTS DOWN when first > last, so
  # `seq 1 0` prints "1\n0" and a zero-record request would silently produce
  # two records. One tool, one loop, no surprise at the boundary.
  awk -v t="$topic" -v n="$RECORDS_PER_TOPIC" \
      'BEGIN { for (i = 1; i <= n; i++) printf "%s-%06d:{\"id\":%d,\"topic\":\"%s\"}\n", t, i, i, t }' \
  | $COMPOSE run --rm -T --entrypoint kafka-console-producer \
      -e KAFKA_OPTS= topic-setup \
      --bootstrap-server kafka-broker-1:9094 \
      --topic "$topic" --property "parse.key=true" --property "key.separator=:"
done

# The producer exits 0 whether or not the broker accepted anything, so read the
# end offsets back off the broker and require them to be non-zero. This is also
# the number the manifest is checked against below — measured, not the
# RECORDS_PER_TOPIC constant, so the check still means something if the producer
# drops records or the broker rejects some.
broker_records=0
for topic in orders payments; do
  t_records=$(end_offsets "$topic")
  [ "$t_records" -gt 0 ] || die "topic ${topic} holds 0 records after producing; the archive would be empty"
  echo "    ${topic}: ${t_records} records on the broker"
  broker_records=$((broker_records + t_records))
done

# ---------------------------------------------------------------------------
# 2. Back up with the digest-pinned engine.
# ---------------------------------------------------------------------------
echo "==> taking a backup with the digest-pinned kafka-backup image"
$COMPOSE --profile tools run --rm kafka-backup \
  backup --config /config/backup-drill.yaml

# ---------------------------------------------------------------------------
# 3. Prove the archive is real, then refresh the fixtures from it.
# ---------------------------------------------------------------------------
# `docker compose run --rm` starts a FRESH container each time, so an `mc alias
# set` in one invocation is gone by the next; the alias comes from
# MC_HOST_local on the minio-setup service instead (see docker-compose.yml).
MC="$COMPOSE run --rm -T --entrypoint mc minio-setup"

# Upstream names segments `segment-<offset>.bin[.zst|.lz4]`, NOT `*.kbak` —
# `.kbak` is only ever this repo's own fixture extension. Select on the
# `/topics/.../segment-` shape so the selector survives a compression change.
listing=$($MC --json ls --recursive "$ARCHIVE" 2>/dev/null)
[ -n "$listing" ] || die "no objects under ${ARCHIVE}: the backup wrote nothing"

MANIFEST_KEY=$(printf '%s\n' "$listing" | python3 -c '
import sys, json
keys = [json.loads(l)["key"] for l in sys.stdin if l.strip()]
m = [k for k in keys if k.endswith("manifest.json")]
if not m:
    sys.exit("no manifest.json in the archive")
print(sorted(m)[0])')
SEGMENT_KEY=$(printf '%s\n' "$listing" | python3 -c '
import sys, json, posixpath
keys = [json.loads(l)["key"] for l in sys.stdin if l.strip()]
segs = [k for k in keys if "/topics/" in k and posixpath.basename(k).startswith("segment-")]
if not segs:
    sys.exit("no segment objects in the archive")
print(sorted(segs)[0])')

echo "==> refreshing the fixtures from the REAL archive"
# `mc cat` to stdout, not `mc cp` + `docker compose cp`: the mc container is
# `--rm`, so there is no container left to copy out of afterwards. With -T
# there is no TTY between here and the object, and the sha256 assertion below
# is what proves the bytes survived the pipe unchanged.
$MC cat "${ARCHIVE}/${MANIFEST_KEY}" 2>/dev/null > e2e/fixtures/manifests/0.21.json
[ -s e2e/fixtures/manifests/0.21.json ] || die "manifest fixture came back empty"

# NOTE ON THE SEGMENT FIXTURE PATH. The task brief targets
# e2e/fixtures/segments/none.kbak. That path is no longer free: none.kbak is a
# hand-minted 5-record UNCOMPRESSED container, and `none`/`zstd`/`lz4` form a
# compression matrix that crates/logweir-engine-oso/tests/kbak.rs cross-compares
# record for record, plus pins for null-vs-empty fields, a corrupted key-length
# prefix and a disagreeing total_len. Overwriting it with this archive's
# 338-record ZSTD-flagged segment fails 8 existing tests (6 in kbak.rs, 2 in
# engine.rs) and makes the filename assert a compression the bytes do not use.
# The upstream bytes therefore land beside the matrix instead of on top of it;
# the manifest fixture, which no test pins to hand-authored content, is
# refreshed in place exactly as the brief asks.
$MC cat "${ARCHIVE}/${SEGMENT_KEY}" 2>/dev/null > e2e/fixtures/segments/upstream-0.21.0.kbak
[ -s e2e/fixtures/segments/upstream-0.21.0.kbak ] || die "segment fixture came back empty"

# The manifest is the archive's own account of itself, so check it against the
# broker (record count) and against the bytes on disk (sha256). An archive of
# empty topics, a truncated download and a manifest with placeholder hashes all
# fail here; none of them would fail an exit-status check.
segment_sha=$(shasum -a 256 e2e/fixtures/segments/upstream-0.21.0.kbak | cut -d' ' -f1)
python3 - "$broker_records" "$SEGMENT_KEY" "$segment_sha" <<'PY'
import json, sys
broker_records, segment_key, segment_sha = int(sys.argv[1]), sys.argv[2], sys.argv[3]
m = json.load(open("e2e/fixtures/manifests/0.21.json"))
segs = [s for t in m["topics"] for p in t["partitions"] for s in p["segments"]]
if not segs:
    sys.exit("manifest lists no segments: the archive is empty")
total = sum(s["record_count"] for s in segs)
if total != broker_records:
    sys.exit(f"manifest holds {total} records but the broker holds {broker_records}")
missing = [s["key"] for s in segs if not s.get("sha256")]
if missing:
    sys.exit(f"segments with an empty sha256 (not a v0.21.0 manifest): {missing}")
recorded = {s["key"]: s["sha256"] for s in segs}
if segment_key not in recorded:
    sys.exit(f"copied segment {segment_key} is not listed in the manifest")
if recorded[segment_key] != segment_sha:
    sys.exit(f"copied segment sha256 {segment_sha} != manifest's {recorded[segment_key]}")
print(f"    manifest: {len(m['topics'])} topics, {len(segs)} segments, "
      f"{total} records, every sha256 present")
print(f"    segment:  {segment_key}")
print(f"              sha256 {segment_sha} matches the manifest byte for byte")
PY

# A KBAK container, not a JSON blob or an HTML error page.
head -c 4 e2e/fixtures/segments/upstream-0.21.0.kbak | grep -q '^KBAK' \
  || die "segment fixture does not start with the KBAK magic"

echo "==> seeded. manifest -> e2e/fixtures/manifests/0.21.json"
echo "               segment -> e2e/fixtures/segments/upstream-0.21.0.kbak (real 0.21.0 bytes)"
