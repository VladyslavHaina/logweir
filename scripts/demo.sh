#!/usr/bin/env bash
# The Logweir demo. Requires: docker, cargo, openssl, shasum, python3 (with the
# `cryptography` package). Takes ~4 minutes on a warm cargo cache.
#
# Zero cloud spend (Global Constraint 17): everything runs in local containers.
#
# WHAT THIS SCRIPT SETS THAT THE README's four commands DO NOT SHOW, and why —
# read this before copying a line out of it into a production runbook:
#
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION
#     MinIO's compose credentials. `logweir` reaches the bucket through
#     `object_store`'s OWN credential chain (see docs/stability.md), which
#     reads these two variables; it does NOT read ~/.aws/credentials.
#
#   LOGWEIR_ENGINE_BIN / LOGWEIR_ENGINE_VERSION / LOGWEIR_ENGINE_DIGEST
#     The container image sets all three. A standalone binary carries NO
#     engine, so a laptop run has to say where the extracted engine is and
#     which digest-pinned image it came from. An empty version or digest is
#     REFUSED (`drill run` exits 1): a signed scorecard must name the engine
#     that produced the restore.
#
#   TMPDIR
#     The rendered restore.yaml and the restore checkpoint land under
#     `std::env::temp_dir()`. On the container engine route below they must be
#     inside the one directory the engine container has bind-mounted.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose -f e2e/compose/docker-compose.yml"

# `jq` is no longer here: the only step that used it was the approval, which
# is now `logweir drill approve`. A prerequisite check that demands a tool
# nothing runs is the same shape as a check that reports ok having run
# nothing — it makes the requirement list untrustworthy.
for tool in docker cargo openssl shasum; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "demo: \`$tool\` is not on \$PATH. Requires: docker, cargo, openssl, shasum, python3." >&2
    exit 1
  }
done

# The Python auditor verifier is step 6/6, four minutes in. Its one dependency
# is checked HERE, at second zero, because discovering it at the end wastes the
# whole run — and because the alternative (skipping the second verifier when it
# cannot import) would let the demo print a green result having run only one of
# the two checks it claims to run. Override the interpreter with
# `LOGWEIR_PYTHON=/path/to/python3` (e.g. a virtualenv).
PYTHON="${LOGWEIR_PYTHON:-python3}"
command -v "$PYTHON" >/dev/null 2>&1 || {
  echo "demo: \`$PYTHON\` is not on \$PATH. Requires: docker, cargo, openssl, shasum, python3." >&2
  exit 1
}
"$PYTHON" -c 'import cryptography' >/dev/null 2>&1 || {
  cat >&2 <<EOF
demo: $PYTHON cannot import \`cryptography\`, which docs/verify_scorecard.py needs.

  pip install cryptography

or point LOGWEIR_PYTHON at an interpreter that has it. This is checked now
rather than at step 6/6 so a four-minute run does not end on a missing package
— and it is NOT skipped, because the independent second verifier is the point.
EOF
  exit 1
}

echo "==> 1/6 extracting the pinned kafka-backup engine"
./scripts/extract-engine.sh

echo "==> 2/6 starting Kafka (KRaft) and MinIO"
# `--wait` blocks until both servers report HEALTHY. The two one-shot setup
# services sit behind the `setup` profile and run afterwards, in order —
# `minio-setup` FIRST, because it creates the `kafka-backups` and
# `logweir-evidence` buckets that steps 3 and 5 write into.
$COMPOSE up -d --wait
$COMPOSE --profile setup run --rm minio-setup
$COMPOSE --profile setup run --rm topic-setup

echo "==> 3/6 producing records and taking a backup with the pinned engine"
# LOGWEIR_SEED_REFRESH_FIXTURES=0: seed the stack, but do NOT refresh the two
# CHECKED-IN fixtures `scripts/e2e-seed.sh` refreshes by default
# (e2e/fixtures/manifests/0.21.json and
# e2e/fixtures/segments/upstream-0.21.0.kbak). A drill needs the archive in
# MinIO, not those files; refreshing them is a maintainer action (`just
# e2e-seed`). Without this, running the quickstart left a stranger with two
# modified tracked files, no explanation, and a working tree that no longer
# satisfies the release gate's clean-tree precondition.
LOGWEIR_SEED_REFRESH_FIXTURES=0 ./scripts/e2e-seed.sh

echo "==> 4/6 minting a signing key and an APPROVER key (two different keys)"
mkdir -p .demo
openssl ecparam -genkey -name prime256v1 -noout | openssl pkcs8 -topk8 -nocrypt -out .demo/signer.pem
openssl ec -in .demo/signer.pem   -pubout -out .demo/signer.pub.pem
openssl ecparam -genkey -name prime256v1 -noout | openssl pkcs8 -topk8 -nocrypt -out .demo/approver.pem
openssl ec -in .demo/approver.pem -pubout -out .demo/approver.pub.pem

# The allowlist is DERIVED from the running broker, never hand-written: an
# allowlist that does not name this cluster makes phase 0 refuse with exit 3,
# which is correct behaviour and a confusing first experience.
CLUSTER_ID=$($COMPOSE run --rm -T --entrypoint kafka-cluster -e KAFKA_OPTS= topic-setup \
  cluster-id --bootstrap-server kafka-broker-1:9094 | awk -F': *' '/Cluster ID/{print $2}' | tr -d '\r')
[ -n "$CLUSTER_ID" ] || { echo "demo: could not read the broker's cluster id" >&2; exit 1; }
printf '{"allowed_cluster_ids":["%s"],"source_cluster_id":null}\n' "$CLUSTER_ID" \
  > .demo/allowed-clusters.json
echo "    target cluster: $CLUSTER_ID"

# The engine ROUTE is probed, never assumed. Upstream publishes
# `osodevops/kafka-backup` for linux/amd64 ONLY (pulling it is permitted by
# global ruling GR6; Global Constraint 14 governs what Logweir PUBLISHES
# under). On a linux/amd64 host the extracted ELF runs directly. On the
# darwin/arm64 laptop this repository is developed on it cannot exec at all
# (ENOEXEC — `logweir doctor` reports exit 126), so the demo falls back to the
# same digest-pinned image under `--platform linux/amd64`.
#
# The choice is PRINTED. A demo that quietly swapped its engine would be
# showing you a result about something other than what it claims to run.
export LOGWEIR_E2E_ENGINE_MOUNT="$PWD/.demo/tmp"
mkdir -p "$LOGWEIR_E2E_ENGINE_MOUNT"
if .engine/kafka-backup --version >/dev/null 2>&1; then
  export LOGWEIR_ENGINE_BIN="$PWD/.engine/kafka-backup"
  echo "    engine route: NATIVE $LOGWEIR_ENGINE_BIN"
else
  export LOGWEIR_ENGINE_BIN="$PWD/e2e/fixtures/engine-docker.sh"
  echo "    engine route: CONTAINER via $LOGWEIR_ENGINE_BIN"
  echo "                  (.engine/kafka-backup is a linux/amd64 ELF and cannot exec here)"
fi
# Read off the engine that will actually run, never hardcoded, so the value in
# the signed scorecard describes the binary that produced the restore.
LOGWEIR_ENGINE_VERSION=$("$LOGWEIR_ENGINE_BIN" --version | awk '{print $NF}')
LOGWEIR_ENGINE_DIGEST=$(tr -d '[:space:]' < third_party/kafka-backup-binary.digest)
export LOGWEIR_ENGINE_VERSION LOGWEIR_ENGINE_DIGEST
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1
export TMPDIR="$LOGWEIR_E2E_ENGINE_MOUNT"
echo "    engine: $LOGWEIR_ENGINE_VERSION $LOGWEIR_ENGINE_DIGEST"

# `sample.window_start` / `sample.window_end` are DEPLOYMENT-SPECIFIC: they name
# the point-in-time range you are drilling, and `examples/drill.yaml` can only
# carry an illustrative one. A window that overlaps no segment is REFUSED
# ("a drill over an empty window would report a pass that means nothing", exit
# 1) — correct behaviour, and a confusing first experience for a reader who
# just ran the seed. So the demo rebinds the window to the last 24 hours, which
# is where step 3 just put the records, and prints what it bound. This mirrors
# what `e2e/tests/harness/mod.rs::spec_example_with_only_the_window_bound` does
# for the e2e suite; it is the ONE field the demo overrides.
WINDOW_END=$("$PYTHON" -c 'import datetime;print(datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))')
WINDOW_START=$("$PYTHON" -c 'import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))')
sed -e "s|^\( *window_start:\).*|\1 \"$WINDOW_START\"|" \
    -e "s|^\( *window_end:\).*|\1   \"$WINDOW_END\"|" \
    examples/drill.yaml > .demo/drill.yaml
grep -q "$WINDOW_START" .demo/drill.yaml && grep -q "$WINDOW_END" .demo/drill.yaml || {
  echo "demo: failed to bind the sample window into .demo/drill.yaml" >&2; exit 1; }
echo "    sample window: $WINDOW_START .. $WINDOW_END  (the only field the demo overrides)"

cargo run --release -p logweir -- doctor \
  --spec .demo/drill.yaml --allowed-clusters .demo/allowed-clusters.json \
  --approver-key .demo/approver.pub.pem

echo "==> 5/6 approving the exact plan, then running the drill"
# The approval binds to the sha256 of the EXACT spec file `drill run` is given,
# so it hashes .demo/drill.yaml — the one with the bound window — never
# examples/drill.yaml. Approving one document and running another is precisely
# what plan_hash exists to catch, and it would be refused at phase 1 (exit 3).
./scripts/demo-approve.sh .demo/drill.yaml
cargo run --release -p logweir -- drill run \
  --spec .demo/drill.yaml \
  --approval .demo/approval.json --approver-key .demo/approver.pub.pem \
  --allowed-clusters .demo/allowed-clusters.json \
  --signing-key .demo/signer.pem \
  --out .demo/scorecard.json \
  --metrics-file .demo/logweir.prom \
  --triggered-by "logweir demo"

echo "==> 6/6 showing and verifying the scorecard, twice"
# --out writes .demo/scorecard.json and its DSSE sidecar beside it as
# .demo/scorecard.sig (Task 21a's --out + .sig rule).
cargo run --release -p logweir -- drill show .demo/scorecard.json
cargo run --release -p logweir -- drill verify \
  --scorecard .demo/scorecard.json --signature .demo/scorecard.sig \
  --public-key .demo/signer.pub.pem
"$PYTHON" docs/verify_scorecard.py .demo/scorecard.json .demo/scorecard.sig .demo/signer.pub.pem

# THE QUICKSTART MUST NOT MODIFY THE REPOSITORY. Everything this script writes
# goes to `.demo/` or `.engine/`, both gitignored. This checks it rather than
# claiming it, because "the demo leaves your tree alone" is exactly the kind of
# statement that quietly stops being true. It is a real check, not decoration:
# reverting the LOGWEIR_SEED_REFRESH_FIXTURES=0 above makes it fail.
if git -C . rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  dirty=$(git status --porcelain 2>/dev/null || true)
  if [ -n "$dirty" ]; then
    echo >&2
    echo "demo: WARNING — the working tree is not clean after this run:" >&2
    printf '%s\n' "$dirty" >&2
    echo "The demo writes only to .demo/ and .engine/, both gitignored, so this" >&2
    echo "is either a change you already had, or a defect. Please report it." >&2
  else
    echo
    echo "Working tree still clean: the demo wrote only to .demo/ and .engine/."
  fi
fi

echo
echo "Done. measured.rto_seconds and measured.rpo_seconds in .demo/scorecard.json are real numbers."
echo "The Prometheus textfile is at .demo/logweir.prom."
echo "Tear the stack down with: just e2e-down"
