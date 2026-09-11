#!/usr/bin/env bash
# `just k8s-demo` — **Phase B's exit criterion**, scripted. Task 24.
#
# ONE COMMAND from an empty docker-desktop cluster to a `Restore` object
# carrying `phase: Succeeded`, `exitCode: 0`, `outcome: pass` and
# `status.evidence.verification.result: Valid` with a `matchedKeyId` — plus a
# `Backup` carrying `exitCode: 0` and the same `Valid`. The transcript of the
# run that proved it is `e2e/k8s/phase-b-demo.md`.
#
# ===========================================================================
# THIS IS AUTHOR-ONLY, AND GLOBAL CONSTRAINT 37 IS NOT RELAXED BY IT
# ===========================================================================
# Two of its steps only work on the machine the images were built on:
#
#   * the `docker tag` step, which makes the SHIPPED
#     `ghcr.io/logweir/<name>@sha256:…` references resolvable on this node.
#     The kubelet keys on the WHOLE reference, so a matching digest under a
#     different repository name is `ErrImageNeverPull` (plan erratum E19(b),
#     `docs/kubernetes.md` §14.3). The tags this script adds it removes again.
#   * the `config/overlays/k8s-demo` patch, which points the controller at
#     `http://host.docker.internal:9000` — this laptop's compose stack.
#
# "Published" means a PULL from a registry the author does not control. This
# script proves the manifests are right and the binaries run; it says nothing
# about whether a stranger can install Logweir, and the install file's digest
# rows still read `blocked: no remote`.
#
# ===========================================================================
# WHERE THE OBJECT STORE IS — critique B **H20**, answered
# ===========================================================================
# Task 7 publishes the Kafka listener for pods (`K8S://host.docker.internal:9095`)
# and NOTHING publishes or addresses the object store. Without an answer the
# demo can neither back up nor restore, and `verify_evidence` has no bucket to
# read. The compose stack's MinIO publishes `9000:9000` already
# (`e2e/compose/docker-compose.yml`), so no compose change is needed —
# STANDING RULE 15's one-owner rule is not touched — and from a docker-desktop
# pod it is reachable as **`http://host.docker.internal:9000`** and only with
# an S3 endpoint override:
#
#   AWS_ENDPOINT_URL=http://host.docker.internal:9000
#   AWS_REGION=us-east-1
#   AWS_ALLOW_HTTP=true
#
# set on **both** the controller Deployment (through the demo overlay) and the
# runner Jobs — which the controller FORWARDS from its own environment
# (`weirkeeper::controllers::backup::ARCHIVE_ADDRESSING_ENV`), so the endpoint
# is configured once instead of in two places that can disagree. The archive is
# `s3://kafka-backups/k8s-demo`, and this script creates the bucket with `mc`
# INSIDE the compose network first: `just e2e-down` runs `down -v` and EMPTIES
# the MinIO volume (plan erratum E12(g)).
#
# ===========================================================================
# `just lint` AND THE COMPOSE STACK ARE TWO DIFFERENT MACHINE STATES
# ===========================================================================
# `scripts/time-unit-suite.sh` refuses to run, exit 1, while 9092 or 9000
# answers (Global Constraint 22), so **`just lint` runs at step 2, BEFORE the
# stack comes up at step 3, and never while it is up.** Check-then-take is
# STANDING RULE 3: step 3 confirms `docker ps` shows no `logweir-*` containers
# before taking the stack, and the cleanup at the end runs `just e2e-down`.
#
# ===========================================================================
# EXIT CODES ARE READ DIRECTLY, NEVER THROUGH A PIPE (STANDING RULE 20)
# ===========================================================================
#     set +e
#     kubectl … > "$OUT/something.out"
#     rc=$?
#     set -e
#
# — the command on its own line, its status on the very next one. The I29
# tokeniser (`crates/logweir/tests/support/exit_code_lint.rs`) runs over this
# file in the DEFAULT test suite from
# `crates/logweir/tests/k8s_demo_lint.rs` and fails naming any line that
# breaks the shape. Its guard is a LITERAL first-word rule — `kubectl`, `curl`,
# `logweir`, `docker`, `just` — so the `logweir` binary goes on `$PATH` through
# a one-entry shim directory and is written as the bare word, exactly as
# `scripts/mvp-demo.sh` does.
#
# STANDING RULE 13's T21–T24 exception: the control plane installs into the
# fixed namespace `logweir-system` and the custom resources go in
# `logweir-t24`; both are deleted at the end, and the run REFUSES to start
# unless `kubectl get crd` shows either zero or exactly the six CRDs this plan
# ships.
set -uo pipefail
cd "$(dirname "$0")/.."

CTX=docker-desktop
NS=logweir-t24
SYS=logweir-system
OUT=.demo/k8s
COMPOSE_FILE=e2e/compose/docker-compose.yml
BOOTSTRAP_INNET=kafka-broker-1:9094
BOOTSTRAP_K8S=host.docker.internal:9095
ARCHIVE_BUCKET=kafka-backups
BACKUP_PREFIX=k8s-demo
ARCHIVE_URL="s3://${ARCHIVE_BUCKET}/${BACKUP_PREFIX}"
S3_ENDPOINT=http://host.docker.internal:9000
S3_REGION=us-east-1
S3_ALLOW_HTTP=true
TOPIC=k8sdemo
RUNNER_IMAGE_TAG=ghcr.io/logweir/logweir:v0.1.0
CONTROLLER_IMAGE_TAG=ghcr.io/logweir/weirkeeper:v0.1.0

mkdir -p "$OUT"
die() { echo; echo "k8s-demo: $*" >&2; exit 1; }
step() { echo; echo "==> $*"; }

# ---------------------------------------------------------------------------
# 1/12 PRE-FLIGHT — the CRD list FIRST (STANDING RULE 13).
# ---------------------------------------------------------------------------
step "1/12 pre-flight: context, CRD list, tools, images"

for tool in docker kubectl awk; do
  command -v "$tool" >/dev/null 2>&1 || die "\`$tool\` is not on \$PATH."
done
PYTHON="${LOGWEIR_PYTHON:-python3}"
command -v "$PYTHON" >/dev/null 2>&1 || die "\`$PYTHON\` is not on \$PATH; set LOGWEIR_PYTHON."

set +e
crds=$(kubectl --context "$CTX" get crd -o name)
rc=$?
set -e
echo "    rc=$rc  (kubectl get crd -o name)"
[ "$rc" -eq 0 ] || die "the $CTX cluster is not reachable (rc=$rc). Enable Kubernetes in Docker Desktop."
# COUNTED WITHOUT A PIPE WHOSE STATUS MATTERS: the count is the value, and the
# `grep` that produces it is inside a substitution, not on a line whose exit
# code anything reads.
ours=$(printf '%s\n' "$crds" | grep -c 'logweir.dev' || true)
echo "    logweir.dev CRDs already installed: $ours"
if [ "$ours" -ne 0 ] && [ "$ours" -ne 6 ]; then
  die "the cluster holds $ours logweir.dev CRDs. STANDING RULE 13 permits 0 (a clean cluster) or exactly 6 (this plan's own install) and nothing between: a partial set means another agent owns this cluster, or a previous run died halfway."
fi

set +e
docker image inspect logweir:check > "$OUT/img-runner.json" 2>&1
rc=$?
set -e
echo "    rc=$rc  (docker image inspect logweir:check)"
[ "$rc" -eq 0 ] || die "no local \`logweir:check\` image. Build it with \`just image\` — and note that a rebuild CHANGES ITS DIGEST, which would then no longer match \`weirkeeper::job::RUNNER_IMAGE\` (plan erratum E19a)."
set +e
docker image inspect weirkeeper:check > "$OUT/img-controller.json" 2>&1
rc=$?
set -e
echo "    rc=$rc  (docker image inspect weirkeeper:check)"
[ "$rc" -eq 0 ] || die "no local \`weirkeeper:check\` image. Build it with \`just image-weirkeeper\` — same digest caveat."

LOGWEIR_BIN="${LOGWEIR_BIN:-$PWD/target/debug/logweir}"
[ -x "$LOGWEIR_BIN" ] || die "no logweir binary at $LOGWEIR_BIN — run \`cargo build -p logweir\` (\`just k8s-demo\` does it for you)."
mkdir -p "$OUT/bin"
ln -sf "$LOGWEIR_BIN" "$OUT/bin/logweir"
PATH="$PWD/$OUT/bin:$PATH"
export PATH

# ---------------------------------------------------------------------------
# 2/12 LINT — WITH THE STACK DOWN, AND BEFORE IT COMES UP.
# ---------------------------------------------------------------------------
step "2/12 just lint, with the compose stack DOWN (Global Constraint 22)"

set +e
just lint > "$OUT/lint.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (just lint)"
[ "$rc" -eq 0 ] || die "\`just lint\` exited $rc with the stack down; see $OUT/lint.log. This runs FIRST on purpose: scripts/time-unit-suite.sh refuses, exit 1, while 9092 or 9000 answers, so there is no later point in this script at which it could run."

# ---------------------------------------------------------------------------
# 3/12 CHECK-THEN-TAKE, THEN THE STACK (STANDING RULE 3).
# ---------------------------------------------------------------------------
step "3/12 check-then-take the compose stack, then just e2e-up"

set +e
docker ps --filter "name=logweir-" --format "{{.Names}}" > "$OUT/docker-ps.txt"
rc=$?
set -e
echo "    rc=$rc  (docker ps --filter name=logweir-)"
[ "$rc" -eq 0 ] || die "\`docker ps\` exited $rc. Is Docker running?"
existing=$(awk 'NF' "$OUT/docker-ps.txt" | wc -l | tr -d ' ')
[ "$existing" -eq 0 ] || die "$existing logweir-* container(s) are already running, so somebody else owns the compose stack (STANDING RULE 3). Wait for them, or run \`just e2e-down\` if you know it is yours:
$(cat "$OUT/docker-ps.txt")"
echo "    no logweir-* containers: the stack is free to take"

set +e
just e2e-up > "$OUT/e2e-up.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (just e2e-up)"
[ "$rc" -eq 0 ] || die "\`just e2e-up\` exited $rc; see $OUT/e2e-up.log"

# ---------------------------------------------------------------------------
# 4/12 THE BUCKET, INSIDE THE COMPOSE NETWORK, BEFORE ANY CUSTOM RESOURCE.
# ---------------------------------------------------------------------------
step "4/12 creating $ARCHIVE_BUCKET/$BACKUP_PREFIX with mc, inside the compose network"

# `just e2e-down` runs `down -v` and EMPTIES the MinIO volume (plan erratum
# E12g), so every owner of the stack makes its own bucket. `mc mb --ignore-existing`
# because `minio-setup` may have made it already and "it is already there" is
# success, not a failure.
#
# `docker compose run --rm` starts a FRESH container each time, so an `mc alias
# set` in one invocation is gone by the next; the alias comes from
# MC_HOST_local on the minio-setup service.
set +e
docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint mc minio-setup mb --ignore-existing "local/$ARCHIVE_BUCKET" > "$OUT/mc-mb.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (mc mb local/$ARCHIVE_BUCKET)"
[ "$rc" -eq 0 ] || die "could not create the bucket (rc=$rc); see $OUT/mc-mb.log"

# SWEPT AT BOTH ENDS, for the reason `scripts/mvp-demo.sh` gives: a second
# backup into a colliding prefix does not accumulate, and
# `harness::corrupt_a_non_oldest_segment` takes the LAST key in sort order out
# of this shared bucket.
sweep_archive() {
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint mc minio-setup rm --recursive --force "local/$ARCHIVE_BUCKET/$BACKUP_PREFIX/" > "$OUT/sweep-archive.log" 2>&1
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || echo "    (nothing to sweep under $ARCHIVE_BUCKET/$BACKUP_PREFIX/)"
  set +e
  docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint mc minio-setup rm --recursive --force "local/$ARCHIVE_BUCKET/logweir/" > "$OUT/sweep-evidence.log" 2>&1
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || echo "    (nothing to sweep under $ARCHIVE_BUCKET/logweir/)"
}
sweep_archive

# ---------------------------------------------------------------------------
# 5/12 RECORDS, AND THE RECOVERY POINT.
# ---------------------------------------------------------------------------
step "5/12 producing records into $TOPIC and computing the recovery point"

set +e
docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint kafka-topics -e KAFKA_OPTS= topic-setup --bootstrap-server "$BOOTSTRAP_INNET" --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1 > "$OUT/topic-create.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kafka-topics --create $TOPIC)"
[ "$rc" -eq 0 ] || die "could not create the topic (rc=$rc); see $OUT/topic-create.log"

RECORDS="${RECORDS:-200}"
# awk generates the range directly: BSD `seq` counts DOWN when first > last.
awk -v n="$RECORDS" 'BEGIN { for (i = 1; i <= n; i++) printf "k-%06d:{\"id\":%d}\n", i, i }' > "$OUT/records.txt"
set +e
docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint kafka-console-producer -e KAFKA_OPTS= topic-setup --bootstrap-server "$BOOTSTRAP_INNET" --topic "$TOPIC" --property "parse.key=true" --property "key.separator=:" < "$OUT/records.txt" > "$OUT/produce.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kafka-console-producer -> $TOPIC)"
[ "$rc" -eq 0 ] || die "producing exited $rc; see $OUT/produce.log"

# The producer exits 0 whether or not the broker accepted anything, so the end
# offsets are read back OFF THE BROKER.
set +e
docker compose -f "$COMPOSE_FILE" run --rm -T --entrypoint kafka-get-offsets -e KAFKA_OPTS= topic-setup --bootstrap-server "$BOOTSTRAP_INNET" --topic "$TOPIC" > "$OUT/offsets.txt" 2> "$OUT/offsets.stderr"
rc=$?
set -e
echo "    rc=$rc  (kafka-get-offsets $TOPIC)"
[ "$rc" -eq 0 ] || die "could not read end offsets (rc=$rc); see $OUT/offsets.stderr"
ON_BROKER=$(awk -F: '{ s += $3 } END { print s+0 }' "$OUT/offsets.txt")
[ "$ON_BROKER" -gt 0 ] || die "the broker holds 0 records on $TOPIC; the archive would be empty"
echo "    $TOPIC holds $ON_BROKER records"

# Plan errata E6/E7: every epoch instant in this project is COMPUTED, never
# typed. `point_in_time` is five seconds after the last record (the boundary is
# inclusive — guard G-PITR) and strictly above the archive floor.
read -r PIT_RFC3339 SAMPLE_START_RFC3339 <<EOF
$("$PYTHON" -c 'import datetime as d
now = d.datetime.now(d.timezone.utc).replace(microsecond=0)
pit = now + d.timedelta(seconds=5)
start = pit - d.timedelta(hours=1)
print(pit.strftime("%Y-%m-%dT%H:%M:%SZ"), start.strftime("%Y-%m-%dT%H:%M:%SZ"))')
EOF
[ -n "${PIT_RFC3339:-}" ] || die "could not compute the recovery point"
echo "    recovery point: $PIT_RFC3339   sample window from: $SAMPLE_START_RFC3339"

# ---------------------------------------------------------------------------
# 6/12 KEYS — TWO OF THEM, AND ONLY PUBLIC HALVES LEAVE THIS DIRECTORY.
# ---------------------------------------------------------------------------
step "6/12 minting the signing and approver keys"

if [ ! -f "$OUT/signing.pem" ]; then
  openssl ecparam -genkey -name prime256v1 -noout > "$OUT/signing.der.pem"
  openssl pkcs8 -topk8 -nocrypt -in "$OUT/signing.der.pem" -out "$OUT/signing.pem"
  openssl ecparam -genkey -name prime256v1 -noout > "$OUT/approver.der.pem"
  openssl pkcs8 -topk8 -nocrypt -in "$OUT/approver.der.pem" -out "$OUT/approver.pem"
  rm -f "$OUT/signing.der.pem" "$OUT/approver.der.pem"
fi
openssl ec -in "$OUT/signing.pem"  -pubout -out "$OUT/signing.pub.pem"  2>/dev/null
openssl ec -in "$OUT/approver.pem" -pubout -out "$OUT/approver.pub.pem" 2>/dev/null
chmod 600 "$OUT/signing.pem" "$OUT/approver.pem"

# The `keyId` a roster entry declares, and the id `matchedKeyId` reports, is
# `VerifyingKey::key_id()` — the sha256 of the SPKI **DER**, hex. Computed with
# openssl so this script and `logweir-verify` cannot disagree about it.
SIGNING_KEY_ID=$(openssl ec -pubin -in "$OUT/signing.pub.pem" -outform DER 2>/dev/null | openssl dgst -sha256 -hex | awk '{print $NF}')
APPROVER_KEY_ID=$(openssl ec -pubin -in "$OUT/approver.pub.pem" -outform DER 2>/dev/null | openssl dgst -sha256 -hex | awk '{print $NF}')
[ -n "$SIGNING_KEY_ID" ] || die "could not compute the signing key id"
echo "    signing key id:  $SIGNING_KEY_ID"
echo "    approver key id: $APPROVER_KEY_ID"
echo "    WARNING: both were minted on this machine, minutes ago, and are attested by nothing."

# ---------------------------------------------------------------------------
# 7/12 THE AUTHOR-ONLY IMAGE STEP (plan erratum E19b).
# ---------------------------------------------------------------------------
step "7/12 tagging the local images with the SHIPPED repository names (author-only)"

# `docs/kubernetes.md` §14.3, measured: the kubelet keys on the WHOLE
# reference, so `ghcr.io/logweir/logweir@sha256:<d>` is `ErrImageNeverPull` on
# a node that holds the same digest under the local name `logweir:check`. One
# `docker tag` makes the shipped reference resolve. THE TAGS ARE REMOVED AT THE
# END, by `cleanup`, so this script leaves the registry as it found it.
set +e
docker tag logweir:check "$RUNNER_IMAGE_TAG"
rc=$?
set -e
echo "    rc=$rc  (docker tag logweir:check $RUNNER_IMAGE_TAG)"
[ "$rc" -eq 0 ] || die "could not tag the runner image (rc=$rc)"
set +e
docker tag weirkeeper:check "$CONTROLLER_IMAGE_TAG"
rc=$?
set -e
echo "    rc=$rc  (docker tag weirkeeper:check $CONTROLLER_IMAGE_TAG)"
[ "$rc" -eq 0 ] || die "could not tag the controller image (rc=$rc)"

set +e
docker inspect --format '{{json .RepoDigests}}' "$RUNNER_IMAGE_TAG" > "$OUT/runner-digests.json"
rc=$?
set -e
echo "    rc=$rc  (docker inspect RepoDigests $RUNNER_IMAGE_TAG)"
cat "$OUT/runner-digests.json"

cleanup() {
  echo
  echo "==> 12/12 cleanup"
  set +e
  kubectl --context "$CTX" delete -f logweir.yaml --ignore-not-found > "$OUT/cleanup-install.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete -f logweir.yaml)"
  set +e
  kubectl --context "$CTX" delete ns "$SYS" "$NS" --ignore-not-found > "$OUT/cleanup-ns.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete ns $SYS $NS)"
  sweep_archive
  set +e
  docker rmi "$RUNNER_IMAGE_TAG" "$CONTROLLER_IMAGE_TAG" > "$OUT/cleanup-tags.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (docker rmi the two author-only tags)"
  set +e
  just e2e-down > "$OUT/cleanup-e2e-down.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (just e2e-down)"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 8/12 THE INSTALL FILE, PLUS THE DEMO'S OWN OVERLAY.
# ---------------------------------------------------------------------------
step "8/12 applying logweir.yaml and the author-only k8s-demo overlay"

set +e
kubectl --context "$CTX" apply --server-side -f logweir.yaml > "$OUT/apply-install.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl apply --server-side -f logweir.yaml)"
[ "$rc" -eq 0 ] || die "applying the install file exited $rc; see $OUT/apply-install.log"

# THE DEMO'S S3 LITERALS LIVE HERE AND NOT IN THE SHIPPED FILE. A stranger
# must be able to apply `logweir.yaml` unedited (Global Constraint 37), so the
# endpoint, the region and the allow-http flag are a kustomize patch this
# script applies on top — and the controller forwards all three to every runner
# Job it creates.
set +e
kubectl --context "$CTX" -n "$SYS" patch deployment weirkeeper --patch-file config/overlays/k8s-demo/deployment-env-patch.yaml > "$OUT/apply-overlay.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl patch deployment weirkeeper --patch-file config/overlays/k8s-demo/deployment-env-patch.yaml)"
[ "$rc" -eq 0 ] || die "applying the demo patch exited $rc; see $OUT/apply-overlay.log"

set +e
kubectl --context "$CTX" -n "$SYS" rollout status deploy/weirkeeper --timeout=180s > "$OUT/rollout.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl rollout status deploy/weirkeeper)"
[ "$rc" -eq 0 ] || die "the controller did not become ready (rc=$rc); see $OUT/rollout.log and \`kubectl -n $SYS describe pod\`"

# ---------------------------------------------------------------------------
# 9/12 THE NAMESPACE, THE FIVE SECRETS AND THE ROSTER.
# ---------------------------------------------------------------------------
step "9/12 namespace $NS, the runner ServiceAccount, the five Secrets and the TrustRoster"

set +e
kubectl --context "$CTX" create namespace "$NS" > "$OUT/ns.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl create namespace $NS)"
[ "$rc" -eq 0 ] || die "could not create $NS (rc=$rc); see $OUT/ns.log"

# The runner ServiceAccount lives in the namespace of the Backup/Restore
# objects, so it is NOT in logweir.yaml (plan erratum E14c).
set +e
kubectl --context "$CTX" -n "$NS" apply -f config/rbac/backup-runner-serviceaccount.yaml > "$OUT/runner-sa.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl apply -f config/rbac/backup-runner-serviceaccount.yaml)"
[ "$rc" -eq 0 ] || die "could not apply the runner ServiceAccount (rc=$rc)"

# 1. the runner's signing key.
set +e
kubectl --context "$CTX" -n "$NS" create secret generic logweir-signing-key --from-file=signing.pem="$OUT/signing.pem" > "$OUT/secret-signing.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (secret/logweir-signing-key)"
[ "$rc" -eq 0 ] || die "could not create logweir-signing-key (rc=$rc)"

# 4. the runner's archive credential.
set +e
kubectl --context "$CTX" -n "$NS" create secret generic logweir-s3 --from-literal=access-key-id=minioadmin --from-literal=secret-access-key=minioadmin > "$OUT/secret-s3.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (secret/logweir-s3)"
[ "$rc" -eq 0 ] || die "could not create logweir-s3 (rc=$rc)"

# 5. the CONTROLLER's read-only evidence credential — a DIFFERENT PRINCIPAL,
# in logweir-system, and the one this whole task is about. MinIO gives no
# second identity out of the box, so the demo uses the same key pair with a
# different Secret name and a different namespace: the SEPARATION THIS PROVES
# IS THE PROJECTION, not the IAM policy. An adopter gives this principal
# `s3:GetObject` on the evidence prefix and nothing else.
set +e
kubectl --context "$CTX" -n "$SYS" create secret generic logweir-evidence-ro --from-literal=access-key-id=minioadmin --from-literal=secret-access-key=minioadmin > "$OUT/secret-evidence.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (secret/logweir-evidence-ro in $SYS)"
[ "$rc" -eq 0 ] || die "could not create logweir-evidence-ro (rc=$rc)"

# The controller reads that Secret from its OWN ENV, so it has to restart to
# see it (env is fixed at container start).
set +e
kubectl --context "$CTX" -n "$SYS" rollout restart deploy/weirkeeper > "$OUT/restart.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl rollout restart deploy/weirkeeper)"
[ "$rc" -eq 0 ] || die "could not restart the controller (rc=$rc)"
set +e
kubectl --context "$CTX" -n "$SYS" rollout status deploy/weirkeeper --timeout=180s > "$OUT/rollout2.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl rollout status deploy/weirkeeper, after the evidence Secret)"
[ "$rc" -eq 0 ] || die "the controller did not come back (rc=$rc)"

# The roster — interface I16, name `default`, cluster-scoped. `signingKeys[]`
# carries the runner's PUBLIC key: with a `signingKeyIds: [string]` shape there
# would be nothing to verify against and this demo could not reach `Valid`.
"$PYTHON" - "$OUT/signing.pub.pem" "$SIGNING_KEY_ID" "$OUT/approver.pub.pem" "$APPROVER_KEY_ID" "$OUT/trustroster.yaml" <<'PY'
import sys, textwrap
sig_pem, sig_id, app_pem, app_id, out = sys.argv[1:6]
def block(path):
    return textwrap.indent(open(path).read().rstrip("\n"), " " * 8)
doc = f"""apiVersion: logweir.dev/v1alpha1
kind: TrustRoster
metadata:
  name: default
spec:
  allowedClusterIds: []
  approverKeys:
    - keyId: {app_id}
      subject: k8s-demo-approver@example.invalid
      spkiPem: |
{block(app_pem)}
  signingKeys:
    - keyId: {sig_id}
      subject: logweir-runner@example.invalid
      spkiPem: |
{block(sig_pem)}
"""
open(out, "w").write(doc)
PY
set +e
kubectl --context "$CTX" apply -f "$OUT/trustroster.yaml" > "$OUT/roster.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl apply -f trustroster.yaml)"
[ "$rc" -eq 0 ] || die "could not apply the TrustRoster (rc=$rc); see $OUT/roster.log"

# ---------------------------------------------------------------------------
# 10/12 THE KafkaCluster, AND `reachable: true` OFF THE PROBE (interface I14).
# ---------------------------------------------------------------------------
step "10/12 KafkaCluster at $BOOTSTRAP_K8S -> status.reachable"

cat > "$OUT/kafkacluster.yaml" <<YAML
apiVersion: logweir.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: demo
  namespace: $NS
spec:
  bootstrapServers: ["$BOOTSTRAP_K8S"]
  auth:
    mode: plaintext
  role: source
YAML
set +e
kubectl --context "$CTX" apply -f "$OUT/kafkacluster.yaml" > "$OUT/kc-apply.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl apply -f kafkacluster.yaml)"
[ "$rc" -eq 0 ] || die "could not apply the KafkaCluster (rc=$rc)"

reachable=""
for _ in $(seq 1 60); do
  set +e
  reachable=$(kubectl --context "$CTX" -n "$NS" get kafkacluster demo -o jsonpath='{.status.reachable}')
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || reachable=""
  [ "$reachable" = "true" ] && break
  sleep 5
done
echo "    rc=$rc  (kubectl get kafkacluster demo -o jsonpath={.status.reachable})"
echo "    reachable: $reachable"
[ "$reachable" = "true" ] || die "the KafkaCluster never became reachable. The probe Job runs \`logweir cluster-probe\` against $BOOTSTRAP_K8S; check \`kubectl -n $NS get jobs\` and the probe pod's log."

# ---------------------------------------------------------------------------
# 11/12 THE Backup, THEN THE APPROVED Restore.
# ---------------------------------------------------------------------------
step "11/12 a Backup, then an approved Restore"

# The runner argv travels on the annotation (`weirkeeper::controllers::
# backup_schedule::RUNNER_ARGV_ANNOTATION`) — `Backup.spec` carries none and
# is sealed by CEL, so a hand-written Backup supplies it. No
# `--backup-id-override`: without it the run uses the `backup_id` the
# controller renders into the plan ConfigMap.
cat > "$OUT/backup.yaml" <<'YAML'
apiVersion: logweir.dev/v1alpha1
kind: Backup
metadata:
  name: demo-backup
  namespace: NAMESPACE
  annotations:
    logweir.dev/runner-argv: '["backup","run","--spec","/plan/backup.yaml","--allowed-clusters","/plan/allowed-clusters.json","--signing-key","/signing/key.pem","--out","/work/backup.json","--receipt-out","/work/receipt.json","--triggered-by","manual"]'
spec:
  sourceRef:
    name: demo
  topics: ["TOPIC"]
  archive:
    url: ARCHIVE_URL
    secretRef:
      name: logweir-s3
  triggeredBy: manual
  deadlineSeconds: 1800
YAML
sed -i.bak -e "s|NAMESPACE|$NS|" -e "s|\"TOPIC\"|\"$TOPIC\"|" -e "s|ARCHIVE_URL|$ARCHIVE_URL|" "$OUT/backup.yaml"
rm -f "$OUT/backup.yaml.bak"
set +e
kubectl --context "$CTX" apply -f "$OUT/backup.yaml" > "$OUT/backup-apply.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl apply -f backup.yaml)"
[ "$rc" -eq 0 ] || die "could not apply the Backup (rc=$rc)"

backup_exit=""
for _ in $(seq 1 90); do
  set +e
  backup_exit=$(kubectl --context "$CTX" -n "$NS" get backup demo-backup -o jsonpath='{.status.exitCode}')
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || backup_exit=""
  [ -n "$backup_exit" ] && break
  sleep 10
done
echo "    rc=$rc  (kubectl get backup demo-backup -o jsonpath={.status.exitCode})"
echo "    exitCode: $backup_exit"
[ "$backup_exit" = "0" ] || die "the Backup did not exit 0 (got '${backup_exit:-<absent>}'). Read it with: kubectl --context $CTX -n $NS get backup demo-backup -o yaml"

backup_verdict=""
for _ in $(seq 1 24); do
  set +e
  backup_verdict=$(kubectl --context "$CTX" -n "$NS" get backup demo-backup -o jsonpath='{.status.evidence.verification.result}')
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || backup_verdict=""
  [ -n "$backup_verdict" ] && [ "$backup_verdict" != "NotAttempted" ] && break
  sleep 5
done
echo "    rc=$rc  (kubectl get backup demo-backup -o jsonpath={.status.evidence.verification.result})"
echo "    verification.result: $backup_verdict"
[ "$backup_verdict" = "Valid" ] || die "the Backup's receipt did not verify (got '${backup_verdict:-<absent>}'). Read the detail with: kubectl --context $CTX -n $NS get backup demo-backup -o jsonpath='{.status.evidence.verification.detail}'"

# ---- the restore plan, and an approval over its EXACT bytes ----------------
cat > "$OUT/restore-plan.yaml" <<YAML
source:
  storage:
    backend: s3
    bucket: $ARCHIVE_BUCKET
    prefix: $BACKUP_PREFIX
    region: $S3_REGION
    endpoint: $S3_ENDPOINT
    path_style: true
    allow_http: true
  backup: latestCompleted
  topics: [$TOPIC]
target:
  bootstrap_servers: [$BOOTSTRAP_K8S]
  mode: newTopic
  topic_mapping_prefix: "drill-"
  marker_topic: logweir.scratch
  default_replication_factor: 1
  teardown: delete
restore:
  point_in_time: "$PIT_RFC3339"
sample:
  window_start: "$SAMPLE_START_RFC3339"
  window_end:   "$PIT_RFC3339"
  records_per_partition: 25
  anchor: head
objectives:
  rto_seconds: 3600
  rpo_seconds: 86400
  pass_rate: 1.0
# THE EVIDENCE BUCKET IS THE ARCHIVE BUCKET, AND THAT IS DELIBERATE.
# \`logweir backup run\` puts its receipt through \`evidence_location(spec.storage)\`
# — the archive's own backend and bucket, prefix \`logweir/\` (Global Constraint
# 6) — and it has no field to point elsewhere. The controller holds ONE
# read-only handle, built from \`LOGWEIR_ARCHIVE_URL\`, and \`Store::get\` takes a
# BUCKET-RELATIVE key. So a restore writing its scorecard into
# \`logweir-evidence\` (what \`examples/restore.yaml\` does) would put one of the
# two documents in a bucket that handle cannot see, and its verification would
# read \`NotAttempted\` — a truthful verdict about a misconfiguration, and not
# the one this demo is here to show.
evidence:
  backend: s3
  bucket: $ARCHIVE_BUCKET
  prefix: logweir/
  region: $S3_REGION
  endpoint: $S3_ENDPOINT
  path_style: true
  allow_http: true
notifications:
  webhooks: []
YAML

set +e
logweir drill approve --spec "$OUT/restore-plan.yaml" --key "$OUT/approver.pem" --approver k8s-demo --ticket DEMO-24 --subject-kind Restore --out "$OUT/approval.json"
rc=$?
set -e
echo "    rc=$rc  (logweir drill approve --subject-kind Restore)"
[ "$rc" -eq 0 ] || die "\`logweir drill approve\` exited $rc"

printf '{"allowed_cluster_ids":["SCRATCH-CLUSTER-NOT-THE-SOURCE"],"source_cluster_id":null}\n' > "$OUT/allowed-clusters.json"

# 2. the approval bundle — four keys at /approval, a Secret since Task 22.
set +e
kubectl --context "$CTX" -n "$NS" create secret generic logweir-approval-bundle --from-file=approval.json="$OUT/approval.json" --from-file=approval.sig="$OUT/approval.sig" --from-file=approver.pub.pem="$OUT/approver.pub.pem" --from-file=allowed-clusters.json="$OUT/allowed-clusters.json" > "$OUT/secret-approval.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (secret/logweir-approval-bundle)"
[ "$rc" -eq 0 ] || die "could not create logweir-approval-bundle (rc=$rc); see $OUT/secret-approval.log"

# 3. the per-cluster SCRAM credential. The demo's broker listener is
# PLAINTEXT, so no runner reads this one — it is created because `just
# check-secrets` counts five and an install that is missing one is exactly what
# that check exists to catch.
set +e
kubectl --context "$CTX" -n "$NS" create secret generic kafka-scram --from-literal=password=unused-on-the-plaintext-listener > "$OUT/secret-scram.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (secret/kafka-scram)"
[ "$rc" -eq 0 ] || die "could not create kafka-scram (rc=$rc)"

set +e
just check-secrets "$NS" > "$OUT/check-secrets.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (just check-secrets $NS)"
[ "$rc" -eq 0 ] || die "\`just check-secrets $NS\` exited $rc; see $OUT/check-secrets.log"

# The Restore, and the Approval over the SAME bytes. `planBytes` is opaque and
# the approval's `plan_hash` binds it, so the two documents below carry the
# same file.
"$PYTHON" - "$OUT/restore-plan.yaml" "$OUT/approval.json" "$NS" "$OUT/restore.yaml" "$PIT_RFC3339" <<'PY'
import json, sys, textwrap
plan_path, approval_path, ns, out, pit = sys.argv[1:6]
plan = open(plan_path).read()
approval = json.load(open(approval_path))
sidecar = open(approval_path.replace(".json", ".sig")).read()
approval_text = open(approval_path).read()
doc = f"""apiVersion: logweir.dev/v1alpha1
kind: Approval
metadata:
  name: demo-approval
  namespace: {ns}
spec:
  subjectRef:
    kind: Restore
    name: demo-restore
  planHash: {approval['plan_hash']}
  approvalBytes: |
{textwrap.indent(approval_text.rstrip(chr(10)), ' ' * 4)}
  sidecarBytes: |
{textwrap.indent(sidecar.rstrip(chr(10)), ' ' * 4)}
---
apiVersion: logweir.dev/v1alpha1
kind: Restore
metadata:
  name: demo-restore
  namespace: {ns}
spec:
  planBytes: |
{textwrap.indent(plan, ' ' * 4)}
  approvalRef:
    name: demo-approval
  sourceArchive:
    url: s3://kafka-backups/k8s-demo
    secretRef:
      name: logweir-s3
  backupSetRef: latestCompleted
  # UNREAD BY THE RECONCILER ON THIS PATH: the plan document's own
  # `source.backup: latestCompleted` is what the runner resolves. The field is
  # required by the schema, so it carries the same word rather than a made-up
  # backup id that would disagree with the approved bytes.
  pointInTime: "{pit}"
  target:
    clusterRef:
      name: demo
    mode: newTopic
    topicNaming:
      prefix: "drill-"
  deadlineSeconds: 1800
"""
open(out, "w").write(doc)
PY
set +e
kubectl --context "$CTX" apply -f "$OUT/restore.yaml" > "$OUT/restore-apply.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl apply -f restore.yaml)"
[ "$rc" -eq 0 ] || die "could not apply the Approval + Restore (rc=$rc); see $OUT/restore-apply.log"

restore_phase=""
for _ in $(seq 1 90); do
  set +e
  restore_phase=$(kubectl --context "$CTX" -n "$NS" get restore demo-restore -o jsonpath='{.status.phase}')
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || restore_phase=""
  case "$restore_phase" in Succeeded|Failed|Refused) break ;; esac
  sleep 10
done
echo "    rc=$rc  (kubectl get restore demo-restore -o jsonpath={.status.phase})"
echo "    phase: $restore_phase"
[ "$restore_phase" = "Succeeded" ] || die "the Restore reached phase '${restore_phase:-<absent>}'. Read it with: kubectl --context $CTX -n $NS get restore demo-restore -o yaml"

restore_verdict=""
for _ in $(seq 1 24); do
  set +e
  restore_verdict=$(kubectl --context "$CTX" -n "$NS" get restore demo-restore -o jsonpath='{.status.evidence.verification.result}')
  rc=$?
  set -e
  [ "$rc" -eq 0 ] || restore_verdict=""
  [ -n "$restore_verdict" ] && [ "$restore_verdict" != "NotAttempted" ] && break
  sleep 5
done
echo "    rc=$rc  (kubectl get restore demo-restore -o jsonpath={.status.evidence.verification.result})"
echo "    verification.result: $restore_verdict"

# ---------------------------------------------------------------------------
# 12/12 THE FOUR FIELDS, THE TWO FIELDS, AND BOTH VERIFICATION BLOCKS.
# ---------------------------------------------------------------------------
step "12/12 reading every field with -o jsonpath (STANDING RULE 20)"

read_field() {  # kind name jsonpath label
  set +e
  value=$(kubectl --context "$CTX" -n "$NS" get "$1" "$2" -o jsonpath="$3")
  rc=$?
  set -e
  echo "    rc=$rc  $4: ${value:-<absent>}"
  printf '%s' "$value"
}

echo "  Backup demo-backup:"
b_exit=$(read_field backup demo-backup '{.status.exitCode}' 'status.exitCode')
b_result=$(read_field backup demo-backup '{.status.evidence.verification.result}' 'verification.result')
b_key=$(read_field backup demo-backup '{.status.evidence.verification.matchedKeyId}' 'verification.matchedKeyId')
b_at=$(read_field backup demo-backup '{.status.evidence.verification.verifiedAt}' 'verification.verifiedAt')
b_receipt=$(read_field backup demo-backup '{.status.evidence.receiptKey}' 'evidence.receiptKey')

echo "  Restore demo-restore:"
r_phase=$(read_field restore demo-restore '{.status.phase}' 'status.phase')
r_exit=$(read_field restore demo-restore '{.status.exitCode}' 'status.exitCode')
r_outcome=$(read_field restore demo-restore '{.status.outcome}' 'status.outcome')
r_result=$(read_field restore demo-restore '{.status.evidence.verification.result}' 'verification.result')
r_key=$(read_field restore demo-restore '{.status.evidence.verification.matchedKeyId}' 'verification.matchedKeyId')
r_scorecard=$(read_field restore demo-restore '{.status.evidence.scorecardKey}' 'evidence.scorecardKey')
r_preflight=$(read_field restore demo-restore '{.status.topicPreflight.timestampType}' 'topicPreflight.timestampType')

set +e
kubectl --context "$CTX" -n "$NS" get backup demo-backup -o jsonpath='{.status.evidence}' > "$OUT/backup-evidence.json"
rc=$?
set -e
echo "    rc=$rc  (backup .status.evidence)"
cat "$OUT/backup-evidence.json"; echo
set +e
kubectl --context "$CTX" -n "$NS" get restore demo-restore -o jsonpath='{.status.evidence}' > "$OUT/restore-evidence.json"
rc=$?
set -e
echo "    rc=$rc  (restore .status.evidence)"
cat "$OUT/restore-evidence.json"; echo

set +e
kubectl --context "$CTX" -n "$SYS" logs deploy/weirkeeper --tail=80 > "$OUT/controller.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl logs deploy/weirkeeper --tail=80)"

# THE EXIT CRITERION, ASSERTED.
[ "$b_exit" = "0" ]            || die "Backup exitCode is '$b_exit', not 0"
[ "$b_result" = "Valid" ]      || die "Backup verification.result is '$b_result', not Valid"
[ -n "$b_key" ]                || die "Backup verification has no matchedKeyId"
[ "$r_phase" = "Succeeded" ]   || die "Restore phase is '$r_phase', not Succeeded"
[ "$r_exit" = "0" ]            || die "Restore exitCode is '$r_exit', not 0"
[ "$r_outcome" = "pass" ]      || die "Restore outcome is '$r_outcome', not pass"
[ "$r_result" = "Valid" ]      || die "Restore verification.result is '$r_result', not Valid"
[ -n "$r_key" ]                || die "Restore verification has no matchedKeyId"

echo
echo "==> PHASE B EXIT CRITERION MET"
echo "    Backup  demo-backup:  exitCode=$b_exit  verification=$b_result  key=$b_key  at=$b_at"
echo "                          receiptKey=$b_receipt"
echo "    Restore demo-restore: phase=$r_phase  exitCode=$r_exit  outcome=$r_outcome"
echo "                          verification=$r_result  key=$r_key"
echo "                          scorecardKey=$r_scorecard  topicPreflight.timestampType=${r_preflight:-<absent>}"
echo "    Both verified by weirkeeper with the read-only logweir-evidence-ro credential,"
echo "    against the runner's public key on the cluster-scoped TrustRoster 'default'."
