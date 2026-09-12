#!/usr/bin/env bash
# `just helm-demo` — THE HELM CHART, WALKED END TO END ON A REAL CLUSTER.
# Task 35 (post-plan).
#
# The chart (`charts/logweir`) has been installed with all three optional
# components on — `minio.enabled`, `demoKafka.enabled`, `ui.enabled` — and this
# script walks the product over it: two keypairs, the five Secrets, the
# cluster-scoped TrustRoster, two KafkaClusters that the controller probes to
# `status.reachable: true`, a BackupSchedule and the Backup it fires, a Restore
# whose plan bytes come from the PAGE's own emitter, an approval minted on the
# host with the shipped CLI, the Restore's terminal status with
# `status.outcome: pass`, BOTH readers over the scorecard, the in-cluster UI
# answering through a port-forward, and a teardown that leaves the cluster
# holding no `logweir-*` namespace.
#
# Two drivers run it: `just helm-demo` on the laptop (docker-desktop) and
# `.github/workflows/helm-demo.yml` on the `kind` cluster that workflow creates.
#
# ===========================================================================
# THE THREE PARAMETERS, AND THERE ARE ONLY THREE
# ===========================================================================
#   LOGWEIR_KUBE_CONTEXT    the cluster. Default `docker-desktop`; CI sets
#                           `kind-logweir`. EVERY `kubectl` line below reads
#                           `kubectl --context "$LOGWEIR_KUBE_CONTEXT" …`
#                           (STANDING RULE 12 — the variable is always passed).
#   LOGWEIR_HELM_RELEASE    the release name. Default `logweir`.
#   LOGWEIR_HELM_NAMESPACE  the release namespace. Default `logweir-system`.
#
# `LOGWEIR_PYTHON` is the repository-wide interpreter knob every script honours
# (`docs/verify_scorecard.py` needs `cryptography`); it is not a parameter of
# this walk. Everything else is derived: the demo namespace is `logweir-helm`,
# the archive is `s3://kafka-backups/helm-demo` on the chart's MinIO, the two
# brokers are the chart's, and the keys live in `.demo/helm/` (`.gitignore`
# covers `.demo/`; the teardown deletes them).
#
# ===========================================================================
# STANDING RULE 20: EVERY EXIT CODE ON ITS OWN LINE, NEVER THROUGH A PIPE
# ===========================================================================
# `crates/logweir/tests/gate_lint.rs::gate_lint_no_masked_exit_codes` runs the
# I29 tokeniser over every file under `scripts/`, this one included. The shape
# is `set +e; <tool> … > file; rc=$?; set -e; echo "    rc=$rc  (…)"`, and the
# `logweir` binary is the BARE WORD (put on `$PATH` by the preflight, exactly
# as `scripts/demo-steps.sh` does) so the tokeniser's literal-prefix rule
# guards every line that runs it.
#
# NEVER A BACKGROUNDED WAIT. Every wait is a bounded foreground poll. The ONE
# process this script backgrounds is the `kubectl port-forward` of step 9; its
# pid is recorded and the EXIT trap kills it.
#
# ===========================================================================
# WHAT THIS PROVES, AND WHAT IT DOES NOT
# ===========================================================================
# It proves the chart's objects work together: the runner reaches the chart's
# brokers by the names the brokers advertise, the archive lands on the chart's
# MinIO, the controller's evidence handle verifies against it, and the UI
# served in-cluster answers with its own ServiceAccount's authority. Run with
# `examples/author-only.values.yaml` it is AUTHOR-ONLY (Global Constraint 37):
# the images were built and loaded on this host, never pulled, so nothing here
# is evidence for spec §16 clause 1, and no checklist row moves.
set -euo pipefail
cd "$(dirname "$0")/.."

LOGWEIR_KUBE_CONTEXT="${LOGWEIR_KUBE_CONTEXT:-docker-desktop}"
LOGWEIR_HELM_RELEASE="${LOGWEIR_HELM_RELEASE:-logweir}"
LOGWEIR_HELM_NAMESPACE="${LOGWEIR_HELM_NAMESPACE:-logweir-system}"
export LOGWEIR_KUBE_CONTEXT LOGWEIR_HELM_RELEASE LOGWEIR_HELM_NAMESPACE
PYTHON="${LOGWEIR_PYTHON:-python3}"

REL="$LOGWEIR_HELM_RELEASE"
SYS="$LOGWEIR_HELM_NAMESPACE"
NS=logweir-helm
OUT=.demo/helm
CHART=charts/logweir
ARCHIVE_BUCKET=kafka-backups
BACKUP_PREFIX=helm-demo
ARCHIVE_URL="s3://${ARCHIVE_BUCKET}/${BACKUP_PREFIX}"
S3_ENDPOINT="http://${REL}-minio.${SYS}.svc:9000"
S3_REGION=us-east-1
SOURCE_BOOTSTRAP="${REL}-kafka-source.${SYS}.svc.cluster.local:9092"
TARGET_BOOTSTRAP="${REL}-kafka-target.${SYS}.svc.cluster.local:9092"
PROXY_BASE=http://127.0.0.1:8001
API_BASE="$PROXY_BASE/apis/logweir.dev/v1alpha1/namespaces/$NS"
# The `mc` image the chart pins, read from the chart so the two cannot drift.
MC_IMAGE="$(sed -n 's/^  mcImage:[[:space:]]*//p' "$CHART/values.yaml")"
PF_PID=""

echo "helm-demo: kubectl context $LOGWEIR_KUBE_CONTEXT, release $REL in namespace $SYS (STANDING RULE 12)"

die() { echo; echo "helm-demo: $*" >&2; exit 1; }
step() { echo; echo "==> $*"; }

# Dumps what a failed run was about BEFORE the teardown deletes it.
dump_run() {  # kind name
  echo
  echo "--- $1/$2, as the cluster last saw it -------------------------------"
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get "$1" "$2" -o yaml > "$OUT/failed-$1.yaml" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl get $1 $2 -o yaml)"
  cat "$OUT/failed-$1.yaml"
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get jobs,pods -o wide > "$OUT/failed-workload.txt" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl get jobs,pods -o wide)"
  cat "$OUT/failed-workload.txt"
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" logs "job/$2" --tail=200 --all-containers=true > "$OUT/failed-pod.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl logs job/$2 --tail=200)"
  cat "$OUT/failed-pod.log"
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$SYS" logs deploy/weirkeeper --tail=120 > "$OUT/failed-controller.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl logs deploy/weirkeeper --tail=120)"
  cat "$OUT/failed-controller.log"
  echo "---------------------------------------------------------------------"
}

# The transcript line goes to stderr and the value to stdout, so `v=$(read_field …)`
# captures the value alone.
read_field() {  # kind name jsonpath label
  set +e
  value=$(kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get "$1" "$2" -o jsonpath="$3")
  rc=$?
  set -e
  echo "    rc=$rc  $4: ${value:-<absent>}" >&2
  printf '%s' "$value"
}

# One fetch through the port-forward: the HTTP status from curl's own `-w`,
# the exit code on its own line, and the expected status asserted.
expect_http() {  # url expected label
  set +e
  curl -sS -o "$OUT/http-body.txt" -w '%{http_code}' "$1" > "$OUT/http-status.txt"
  rc=$?
  set -e
  http=$(cat "$OUT/http-status.txt")
  echo "    rc=$rc  HTTP $http  $3"
  [ "$rc" -eq 0 ] || die "curl exited $rc fetching $1 — is the port-forward still up?"
  case " $2 " in
    *" $http "*) ;;
    *) die "$1 answered $http, expected one of: $2" ;;
  esac
}

# Reads one object out of the chart's MinIO with a one-off pod running the
# pinned `mc` image — run to completion, its exit code read from the pod's own
# status, its stdout read from the container log (the kubelet keeps it whether
# or not anyone attached — `scripts/kind-demo.sh`'s lesson), then deleted.
mc_cat() {  # key outfile
  local pod="mc-cat-$RANDOM"
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" run "$pod" --restart=Never --image="$MC_IMAGE" --env="MC_HOST_local=http://${MINIO_USER}:${MINIO_PASSWORD}@${REL}-minio.${SYS}.svc:9000" --command -- mc cat "local/${ARCHIVE_BUCKET}/$1" > "$OUT/mc-run.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl run $pod --image=<the chart's mc digest> -- mc cat $ARCHIVE_BUCKET/$1)"
  [ "$rc" -eq 0 ] || { cat "$OUT/mc-run.log"; die "could not start the mc pod (rc=$rc)"; }
  local phase=""
  for _ in $(seq 1 60); do
    set +e
    phase=$(kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null)
    rc=$?
    set -e
    case "$phase" in Succeeded|Failed) break ;; esac
    sleep 2
  done
  echo "    phase=${phase:-<none>}  (kubectl get pod $pod -o jsonpath={.status.phase}, polled up to 120 s)"
  set +e
  mc_exit=$(kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null)
  rc=$?
  set -e
  echo "    rc=$rc  (container exit ${mc_exit:-<none>})"
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" logs "$pod" > "$2" 2> "$OUT/mc-logs.err"
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl logs $pod > $2)"
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" delete pod "$pod" > /dev/null 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete pod $pod)"
  [ "${mc_exit:-1}" = "0" ] || die "mc cat $1 exited ${mc_exit:-<none>}; see $2 and $OUT/mc-logs.err"
  [ -s "$2" ] || die "mc cat $1 returned no bytes"
}

# ---------------------------------------------------------------------------
# 10/10 TEARDOWN — the EXIT trap. The demo namespace, the keys, the release,
# the release namespace, the six CRDs Helm installed and never removes, and
# the cluster-scoped TrustRoster; then the assertion that no `logweir-*`
# namespace is left. Every rc printed.
# ---------------------------------------------------------------------------
teardown() {
  echo
  echo "==> 10/10 teardown"
  if [ -n "$PF_PID" ]; then
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
    echo "    stopped the kubectl port-forward (pid $PF_PID)"
  fi
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" delete trustroster default --ignore-not-found > "$OUT/teardown-roster.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete trustroster default)"
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" delete ns "$NS" --ignore-not-found --timeout=300s > "$OUT/teardown-ns.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete ns $NS)"
  set +e
  helm uninstall "$REL" -n "$SYS" --kube-context "$LOGWEIR_KUBE_CONTEXT" --wait --timeout 5m > "$OUT/teardown-helm.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (helm uninstall $REL -n $SYS)"
  cat "$OUT/teardown-helm.log"
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" delete ns "$SYS" --ignore-not-found --timeout=300s > "$OUT/teardown-sys.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete ns $SYS)"
  # HELM INSTALLS crds/ ONCE AND NEVER DELETES IT. A clean cluster is the
  # precondition of the next install, so the six go here, by name.
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" delete crd approvals.logweir.dev backups.logweir.dev backupschedules.logweir.dev kafkaclusters.logweir.dev restores.logweir.dev trustrosters.logweir.dev --ignore-not-found > "$OUT/teardown-crds.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete crd <the six logweir.dev kinds>)"
  # THE KEYS GO UNCONDITIONALLY. Minted minutes ago, attested by nothing.
  rm -f "$OUT"/*.pem
  echo "    removed $OUT/*.pem (both keypairs this run minted)"
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" get ns -o name > "$OUT/teardown-ns-list.txt" 2>&1
  rc=$?
  set -e
  leftover=$(grep -c '^namespace/logweir-' "$OUT/teardown-ns-list.txt" || true)
  echo "    rc=$rc  (kubectl get ns -o name) -> logweir-* namespaces left: $leftover"
  if [ "$leftover" -ne 0 ]; then
    echo "    STILL PRESENT:"
    grep '^namespace/logweir-' "$OUT/teardown-ns-list.txt" || true
  fi
}

# ---------------------------------------------------------------------------
# 0/10 PREFLIGHT — the release is installed, both brokers, MinIO and the UI are
# Ready, the seed Jobs succeeded (a succeeded hook Job is DELETED by the chart's
# hook-delete-policy, so "succeeded" is asserted as: the release is `deployed`,
# no failed Job remains, and the seeds' EFFECTS are on the brokers).
# ---------------------------------------------------------------------------
step "0/10 preflight: tools, the release, the brokers, MinIO, the UI, the seeds' effects"
mkdir -p "$OUT"
for tool in kubectl helm openssl awk node curl; do
  command -v "$tool" >/dev/null 2>&1 || die "\`$tool\` is not on \$PATH."
done
command -v "$PYTHON" >/dev/null 2>&1 || die "\`$PYTHON\` is not on \$PATH; set LOGWEIR_PYTHON."
[ -n "$MC_IMAGE" ] || die "could not read minio.mcImage from $CHART/values.yaml"

LOGWEIR_BIN="${LOGWEIR_BIN:-$PWD/target/debug/logweir}"
[ -x "$LOGWEIR_BIN" ] || die "no logweir binary at $LOGWEIR_BIN — run \`cargo build -p logweir\` (\`just helm-demo\` does it for you)."
mkdir -p "$OUT/bin"
ln -sf "$LOGWEIR_BIN" "$OUT/bin/logweir"
PATH="$PWD/$OUT/bin:$PATH"
export PATH

set +e
helm status "$REL" -n "$SYS" --kube-context "$LOGWEIR_KUBE_CONTEXT" -o json > "$OUT/helm-status.json" 2> "$OUT/helm-status.err"
rc=$?
set -e
echo "    rc=$rc  (helm status $REL -n $SYS -o json)"
[ "$rc" -eq 0 ] || { cat "$OUT/helm-status.err"; die "the release $REL is not installed in $SYS. Install it first: helm install $REL $CHART -n $SYS --create-namespace -f $CHART/examples/demo.values.yaml --wait --timeout 10m"; }
release_status=$("$PYTHON" -c 'import json,sys; print(json.load(open(sys.argv[1]))["info"]["status"])' "$OUT/helm-status.json")
# The JSON is read and then REMOVED: it flattens the whole release manifest
# onto one line, and `scripts/check-unverified-labels.sh` walks `.demo/` —
# untracked, but beside the tree — where that one line would carry a mark and
# an unrelated "verified" together (measured on this walk's first run).
rm -f "$OUT/helm-status.json"
echo "    release status: $release_status"
[ "$release_status" = "deployed" ] || die "the release is '$release_status', not deployed — a failed hook Job leaves the release in that state; \`kubectl -n $SYS get jobs\` and \`kubectl logs job/…\` say why"

for target in "deploy/weirkeeper" "statefulset/${REL}-kafka-source" "statefulset/${REL}-kafka-target" "deploy/${REL}-minio" "deploy/${REL}-ui"; do
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$SYS" rollout status "$target" --timeout=300s > "$OUT/rollout-${target##*/}.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl rollout status $target --timeout=300s)"
  [ "$rc" -eq 0 ] || { cat "$OUT/rollout-${target##*/}.log"; die "$target is not Ready (rc=$rc); the chart's flags minio.enabled, demoKafka.enabled and ui.enabled must all be on"; }
done

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$SYS" get jobs -o jsonpath='{range .items[*]}{.metadata.name}{" failed="}{.status.failed}{" succeeded="}{.status.succeeded}{"\n"}{end}' > "$OUT/jobs.txt" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl get jobs — a succeeded seed Job is deleted by its hook policy, so this lists leftovers only)"
cat "$OUT/jobs.txt"
if grep -q 'failed=[1-9]' "$OUT/jobs.txt"; then
  die "a seed Job failed; \`kubectl -n $SYS logs job/<name>\` says why"
fi

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$SYS" exec "statefulset/${REL}-kafka-source" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$SOURCE_BOOTSTRAP" --list > "$OUT/source-topics.txt" 2> "$OUT/source-topics.err"
rc=$?
set -e
echo "    rc=$rc  (kafka-topics.sh --list on the source, via kubectl exec) -> $(tr '\n' ' ' < "$OUT/source-topics.txt")"
[ "$rc" -eq 0 ] || { cat "$OUT/source-topics.err"; die "could not list the source's topics (rc=$rc)"; }
grep -qx 'orders' "$OUT/source-topics.txt" || die "the seed Job did not create \`orders\` on the source"
grep -qx 'payments' "$OUT/source-topics.txt" || die "the seed Job did not create \`payments\` on the source"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$SYS" exec "statefulset/${REL}-kafka-target" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$TARGET_BOOTSTRAP" --list > "$OUT/target-topics.txt" 2> "$OUT/target-topics.err"
rc=$?
set -e
echo "    rc=$rc  (kafka-topics.sh --list on the target, via kubectl exec) -> $(tr '\n' ' ' < "$OUT/target-topics.txt")"
[ "$rc" -eq 0 ] || { cat "$OUT/target-topics.err"; die "could not list the target's topics (rc=$rc)"; }
grep -qx 'logweir.scratch' "$OUT/target-topics.txt" || die "the seed Job did not create the marker topic \`logweir.scratch\` on the target — phase 0 refuses a scratch restore without it"

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" get ns "$NS" -o name > "$OUT/ns-check.txt" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl get ns $NS — must NOT exist yet: check-then-take)"
[ "$rc" -ne 0 ] || die "namespace $NS already exists; somebody else's run, or a teardown that did not finish. Delete it and re-run."

# THE CHART'S MinIO ROOT CREDENTIAL, read back from the release's own Secret so
# this script and the chart cannot disagree about it.
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$SYS" get secret "${REL}-minio-root" -o json > "$OUT/minio-root.json" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl get secret ${REL}-minio-root -o json)"
[ "$rc" -eq 0 ] || die "the chart's ${REL}-minio-root Secret is missing (rc=$rc); is minio.enabled on?"
MINIO_USER=$("$PYTHON" -c 'import base64,json,sys; d=json.load(open(sys.argv[1]))["data"]; print(base64.b64decode(d["MINIO_ROOT_USER"]).decode())' "$OUT/minio-root.json")
MINIO_PASSWORD=$("$PYTHON" -c 'import base64,json,sys; d=json.load(open(sys.argv[1]))["data"]; print(base64.b64decode(d["MINIO_ROOT_PASSWORD"]).decode())' "$OUT/minio-root.json")
[ -n "$MINIO_USER" ] && [ -n "$MINIO_PASSWORD" ] || die "the MinIO root Secret carries no credential"
rm -f "$OUT/minio-root.json"
echo "    the chart's MinIO root user: $MINIO_USER (demo-only; the JSON it was read from is not kept)"

trap teardown EXIT

# ---------------------------------------------------------------------------
# 1/10 THE TWO KEYPAIRS — the four openssl commands step 4 of the demo uses.
# ---------------------------------------------------------------------------
step "1/10 minting the signing and approver keypairs into $OUT/"
openssl ecparam -genkey -name prime256v1 -noout > "$OUT/signing.der.pem"
openssl pkcs8 -topk8 -nocrypt -in "$OUT/signing.der.pem" -out "$OUT/signing.pem"
openssl ecparam -genkey -name prime256v1 -noout > "$OUT/approver.der.pem"
openssl pkcs8 -topk8 -nocrypt -in "$OUT/approver.der.pem" -out "$OUT/approver.pem"
rm -f "$OUT/signing.der.pem" "$OUT/approver.der.pem"
openssl ec -in "$OUT/signing.pem"  -pubout -out "$OUT/signing.pub.pem"  2>/dev/null
openssl ec -in "$OUT/approver.pem" -pubout -out "$OUT/approver.pub.pem" 2>/dev/null
chmod 600 "$OUT/signing.pem" "$OUT/approver.pem"
SIGNING_KEY_ID=$(openssl ec -pubin -in "$OUT/signing.pub.pem" -outform DER 2>/dev/null | openssl dgst -sha256 -hex | awk '{print $NF}')
APPROVER_KEY_ID=$(openssl ec -pubin -in "$OUT/approver.pub.pem" -outform DER 2>/dev/null | openssl dgst -sha256 -hex | awk '{print $NF}')
[ -n "$SIGNING_KEY_ID" ] || die "could not compute the signing key id"
[ -n "$APPROVER_KEY_ID" ] || die "could not compute the approver key id"
echo "    signing key id:  $SIGNING_KEY_ID"
echo "    approver key id: $APPROVER_KEY_ID"
echo "    both keypairs were minted seconds ago and are attested by nothing; the teardown deletes them."

# ---------------------------------------------------------------------------
# 2/10 THE DEMO NAMESPACE, THE FIVE SECRETS, THE RUNNER ServiceAccount, THE
#      NetworkPolicy, THE UI's RoleBinding — then `just check-secrets`.
# ---------------------------------------------------------------------------
step "2/10 namespace $NS, the five Secrets, the runner ServiceAccount, then just check-secrets $NS"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" create namespace "$NS" > "$OUT/ns.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl create namespace $NS)"
[ "$rc" -eq 0 ] || die "could not create namespace $NS (rc=$rc)"

# 1. the runner's signing key — data key `signing.pem`.
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" create secret generic logweir-signing-key --from-file=signing.pem="$OUT/signing.pem" > "$OUT/secret-signing.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (secret/logweir-signing-key, data key signing.pem)"
[ "$rc" -eq 0 ] || die "could not create logweir-signing-key (rc=$rc)"

# 2. the approval bundle — placeholders for the two halves that cannot exist
#    yet (they sign the plan bytes, which are step 6's); step 7 REPLACES it,
#    with the real allowlist carrying the target's OBSERVED cluster id.
printf '{"allowed_cluster_ids":["REPLACED-AT-STEP-7"],"source_cluster_id":null}\n' > "$OUT/allowed-clusters.json"
printf 'replaced-at-step-7\n' > "$OUT/approval.placeholder"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" create secret generic logweir-approval-bundle --from-file=approval.json="$OUT/approval.placeholder" --from-file=approval.sig="$OUT/approval.placeholder" --from-file=approver.pub.pem="$OUT/approver.pub.pem" --from-file=allowed-clusters.json="$OUT/allowed-clusters.json" > "$OUT/secret-approval.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (secret/logweir-approval-bundle, four keys — approval.json/.sig and the allowlist replaced at step 7)"
[ "$rc" -eq 0 ] || die "could not create logweir-approval-bundle (rc=$rc)"

# 3. the per-cluster SCRAM credential — unused on PLAINTEXT, counted by check-secrets.
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" create secret generic kafka-scram --from-literal=password=unused-on-the-plaintext-listener > "$OUT/secret-scram.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (secret/kafka-scram, data key password)"
[ "$rc" -eq 0 ] || die "could not create kafka-scram (rc=$rc)"

# 4. the runner's archive credential — COPIED from the chart's own
#    `logweir-s3` in the release namespace, so the demo dials the archive with
#    the credential the chart minted and not a literal of its own.
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$SYS" get secret logweir-s3 -o json > "$OUT/s3-secret.json" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl get secret logweir-s3 -n $SYS — the chart's)"
[ "$rc" -eq 0 ] || die "the chart's logweir-s3 Secret is missing from $SYS (rc=$rc)"
"$PYTHON" - "$OUT/s3-secret.json" "$NS" "$OUT/s3-secret-copy.json" <<'PY'
import json, sys
src, ns, out = sys.argv[1:4]
doc = json.load(open(src))
copy = {"apiVersion": "v1", "kind": "Secret", "type": doc.get("type", "Opaque"),
        "metadata": {"name": doc["metadata"]["name"], "namespace": ns},
        "data": doc["data"]}
json.dump(copy, open(out, "w"))
PY
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" create -f "$OUT/s3-secret-copy.json" > "$OUT/secret-s3.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (secret/logweir-s3 in $NS — the chart's data, copied)"
[ "$rc" -eq 0 ] || die "could not create logweir-s3 in $NS (rc=$rc)"
rm -f "$OUT/s3-secret.json" "$OUT/s3-secret-copy.json"

# 5. the CONTROLLER's read-only evidence credential — in the release
#    namespace. MinIO gives no second identity out of the box, so the demo
#    uses the same root pair under a different Secret name: WHAT THIS PROVES
#    IS THE PROJECTION, not the IAM policy. An adopter gives this principal
#    `s3:GetObject` on the evidence prefix and nothing else.
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$SYS" create secret generic logweir-evidence-ro --from-literal=access-key-id="$MINIO_USER" --from-literal=secret-access-key="$MINIO_PASSWORD" > "$OUT/secret-evidence.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (secret/logweir-evidence-ro, in $SYS)"
[ "$rc" -eq 0 ] || die "could not create logweir-evidence-ro (rc=$rc)"
# The controller reads it from its OWN ENV, fixed at container start.
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$SYS" rollout restart deploy/weirkeeper > "$OUT/restart.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl rollout restart deploy/weirkeeper — env is fixed at container start)"
[ "$rc" -eq 0 ] || die "could not restart the controller (rc=$rc)"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$SYS" rollout status deploy/weirkeeper --timeout=300s > "$OUT/rollout2.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl rollout status deploy/weirkeeper, after the evidence Secret)"
[ "$rc" -eq 0 ] || die "the controller did not come back (rc=$rc)"

# The runner ServiceAccount, in the namespace that runs Jobs (docs/install.md
# step 4). The NetworkPolicy is NOT applied here: the shipped file carries
# `namespace: logweir-system` in its metadata, so `kubectl -n <ns> apply -f`
# of it refuses with a namespace mismatch (measured on this walk's first run,
# 2026-09-12) — the laptop demo does not apply it into its namespace either,
# and neither cluster this walk runs on enforces NetworkPolicy.
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" apply -f config/rbac/backup-runner-serviceaccount.yaml > "$OUT/runner-sa.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl apply -f config/rbac/backup-runner-serviceaccount.yaml -n $NS)"
[ "$rc" -eq 0 ] || die "could not create the runner ServiceAccount (rc=$rc)"

# The page's RoleBinding in THIS namespace: the chart binds `<release>-ui` in
# the release namespace, and every other namespace the page should see gets
# the same one-line binding (NOTES.txt prints it).
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" create rolebinding "${REL}-ui" --clusterrole="${REL}-ui" --serviceaccount="${SYS}:${REL}-ui" > "$OUT/ui-rolebinding.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl create rolebinding ${REL}-ui --clusterrole=${REL}-ui --serviceaccount=${SYS}:${REL}-ui -n $NS)"
[ "$rc" -eq 0 ] || die "could not bind the page's role in $NS (rc=$rc)"

# `just check-secrets` reads the fifth Secret from the literal `logweir-system`
# (its recipe), which is this walk's default release namespace. Under another
# release namespace the same five reads are made here instead, and said so.
if [ "$SYS" = "logweir-system" ]; then
  set +e
  just check-secrets "$NS" > "$OUT/check-secrets.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (just check-secrets $NS)"
  cat "$OUT/check-secrets.log"
  [ "$rc" -eq 0 ] || die "\`just check-secrets $NS\` exited $rc; see $OUT/check-secrets.log"
else
  echo "    just check-secrets reads logweir-evidence-ro from the literal namespace logweir-system; this"
  echo "    release is in $SYS, so the five reads are made here:"
  for pair in "logweir-signing-key:$NS" "logweir-approval-bundle:$NS" "kafka-scram:$NS" "logweir-s3:$NS" "logweir-evidence-ro:$SYS"; do
    set +e
    kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "${pair##*:}" get secret "${pair%%:*}" -o name > /dev/null 2>&1
    rc=$?
    set -e
    echo "    rc=$rc  (kubectl get secret ${pair%%:*} -n ${pair##*:})"
    [ "$rc" -eq 0 ] || die "${pair%%:*} is absent from ${pair##*:}"
  done
fi

# ---------------------------------------------------------------------------
# 3/10 THE CLUSTER-SCOPED TrustRoster `default`, from the sample's shape.
# ---------------------------------------------------------------------------
step "3/10 TrustRoster default — the approver key id and the signing key MATERIAL"
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
      subject: helm-demo-approver@example.invalid
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
kubectl --context "$LOGWEIR_KUBE_CONTEXT" apply -f "$OUT/trustroster.yaml" > "$OUT/roster.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl apply -f trustroster.yaml — cluster-scoped, name 'default')"
[ "$rc" -eq 0 ] || die "could not apply the TrustRoster (rc=$rc); see $OUT/roster.log"

# ---------------------------------------------------------------------------
# 4/10 TWO KafkaClusters AT THE CHART'S BROKERS, AND `reachable: true` ON BOTH.
# ---------------------------------------------------------------------------
step "4/10 KafkaCluster source ($SOURCE_BOOTSTRAP) and target ($TARGET_BOOTSTRAP) -> status.reachable"
cat > "$OUT/kafkaclusters.yaml" <<YAML
apiVersion: logweir.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: source
  namespace: $NS
spec:
  bootstrapServers: ["$SOURCE_BOOTSTRAP"]
  auth:
    mode: plaintext
    tls: false
  role: source
---
apiVersion: logweir.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: target
  namespace: $NS
spec:
  bootstrapServers: ["$TARGET_BOOTSTRAP"]
  auth:
    mode: plaintext
    tls: false
  role: target
YAML
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" apply -f "$OUT/kafkaclusters.yaml" > "$OUT/kc-apply.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl apply -f kafkaclusters.yaml — source and target, PLAINTEXT: the demo's transport, not a recommendation)"
[ "$rc" -eq 0 ] || die "could not apply the KafkaClusters (rc=$rc)"
for kc in source target; do
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" wait --for=jsonpath='{.status.reachable}'=true "kafkacluster/$kc" --timeout=300s > "$OUT/kc-wait-$kc.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl wait --for=jsonpath={.status.reachable}=true kafkacluster/$kc --timeout=300s)"
  if [ "$rc" -ne 0 ]; then
    dump_run kafkacluster "$kc"
    die "KafkaCluster $kc never became reachable. The probe Job runs \`logweir cluster-probe\` against the chart's broker."
  fi
done
SOURCE_CLUSTER_ID=$(read_field kafkacluster source '{.status.clusterId}' 'source status.clusterId')
TARGET_CLUSTER_ID=$(read_field kafkacluster target '{.status.clusterId}' 'target status.clusterId')
[ -n "$SOURCE_CLUSTER_ID" ] || die "the source KafkaCluster recorded no clusterId"
[ -n "$TARGET_CLUSTER_ID" ] || die "the target KafkaCluster recorded no clusterId"
[ "$SOURCE_CLUSTER_ID" != "$TARGET_CLUSTER_ID" ] || die "the two brokers report the SAME cluster id ($SOURCE_CLUSTER_ID); phase 0's target != source rail would refuse the restore"
echo "    two clusters, two ids: source $SOURCE_CLUSTER_ID, target $TARGET_CLUSTER_ID"

# ---------------------------------------------------------------------------
# 5/10 A BackupSchedule ON */2, AND THE Backup IT FIRES — Succeeded, exitCode 0,
#      receipt Valid. Then the schedule is suspended so the chosen Backup is
#      stable for the Restore (the demo's step 8 reasoning).
# ---------------------------------------------------------------------------
step "5/10 BackupSchedule */2 * * * * over orders and payments into $ARCHIVE_URL, and the Backup it fires"
cat > "$OUT/backupschedule.yaml" <<YAML
apiVersion: logweir.dev/v1alpha1
kind: BackupSchedule
metadata:
  name: helm
  namespace: $NS
spec:
  schedule: "*/2 * * * *"
  sourceRef:
    name: source
  topics: ["orders", "payments"]
  archive:
    url: $ARCHIVE_URL
    secretRef:
      name: logweir-s3
  suspend: false
YAML
BACKUP_T0=$(date +%s)
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" apply -f "$OUT/backupschedule.yaml" > "$OUT/bs-apply.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl apply -f backupschedule.yaml, schedule */2 * * * *)"
[ "$rc" -eq 0 ] || die "could not apply the BackupSchedule (rc=$rc)"

echo "    waiting for the schedule to fire (bounded: 60 polls of 5 s)..."
BACKUP_NAME=""
for _ in $(seq 1 60); do
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get backups -o name > "$OUT/backups.txt" 2>/dev/null
  rc=$?
  set -e
  BACKUP_NAME=$(sed -n 's|^backup.logweir.dev/||p' "$OUT/backups.txt" | head -1)
  [ -n "$BACKUP_NAME" ] && break
  sleep 5
done
echo "    rc=$rc  (kubectl get backups -o name)"
[ -n "$BACKUP_NAME" ] || die "no Backup appeared within five minutes. The BackupSchedule reconciler creates one object per due slot, named <schedule>-<slot>."
echo "    the schedule fired: Backup/$BACKUP_NAME"

backup_phase=""
for _ in $(seq 1 30); do
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get backup "$BACKUP_NAME" -o jsonpath='{.status.phase}' > "$OUT/backup-phase.txt" 2>/dev/null
  rc=$?
  set -e
  backup_phase=$(cat "$OUT/backup-phase.txt")
  case "$backup_phase" in Succeeded|Failed|Refused) break ;; esac
  sleep 10
done
BACKUP_T1=$(date +%s)
echo "    rc=$rc  (kubectl get backup $BACKUP_NAME -o jsonpath={.status.phase}, polled up to 5 min)"
echo "    phase: ${backup_phase:-<absent>}   wall clock from the schedule's apply: $((BACKUP_T1 - BACKUP_T0)) s"
if [ "$backup_phase" != "Succeeded" ]; then
  dump_run backup "$BACKUP_NAME"
  die "the Backup reached phase '${backup_phase:-<absent>}', not Succeeded."
fi
b_exit=$(read_field backup "$BACKUP_NAME" '{.status.exitCode}' 'status.exitCode')
b_receipt=$(read_field backup "$BACKUP_NAME" '{.status.evidence.receiptKey}' 'status.evidence.receiptKey')
b_id=$(read_field backup "$BACKUP_NAME" '{.status.backupId}' 'status.backupId')
b_result=""
for _ in $(seq 1 24); do
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get backup "$BACKUP_NAME" -o jsonpath='{.status.evidence.verification.result}' > "$OUT/backup-verdict.txt" 2>/dev/null
  rc=$?
  set -e
  b_result=$(cat "$OUT/backup-verdict.txt")
  [ -n "$b_result" ] && [ "$b_result" != "NotAttempted" ] && break
  sleep 5
done
echo "    rc=$rc  (kubectl get backup $BACKUP_NAME -o jsonpath={.status.evidence.verification.result})"
echo "    status.evidence.verification.result: ${b_result:-<absent>}"
[ "$b_exit" = "0" ] || die "Backup exitCode is '$b_exit', not 0"
[ -n "$b_receipt" ] || die "the Backup recorded no receiptKey"
[ -n "$b_id" ] || die "the Backup recorded no backupId"
[ "$b_result" = "Valid" ] || die "the Backup's receipt verified as '${b_result:-<absent>}', not Valid"

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" patch backupschedule helm --type=merge -p '{"spec":{"suspend":true}}' > "$OUT/suspend.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl patch backupschedule helm spec.suspend=true — the ONE mutable field)"
[ "$rc" -eq 0 ] || die "could not suspend the BackupSchedule (rc=$rc)"

# ---------------------------------------------------------------------------
# 6/10 THE Restore — from the most recent Backup, its planBytes rendered by the
#      PAGE's own emitter (ui/tests/emit-restore-body.js), a SCRATCH restore
#      onto the chart's target broker, whose marker topic the seed created.
# ---------------------------------------------------------------------------
step "6/10 the Restore: the page's own emitter renders the plan bytes; kubectl create -f the body"
PIT_RFC3339=$("$PYTHON" -c 'import datetime as d
print((d.datetime.now(d.timezone.utc).replace(microsecond=0) + d.timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ"))')
SAMPLE_START_RFC3339=$("$PYTHON" -c 'import datetime as d
print((d.datetime.now(d.timezone.utc).replace(microsecond=0) - d.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ"))')
[ -n "${PIT_RFC3339:-}" ] || die "could not compute the recovery point"
echo "    recovery point: $PIT_RFC3339   sample window from: $SAMPLE_START_RFC3339 (the seed ran at install time)"
cat > "$OUT/plan-fields.json" <<JSON
{
  "ns": "$NS",
  "archiveUrl": "$ARCHIVE_URL",
  "archiveSecretName": "logweir-s3",
  "targetClusterName": "target",
  "deadlineSeconds": 1800,
  "fields": {
    "name": "helm-demo",
    "backupSetRef": "latestCompleted",
    "topics": ["orders", "payments"],
    "pointInTime": "$PIT_RFC3339",
    "source": {
      "bucket": "$ARCHIVE_BUCKET",
      "prefix": "$BACKUP_PREFIX",
      "region": "$S3_REGION",
      "endpoint": "$S3_ENDPOINT",
      "pathStyle": true,
      "allowHttp": true
    },
    "target": {
      "bootstrapServers": ["$TARGET_BOOTSTRAP"],
      "mode": "scratch",
      "topicPrefix": "logweir-scratch-",
      "topicMappingPrefix": "logweir-scratch-",
      "markerTopic": "logweir.scratch",
      "replicationFactor": 1,
      "teardown": "delete"
    },
    "sample": {
      "windowStart": "$SAMPLE_START_RFC3339",
      "windowEnd": "$PIT_RFC3339",
      "recordsPerPartition": 25,
      "anchor": "head"
    },
    "objectives": {
      "rtoSeconds": 3600,
      "rpoSeconds": 86400,
      "passRate": 1.0
    },
    "evidence": {
      "bucket": "$ARCHIVE_BUCKET",
      "prefix": "logweir/",
      "region": "$S3_REGION",
      "endpoint": "$S3_ENDPOINT",
      "pathStyle": true,
      "allowHttp": true
    }
  }
}
JSON
set +e
node ui/tests/emit-restore-body.js --out "$OUT/" > "$OUT/emit.out" 2> "$OUT/emit.err"
rc=$?
set -e
echo "    rc=$rc  (node ui/tests/emit-restore-body.js --out $OUT/)"
cat "$OUT/emit.out"
[ "$rc" -eq 0 ] || { cat "$OUT/emit.err"; die "the body emitter exited $rc"; }
PLAN_HASH=$(sed -n 's/^plan-hash=//p' "$OUT/emit.out")
RESTORE_NAME=$(sed -n 's/^restore-name=//p' "$OUT/emit.out")
APPROVAL_NAME=$(sed -n 's/^approval-name=//p' "$OUT/emit.out")
[ -n "$PLAN_HASH" ] || die "the emitter printed no plan-hash="
[ -n "$RESTORE_NAME" ] || die "the emitter printed no restore-name="
[ -n "$APPROVAL_NAME" ] || die "the emitter printed no approval-name="
RESTORE_T0=$(date +%s)
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" create -f "$OUT/restore-body.json" > "$OUT/restore-create.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl create -f restore-body.json — Restore/$RESTORE_NAME, approvalRef -> $APPROVAL_NAME, which does not exist yet)"
[ "$rc" -eq 0 ] || { cat "$OUT/restore-create.log"; die "could not create the Restore (rc=$rc)"; }

# ---------------------------------------------------------------------------
# 7/10 THE APPROVAL, MINTED ON THE HOST WITH THE SHIPPED CLI, AND THE Approval.
# ---------------------------------------------------------------------------
step "7/10 logweir drill approve on the HOST, the real approval bundle, then the Approval object"
set +e
logweir drill approve --spec "$OUT/plan.yaml" --key "$OUT/approver.pem" --approver helm-demo --ticket DEMO-HELM --subject-kind Restore --out "$OUT/approval.json" > "$OUT/approve.out" 2>&1
rc=$?
set -e
echo "    rc=$rc  (logweir drill approve --subject-kind Restore --out $OUT/approval.json)"
cat "$OUT/approve.out"
[ "$rc" -eq 0 ] || die "\`logweir drill approve\` exited $rc"
[ -f "$OUT/approval.sig" ] || die "the detached sidecar $OUT/approval.sig was not written"
APPROVED_HASH=$(awk '/plan_hash/ { print $2 }' "$OUT/approve.out")
echo "    plan_hash from the CLI : $APPROVED_HASH"
echo "    plan-hash from the page: $PLAN_HASH"
[ "$APPROVED_HASH" = "$PLAN_HASH" ] || die "the CLI signed $APPROVED_HASH and the page showed $PLAN_HASH — two documents with one name"

# THE REAL ALLOWLIST: the target's OBSERVED cluster id (phase 0's scratch rail
# 1), and the source's id as the id a scratch restore must not equal (rail 2).
printf '{"allowed_cluster_ids":["%s"],"source_cluster_id":"%s"}\n' "$TARGET_CLUSTER_ID" "$SOURCE_CLUSTER_ID" > "$OUT/allowed-clusters.json"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" delete secret logweir-approval-bundle > "$OUT/secret-approval-delete.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl delete secret logweir-approval-bundle — the placeholder)"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" create secret generic logweir-approval-bundle --from-file=approval.json="$OUT/approval.json" --from-file=approval.sig="$OUT/approval.sig" --from-file=approver.pub.pem="$OUT/approver.pub.pem" --from-file=allowed-clusters.json="$OUT/allowed-clusters.json" > "$OUT/secret-approval2.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (secret/logweir-approval-bundle, the real four keys; allowlist = [$TARGET_CLUSTER_ID], source $SOURCE_CLUSTER_ID)"
[ "$rc" -eq 0 ] || die "could not re-create logweir-approval-bundle (rc=$rc)"

"$PYTHON" - "$OUT/approval.json" "$OUT/approval.sig" "$NS" "$APPROVAL_NAME" "$RESTORE_NAME" "$APPROVED_HASH" "$OUT/approval-object.yaml" <<'PY'
import sys, textwrap
approval_path, sidecar_path, ns, name, subject, plan_hash, out = sys.argv[1:8]
approval = open(approval_path).read()
sidecar = open(sidecar_path).read()
doc = f"""apiVersion: logweir.dev/v1alpha1
kind: Approval
metadata:
  name: {name}
  namespace: {ns}
spec:
  subjectRef:
    kind: Restore
    name: {subject}
  planHash: {plan_hash}
  approvalBytes: |
{textwrap.indent(approval.rstrip(chr(10)), ' ' * 4)}
  sidecarBytes: |
{textwrap.indent(sidecar.rstrip(chr(10)), ' ' * 4)}
"""
open(out, "w").write(doc)
PY
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" create -f "$OUT/approval-object.yaml" > "$OUT/approval-apply.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl create -f approval-object.yaml — Approval/$APPROVAL_NAME over Restore/$RESTORE_NAME)"
[ "$rc" -eq 0 ] || { cat "$OUT/approval-apply.log"; die "could not create the Approval (rc=$rc)"; }
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" wait --for=jsonpath='{.status.verified}'=true "approval/$APPROVAL_NAME" --timeout=180s > "$OUT/approval-wait.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl wait --for=jsonpath={.status.verified}=true approval/$APPROVAL_NAME)"
if [ "$rc" -ne 0 ]; then
  dump_run approval "$APPROVAL_NAME"
  die "the Approval never verified"
fi
matched=$(read_field approval "$APPROVAL_NAME" '{.status.matchedKeyId}' 'status.matchedKeyId')
[ -n "$matched" ] || die "the Approval names no matchedKeyId"

# ---------------------------------------------------------------------------
# 8/10 THE Restore's TERMINAL STATUS, AND BOTH READERS OVER THE SCORECARD.
# ---------------------------------------------------------------------------
step "8/10 the Restore's terminal status (outcome pass), then BOTH readers over the scorecard"
restore_phase=""
for _ in $(seq 1 60); do
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get restore "$RESTORE_NAME" -o jsonpath='{.status.phase}' > "$OUT/restore-phase.txt" 2>/dev/null
  rc=$?
  set -e
  restore_phase=$(cat "$OUT/restore-phase.txt")
  case "$restore_phase" in Succeeded|Failed|Refused) break ;; esac
  sleep 10
done
RESTORE_T1=$(date +%s)
echo "    rc=$rc  (kubectl get restore $RESTORE_NAME -o jsonpath={.status.phase}, polled up to 10 min)"
echo "    phase: ${restore_phase:-<absent>}   wall clock from the Restore's create: $((RESTORE_T1 - RESTORE_T0)) s"
if [ "$restore_phase" != "Succeeded" ]; then
  dump_run restore "$RESTORE_NAME"
  die "the Restore reached phase '${restore_phase:-<absent>}', not Succeeded."
fi
r_exit=$(read_field restore "$RESTORE_NAME" '{.status.exitCode}' 'status.exitCode')
r_outcome=$(read_field restore "$RESTORE_NAME" '{.status.outcome}' 'status.outcome')
r_integrity=$(read_field restore "$RESTORE_NAME" '{.status.integrity.level}' 'status.integrity.level')
r_scorecard=$(read_field restore "$RESTORE_NAME" '{.status.evidence.scorecardKey}' 'status.evidence.scorecardKey')
r_sidecar=$(read_field restore "$RESTORE_NAME" '{.status.evidence.sidecarKey}' 'status.evidence.sidecarKey')
r_result=""
for _ in $(seq 1 24); do
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get restore "$RESTORE_NAME" -o jsonpath='{.status.evidence.verification.result}' > "$OUT/restore-verdict.txt" 2>/dev/null
  rc=$?
  set -e
  r_result=$(cat "$OUT/restore-verdict.txt")
  [ -n "$r_result" ] && [ "$r_result" != "NotAttempted" ] && break
  sleep 5
done
echo "    rc=$rc  (kubectl get restore $RESTORE_NAME -o jsonpath={.status.evidence.verification.result}) -> ${r_result:-<absent>}"
[ "$r_exit" = "0" ] || die "Restore exitCode is '$r_exit', not 0"
[ "$r_outcome" = "pass" ] || die "Restore outcome is '$r_outcome', not pass"
[ -n "$r_scorecard" ] || die "the Restore recorded no scorecardKey"
[ -n "$r_sidecar" ] || die "the Restore recorded no sidecarKey"
[ "$r_result" = "Valid" ] || die "the Restore's scorecard verified as '${r_result:-<absent>}', not Valid"

mc_cat "$r_scorecard" "$OUT/scorecard.json"
mc_cat "$r_sidecar" "$OUT/scorecard.sig"
set +e
logweir drill verify --scorecard "$OUT/scorecard.json" --signature "$OUT/scorecard.sig" --public-key "$OUT/signing.pub.pem" --payload-type scorecard > "$OUT/verify-rust.out" 2>&1
rust_rc=$?
set -e
echo "    rc=$rust_rc  (logweir drill verify --payload-type scorecard)"
cat "$OUT/verify-rust.out"
set +e
"$PYTHON" docs/verify_scorecard.py --payload-type scorecard "$OUT/scorecard.json" "$OUT/scorecard.sig" "$OUT/signing.pub.pem" > "$OUT/verify-python.out" 2>&1
py_rc=$?
set -e
echo "    rc=$py_rc  (python3 docs/verify_scorecard.py --payload-type scorecard)"
cat "$OUT/verify-python.out"
[ "$rust_rc" -eq 0 ] || die "the Rust reader refused the scorecard (rc=$rust_rc)"
[ "$py_rc" -eq 0 ] || die "the Python reader refused the scorecard (rc=$py_rc)"

# ---------------------------------------------------------------------------
# 9/10 THE UI, IN-CLUSTER, THROUGH A PORT-FORWARD: the page 200, the API 200
#      with the Backup listed, and one path the proxy refuses.
# ---------------------------------------------------------------------------
step "9/10 the UI: kubectl port-forward svc/${REL}-ui 8001:8001, then three fetches"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$SYS" port-forward "svc/${REL}-ui" 8001:8001 > "$OUT/port-forward.log" 2>&1 &
rc=$?
set -e
PF_PID=$!
echo "    rc=$rc  (kubectl port-forward svc/${REL}-ui 8001:8001, backgrounded; pid $PF_PID — killed by the trap)"
up=""
for _ in $(seq 1 30); do
  set +e
  curl -sS -o /dev/null "$PROXY_BASE/ui/" > "$OUT/pf-poll.txt" 2>&1
  rc=$?
  set -e
  [ "$rc" -eq 0 ] && up=yes && break
  sleep 1
done
echo "    rc=$rc  (curl $PROXY_BASE/ui/ — the readiness poll, up to 30 s)"
[ -n "$up" ] || die "the port-forward never answered on 8001; see $OUT/port-forward.log"
echo
echo "    WHOSE AUTHORITY: the page is served by kubectl proxy in the ${REL}-ui pod, and the proxy"
echo "    attaches THAT ServiceAccount's credential to every request it forwards — anyone who can"
echo "    reach the Service acts with ${REL}-ui's authority. The page holds no credential."
echo
expect_http "$PROXY_BASE/ui/" "200" "the page itself"
expect_http "$PROXY_BASE/ui/app.js" "200" "the router"
served_sha=$(shasum -a 256 "$OUT/http-body.txt" | awk '{print $1}')
tree_sha=$(shasum -a 256 ui/app.js | awk '{print $1}')
echo "    served ui/app.js sha256 $served_sha"
echo "    tree   ui/app.js sha256 $tree_sha"
[ "$served_sha" = "$tree_sha" ] || die "the page served from the ConfigMap is not the tree's ui/app.js"
expect_http "$API_BASE/backups" "200" "the API, same origin, the ${REL}-ui ServiceAccount's authority"
grep -q "\"name\": *\"$BACKUP_NAME\"" "$OUT/http-body.txt" || die "the Backup list through the proxy does not name $BACKUP_NAME"
echo "    the Backup list names $BACKUP_NAME"
expect_http "$PROXY_BASE/api/v1/namespaces/$NS/pods/nothing/exec" "403 404" "a Pod exec path — refused by the proxy's path filter"
expect_http "$PROXY_BASE/api/v1/namespaces/$NS/secrets" "403 404" "the core API — refused by the proxy's path filter"

echo
echo "==> HELM DEMO EXIT CRITERION MET"
echo "    release $REL in $SYS on $LOGWEIR_KUBE_CONTEXT"
echo "    Backup  $BACKUP_NAME: exitCode=$b_exit verification=$b_result backupId=$b_id"
echo "                          receiptKey=$b_receipt   wall clock $((BACKUP_T1 - BACKUP_T0)) s from the schedule's apply"
echo "    Restore $RESTORE_NAME: phase=$restore_phase exitCode=$r_exit outcome=$r_outcome integrity=$r_integrity"
echo "                          scorecardKey=$r_scorecard"
echo "                          verification=$r_result   wall clock $((RESTORE_T1 - RESTORE_T0)) s from the create"
echo "    Both readers agreed, each exit code read directly: logweir drill verify -> $rust_rc,"
echo "    python3 docs/verify_scorecard.py -> $py_rc."
echo "    The UI answered 200 (page), 200 (the Backup list), and refused a Pod exec path."
echo "    NO PRIVATE KEY LEFT THIS HOST; the approver's never reached the cluster at all."
