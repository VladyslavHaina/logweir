#!/usr/bin/env bash
# `just helm-demo` — THE HELM CHART, WALKED END TO END ON A REAL CLUSTER.
# Task 35 (post-plan); one namespace with workspaces since OD-23 (PROD-19.1).
#
# The chart (`charts/logweir`) has been installed with its optional components
# on — `minio.enabled`, `demoKafka.enabled`, the console in its loopback
# administrator mode (`api.console.mode=localAdmin`) and two workspaces,
# `default` and `other` (`examples/demo.values.yaml`) — and this script walks
# the product over it, in the release namespace and workspace `default`: an
# approver keypair, the chart's archive credential bound to its location, two
# KafkaClusters the controller probes to `status.reachable: true`, the
# cluster-scoped TrustRoster (the chart's signing key, the approver key, the
# target's observed cluster id), a BackupSchedule and the Backup it fires, a
# Backup in `other` naming `default`'s connection refused `WorkspaceMismatch`,
# a Restore whose plan bytes come from the console's own emitter, an approval
# minted on the host with the shipped CLI, the Restore's terminal status with
# `status.outcome: pass`, BOTH readers over the scorecard, the console
# answering through a port-forward (and refusing `default`'s Backup from
# `other`), and a teardown that leaves the cluster holding no `logweir-*`
# namespace.
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
#   LOGWEIR_HELM_NAMESPACE  the release namespace, the install's one namespace.
#                           Default `logweir-system`.
#
# `LOGWEIR_PYTHON` is the repository-wide interpreter knob every script honours
# (`docs/verify_scorecard.py` needs `cryptography`); it is not a parameter of
# this walk. Everything else is derived: every object lives in the release
# namespace, the archive is `s3://kafka-backups/helm-demo` on the chart's
# MinIO, the two brokers are the chart's, the signing key is the chart's
# (its public half read from `logweir-signing-trust`), and the approver key
# lives in `.demo/helm/` (`.gitignore` covers `.demo/`; the teardown deletes it).
#
# ===========================================================================
# STANDING RULE 20: EVERY EXIT CODE ON ITS OWN LINE, NEVER THROUGH A PIPE
# ===========================================================================
# `crates/logweir/tests/gate_lint.rs::gate_lint_no_masked_exit_codes` runs the
# I29 tokeniser over every file under `scripts/`, this one included. The shape
# is `set +e; <tool> … > file; rc=$?; set -e; echo "    rc=$rc  (…)"`, and the
# `logweir` binary is the BARE WORD (put on `$PATH` by the preflight) so the
# tokeniser's literal-prefix rule guards every line that runs it.
#
# NEVER A BACKGROUNDED WAIT. Every wait is a bounded foreground poll. The ONE
# process this script backgrounds is the `kubectl port-forward` of step 9; its
# pid is recorded and the EXIT trap kills it.
#
# ===========================================================================
# WHAT THIS PROVES, AND WHAT IT DOES NOT
# ===========================================================================
# It proves the chart's objects work together in one namespace: the runner
# reaches the chart's brokers by the names the brokers advertise, the archive
# lands on the chart's MinIO, the controller's evidence handle verifies against
# it, the workspace label separates `default` from `other` at the controller
# and the API, and the console serves the page and its product API through a
# port-forward. Run with `examples/author-only.values.yaml` it is AUTHOR-ONLY
# (Global Constraint 37): the images were built and loaded on this host, never
# pulled, so nothing here is evidence for spec §16 clause 1, and no checklist
# row moves.
set -euo pipefail
cd "$(dirname "$0")/.."

LOGWEIR_KUBE_CONTEXT="${LOGWEIR_KUBE_CONTEXT:-docker-desktop}"
LOGWEIR_HELM_RELEASE="${LOGWEIR_HELM_RELEASE:-logweir}"
LOGWEIR_HELM_NAMESPACE="${LOGWEIR_HELM_NAMESPACE:-logweir-system}"
export LOGWEIR_KUBE_CONTEXT LOGWEIR_HELM_RELEASE LOGWEIR_HELM_NAMESPACE
PYTHON="${LOGWEIR_PYTHON:-python3}"

REL="$LOGWEIR_HELM_RELEASE"
NS="$LOGWEIR_HELM_NAMESPACE"
# The walk's workspace is the first of demo.values.yaml's list, the one the
# chart labels its logweir-s3 with; `other` only has to be unable to use it.
WS=default
WS_OTHER=other
OUT=.demo/helm
CHART=charts/logweir
ARCHIVE_BUCKET=kafka-backups
BACKUP_PREFIX=helm-demo
ARCHIVE_URL="s3://${ARCHIVE_BUCKET}/${BACKUP_PREFIX}"
S3_ENDPOINT="http://${REL}-minio.${NS}.svc:9000"
S3_REGION=us-east-1
SOURCE_BOOTSTRAP="${REL}-kafka-source.${NS}.svc.cluster.local:9092"
TARGET_BOOTSTRAP="${REL}-kafka-target.${NS}.svc.cluster.local:9092"
# The kinds this walk creates, all in the release namespace: the preflight
# requires none to exist, and the teardown deletes them while the controller
# still runs (their finalizers need it).
WALK_KINDS=kafkaclusters,backupschedules,backups,restores,approvals
# The console's localAdmin listener: loopback in the pod, reached with
# `kubectl port-forward`. The local port must equal the listen port, because
# the console's origin carries it (docs/install.md §5e).
CONSOLE_PORT="$(sed -n 's/^    port:[[:space:]]*\([0-9][0-9]*\).*$/\1/p' "$CHART/values.yaml" | head -1)"
CONSOLE_BASE="http://127.0.0.1:${CONSOLE_PORT}"
# The `mc` image the chart pins, read from the chart so the two cannot drift.
MC_IMAGE="$(sed -n 's/^  mcImage:[[:space:]]*//p' "$CHART/values.yaml")"
PF_PID=""

echo "helm-demo: kubectl context $LOGWEIR_KUBE_CONTEXT, release $REL in namespace $NS, workspace $WS (STANDING RULE 12)"

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
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" logs deploy/weirkeeper --tail=120 > "$OUT/failed-controller.log" 2>&1
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
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" run "$pod" --restart=Never --image="$MC_IMAGE" --env="MC_HOST_local=http://${MINIO_USER}:${MINIO_PASSWORD}@${REL}-minio.${NS}.svc:9000" --command -- mc cat "local/${ARCHIVE_BUCKET}/$1" > "$OUT/mc-run.log" 2>&1
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
# 10/10 TEARDOWN — the EXIT trap. The walk's objects (while the controller
# still runs, so their finalizers are honoured), the cluster-scoped
# TrustRoster, the release, the release namespace, the CRDs Helm installed from
# crds/ and never removes, and the approver key; then the assertion that no
# `logweir-*` namespace is left. Every rc printed.
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
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" delete "$WALK_KINDS" --all --ignore-not-found --timeout=300s > "$OUT/teardown-objects.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete $WALK_KINDS --all -n $NS — the walk's objects, before the controller goes)"
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" delete trustroster default --ignore-not-found > "$OUT/teardown-roster.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete trustroster default)"
  set +e
  helm uninstall "$REL" -n "$NS" --kube-context "$LOGWEIR_KUBE_CONTEXT" --wait --timeout 5m > "$OUT/teardown-helm.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (helm uninstall $REL -n $NS)"
  cat "$OUT/teardown-helm.log"
  # The retained identity objects (helm.sh/resource-policy: keep) and the
  # walk's evidence Secret go with the namespace.
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" delete ns "$NS" --ignore-not-found --timeout=300s > "$OUT/teardown-ns.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete ns $NS)"
  # HELM INSTALLS crds/ ONCE AND NEVER DELETES IT. A clean cluster is the
  # precondition of the next install, so every CRD the chart ships goes here,
  # read from the chart's own directory.
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" delete -f "$CHART/crds/" --ignore-not-found --timeout=300s > "$OUT/teardown-crds.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl delete -f $CHART/crds/ — every logweir.dev CRD the chart ships)"
  # THE KEY GOES UNCONDITIONALLY. Minted minutes ago, attested by nothing.
  rm -f "$OUT"/*.pem
  echo "    removed $OUT/*.pem (the approver keypair this run minted, the signer's public key)"
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
# 0/10 PREFLIGHT — the release is installed with workspaces default and other,
# both brokers, MinIO and the console are Ready, the seed Jobs succeeded (a
# succeeded hook Job is DELETED by the chart's hook-delete-policy, so
# "succeeded" is asserted as: the release is `deployed`, no failed Job remains,
# and the seeds' EFFECTS are on the brokers), and none of the walk's objects
# exist yet.
# ---------------------------------------------------------------------------
step "0/10 preflight: tools, the release and its workspaces, the brokers, MinIO, the console, the seeds' effects"
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
helm status "$REL" -n "$NS" --kube-context "$LOGWEIR_KUBE_CONTEXT" -o json > "$OUT/helm-status.json" 2> "$OUT/helm-status.err"
rc=$?
set -e
echo "    rc=$rc  (helm status $REL -n $NS -o json)"
[ "$rc" -eq 0 ] || { cat "$OUT/helm-status.err"; die "the release $REL is not installed in $NS. Install it first with the path at the top of $CHART/examples/demo.values.yaml (the console's key Secret, then helm install with --set identity.installationTrust.enabled=false: this walk administers trust with its own TrustRoster, which a fresh install's default TrustPolicy would supersede)"; }
release_status=$("$PYTHON" -c 'import json,sys; print(json.load(open(sys.argv[1]))["info"]["status"])' "$OUT/helm-status.json")
# The JSON is read and then REMOVED: it flattens the whole release manifest
# onto one line, and `scripts/check-unverified-labels.sh` walks `.demo/` —
# untracked, but beside the tree — where that one line would carry a mark and
# an unrelated "verified" together (measured on this walk's first run).
rm -f "$OUT/helm-status.json"
echo "    release status: $release_status"
[ "$release_status" = "deployed" ] || die "the release is '$release_status', not deployed — a failed hook Job leaves the release in that state; \`kubectl -n $NS get jobs\` and \`kubectl logs job/…\` say why"

for target in "deploy/weirkeeper" "statefulset/${REL}-kafka-source" "statefulset/${REL}-kafka-target" "deploy/${REL}-minio" "deploy/${REL}-api"; do
  set +e
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" rollout status "$target" --timeout=300s > "$OUT/rollout-${target##*/}.log" 2>&1
  rc=$?
  set -e
  echo "    rc=$rc  (kubectl rollout status $target --timeout=300s)"
  [ "$rc" -eq 0 ] || { cat "$OUT/rollout-${target##*/}.log"; die "$target is not Ready (rc=$rc); the chart's flags minio.enabled, demoKafka.enabled and api.console.enabled (mode localAdmin) must all be on"; }
done

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get deploy weirkeeper -o jsonpath='{.spec.template.spec.containers[*].env[?(@.name=="LOGWEIR_WORKSPACES")].value}' > "$OUT/workspaces.txt" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl get deploy weirkeeper — LOGWEIR_WORKSPACES: $(cat "$OUT/workspaces.txt"))"
[ "$(cat "$OUT/workspaces.txt")" = "$WS,$WS_OTHER" ] || die "the install's workspaces are '$(cat "$OUT/workspaces.txt")', not $WS,$WS_OTHER; install with $CHART/examples/demo.values.yaml"

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get jobs -o jsonpath='{range .items[*]}{.metadata.name}{" failed="}{.status.failed}{" succeeded="}{.status.succeeded}{"\n"}{end}' > "$OUT/jobs.txt" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl get jobs — a succeeded seed Job is deleted by its hook policy, so this lists leftovers only)"
cat "$OUT/jobs.txt"
if grep -q 'failed=[1-9]' "$OUT/jobs.txt"; then
  die "a seed Job failed; \`kubectl -n $NS logs job/<name>\` says why"
fi

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" exec "statefulset/${REL}-kafka-source" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$SOURCE_BOOTSTRAP" --list > "$OUT/source-topics.txt" 2> "$OUT/source-topics.err"
rc=$?
set -e
echo "    rc=$rc  (kafka-topics.sh --list on the source, via kubectl exec) -> $(tr '\n' ' ' < "$OUT/source-topics.txt")"
[ "$rc" -eq 0 ] || { cat "$OUT/source-topics.err"; die "could not list the source's topics (rc=$rc)"; }
grep -qx 'orders' "$OUT/source-topics.txt" || die "the seed Job did not create \`orders\` on the source"
grep -qx 'payments' "$OUT/source-topics.txt" || die "the seed Job did not create \`payments\` on the source"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" exec "statefulset/${REL}-kafka-target" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$TARGET_BOOTSTRAP" --list > "$OUT/target-topics.txt" 2> "$OUT/target-topics.err"
rc=$?
set -e
echo "    rc=$rc  (kafka-topics.sh --list on the target, via kubectl exec) -> $(tr '\n' ' ' < "$OUT/target-topics.txt")"
[ "$rc" -eq 0 ] || { cat "$OUT/target-topics.err"; die "could not list the target's topics (rc=$rc)"; }
grep -qx 'logweir.scratch' "$OUT/target-topics.txt" || die "the seed Job did not create the marker topic \`logweir.scratch\` on the target — phase 0 refuses a scratch restore without it"

set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get "$WALK_KINDS" -o name > "$OUT/objects-check.txt" 2> "$OUT/objects-check.err"
rc=$?
set -e
echo "    rc=$rc  (kubectl get $WALK_KINDS -n $NS — none may exist yet: check-then-take)"
[ "$rc" -eq 0 ] || { cat "$OUT/objects-check.err"; die "could not list the walk's kinds in $NS (rc=$rc)"; }
[ ! -s "$OUT/objects-check.txt" ] || { cat "$OUT/objects-check.txt"; die "$NS already holds Logweir objects; somebody else's run, or a teardown that did not finish. The teardown deletes every object of these kinds, so the walk will not start over them."; }

# THE CHART'S MinIO ROOT CREDENTIAL, read back from the release's own Secret so
# this script and the chart cannot disagree about it.
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get secret "${REL}-minio-root" -o json > "$OUT/minio-root.json" 2>&1
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
# 1/10 THE APPROVER KEYPAIR, AND THE SIGNER'S PUBLIC KEY. The runner Jobs sign
#      with the chart's `logweir-signing-key` (the identity hook made it); its
#      public half and key id are in the `logweir-signing-trust` ConfigMap
#      (identity.publicConfigMapName), written for TrustRoster/default.
# ---------------------------------------------------------------------------
step "1/10 minting the approver keypair into $OUT/; the chart's signing key, read from logweir-signing-trust"
openssl ecparam -genkey -name prime256v1 -noout > "$OUT/approver.der.pem"
openssl pkcs8 -topk8 -nocrypt -in "$OUT/approver.der.pem" -out "$OUT/approver.pem"
rm -f "$OUT/approver.der.pem"
openssl ec -in "$OUT/approver.pem" -pubout -out "$OUT/approver.pub.pem" 2>/dev/null
chmod 600 "$OUT/approver.pem"
APPROVER_KEY_ID=$(openssl ec -pubin -in "$OUT/approver.pub.pem" -outform DER 2>/dev/null | openssl dgst -sha256 -hex | awk '{print $NF}')
[ -n "$APPROVER_KEY_ID" ] || die "could not compute the approver key id"
echo "    approver key id: $APPROVER_KEY_ID (minted seconds ago, attested by nothing; the teardown deletes it)"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get configmap logweir-signing-trust -o json > "$OUT/signing-trust.json" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl get configmap logweir-signing-trust -o json — public material only)"
[ "$rc" -eq 0 ] || { cat "$OUT/signing-trust.json"; die "the chart's logweir-signing-trust ConfigMap is missing (rc=$rc); is identity.enabled on?"; }
SIGNING_KEY_ID=$("$PYTHON" - "$OUT/signing-trust.json" "$OUT/signing.pub.pem" <<'PY'
import json, sys
data = json.load(open(sys.argv[1])).get("data") or {}
open(sys.argv[2], "w").write(data.get("signing.pub.pem", ""))
print(data.get("key-id", ""))
PY
)
rm -f "$OUT/signing-trust.json"
[ -n "$SIGNING_KEY_ID" ] && [ -s "$OUT/signing.pub.pem" ] || die "logweir-signing-trust carries no key-id or signing.pub.pem; the identity hook has not run"
echo "    signing key id:  $SIGNING_KEY_ID (the chart's, generated by the identity hook)"

# ---------------------------------------------------------------------------
# 2/10 THE CREDENTIALS: the chart's logweir-s3 is in workspace default and is
#      bound to its location; the controller's read-only evidence credential.
# ---------------------------------------------------------------------------
step "2/10 logweir-s3 in workspace $WS, bound to $ARCHIVE_URL; logweir-evidence-ro for the controller"
s3_ws=$(read_field secret logweir-s3 '{.metadata.labels.logweir\.dev/workspace}' 'secret/logweir-s3 label logweir.dev/workspace')
[ "$s3_ws" = "$WS" ] || die "the chart's logweir-s3 is labelled workspace '${s3_ws:-<absent>}', not $WS; a runner Job of $WS may not mount it"

# BOUND to the location it is used at (FX-20, docs/kubernetes.md "Credential
# binding"): a runner refuses a Secret whose `logweir-binding` is not the
# binding of the location it dials, `CredentialBindingMismatch`, before any
# request. The walk's Backups (the controller's AWS_* route, which the chart
# renders from this same MinIO endpoint and region) and its Restore's plan
# `source` and `evidence` are one location, bucket and endpoint, so one binding
# covers all three. `--apply` prints what it binds; the walk states the
# endpoint it means, and the tool refuses any other.
BIND_CONFIRM="$S3_ENDPOINT bucket=$ARCHIVE_BUCKET region=$S3_REGION pathStyle=true allowHttp=true"
set +e
"$PYTHON" scripts/bind-credential.py --context "$LOGWEIR_KUBE_CONTEXT" --namespace "$NS" --location "$ARCHIVE_URL" --endpoint "$S3_ENDPOINT" --region "$S3_REGION" --path-style true --allow-http true --secret logweir-s3 --apply --confirm-endpoint "$BIND_CONFIRM" > "$OUT/bind-s3.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (scripts/bind-credential.py --location $ARCHIVE_URL --secret logweir-s3 --apply --confirm-endpoint '$BIND_CONFIRM')"
cat "$OUT/bind-s3.log"
[ "$rc" -eq 0 ] || die "could not bind logweir-s3 to $ARCHIVE_URL (rc=$rc); see $OUT/bind-s3.log"

# The CONTROLLER's read-only evidence credential. MinIO gives no second
# identity out of the box, so the demo uses the same root pair under a
# different Secret name: WHAT THIS PROVES IS THE PROJECTION, not the IAM
# policy. An adopter gives this principal `s3:GetObject` on the evidence prefix
# and nothing else. No workspace label: the controller reads it from its env,
# and no object may name it.
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" create secret generic logweir-evidence-ro --from-literal=access-key-id="$MINIO_USER" --from-literal=secret-access-key="$MINIO_PASSWORD" > "$OUT/secret-evidence.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (secret/logweir-evidence-ro)"
[ "$rc" -eq 0 ] || die "could not create logweir-evidence-ro (rc=$rc)"
# The controller reads it from its OWN ENV, fixed at container start.
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" rollout restart deploy/weirkeeper > "$OUT/restart.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl rollout restart deploy/weirkeeper — env is fixed at container start)"
[ "$rc" -eq 0 ] || die "could not restart the controller (rc=$rc)"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" rollout status deploy/weirkeeper --timeout=300s > "$OUT/rollout2.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl rollout status deploy/weirkeeper, after the evidence Secret)"
[ "$rc" -eq 0 ] || die "the controller did not come back (rc=$rc)"

# ---------------------------------------------------------------------------
# 3/10 TWO KafkaClusters AT THE CHART'S BROKERS, AND `reachable: true` ON BOTH.
# ---------------------------------------------------------------------------
step "3/10 KafkaCluster source ($SOURCE_BOOTSTRAP) and target ($TARGET_BOOTSTRAP) in workspace $WS -> status.reachable"
cat > "$OUT/kafkaclusters.yaml" <<YAML
apiVersion: logweir.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: source
  namespace: $NS
  labels:
    logweir.dev/workspace: $WS
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
  labels:
    logweir.dev/workspace: $WS
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
[ "$SOURCE_CLUSTER_ID" != "$TARGET_CLUSTER_ID" ] || die "the two brokers report the SAME cluster id ($SOURCE_CLUSTER_ID); a scratch restore must not write into the cluster it backs up"
echo "    two clusters, two ids: source $SOURCE_CLUSTER_ID, target $TARGET_CLUSTER_ID"

# ---------------------------------------------------------------------------
# 4/10 THE CLUSTER-SCOPED TrustRoster `default`, from the sample's shape. AFTER
#      step 3, because its `allowedClusterIds` is the restore-target allowlist
#      phase 0 reads (the controller copies it into each Restore's approval
#      bundle) and must carry the target's OBSERVED id; `spec` is immutable.
# ---------------------------------------------------------------------------
step "4/10 TrustRoster default — the approver key id, the chart's signing key, allowedClusterIds [$TARGET_CLUSTER_ID]"
"$PYTHON" - "$OUT/signing.pub.pem" "$SIGNING_KEY_ID" "$OUT/approver.pub.pem" "$APPROVER_KEY_ID" "$TARGET_CLUSTER_ID" "$OUT/trustroster.yaml" <<'PY'
import sys, textwrap
sig_pem, sig_id, app_pem, app_id, target_id, out = sys.argv[1:7]
def block(path):
    return textwrap.indent(open(path).read().rstrip("\n"), " " * 8)
doc = f"""apiVersion: logweir.dev/v1alpha1
kind: TrustRoster
metadata:
  name: default
spec:
  allowedClusterIds: ["{target_id}"]
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
# 5/10 A BackupSchedule ON */2, AND THE Backup IT FIRES — Succeeded, exitCode 0,
#      receipt Valid. Then the schedule is suspended so the chosen Backup is
#      stable for the Restore (the demo's step 8 reasoning). Then the boundary:
#      a Backup in workspace `other` naming `default`'s connection is refused
#      `WorkspaceMismatch` and gets no Job.
# ---------------------------------------------------------------------------
step "5/10 BackupSchedule */2 * * * * over orders and payments into $ARCHIVE_URL, the Backup it fires, and a Backup in $WS_OTHER refused"
cat > "$OUT/backupschedule.yaml" <<YAML
apiVersion: logweir.dev/v1alpha1
kind: BackupSchedule
metadata:
  name: helm
  namespace: $NS
  labels:
    logweir.dev/workspace: $WS
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
  kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get backups -l "logweir.dev/workspace=$WS" -o name > "$OUT/backups.txt" 2>/dev/null
  rc=$?
  set -e
  BACKUP_NAME=$(sed -n 's|^backup.logweir.dev/||p' "$OUT/backups.txt" | head -1)
  [ -n "$BACKUP_NAME" ] && break
  sleep 5
done
echo "    rc=$rc  (kubectl get backups -l logweir.dev/workspace=$WS -o name)"
[ -n "$BACKUP_NAME" ] || die "no Backup labelled workspace $WS appeared within five minutes. The BackupSchedule reconciler creates one object per due slot, named <schedule>-<slot>, with the schedule's workspace label."
echo "    the schedule fired: Backup/$BACKUP_NAME, in workspace $WS"

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

# THE BOUNDARY, AT THE CONTROLLER. The same run, labelled `other`: its
# connection and its credential are `default`'s, so the workspace gate refuses
# it before any Job (crates/weirkeeper/src/workspace.rs).
cat > "$OUT/backup-other.yaml" <<YAML
apiVersion: logweir.dev/v1alpha1
kind: Backup
metadata:
  name: helm-other
  namespace: $NS
  labels:
    logweir.dev/workspace: $WS_OTHER
spec:
  sourceRef:
    name: source
  topics: ["orders"]
  archive:
    url: $ARCHIVE_URL
    secretRef:
      name: logweir-s3
  triggeredBy: manual
  deadlineSeconds: 600
YAML
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" create -f "$OUT/backup-other.yaml" > "$OUT/backup-other.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl create -f backup-other.yaml — Backup/helm-other in $WS_OTHER, naming $WS's KafkaCluster source and logweir-s3)"
[ "$rc" -eq 0 ] || { cat "$OUT/backup-other.log"; die "could not create Backup/helm-other (rc=$rc)"; }
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" wait --for=jsonpath='{.status.conditions[0].reason}'=WorkspaceMismatch backup/helm-other --timeout=120s > "$OUT/backup-other-wait.log" 2>&1
rc=$?
set -e
echo "    rc=$rc  (kubectl wait --for=jsonpath={.status.conditions[0].reason}=WorkspaceMismatch backup/helm-other)"
if [ "$rc" -ne 0 ]; then
  dump_run backup helm-other
  die "Backup/helm-other in $WS_OTHER was not refused WorkspaceMismatch"
fi
other_message=$(read_field backup helm-other '{.status.conditions[0].message}' 'status.conditions[0].message')
[ -n "$other_message" ] || die "the WorkspaceMismatch condition carries no message"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" get jobs -l "logweir.dev/workspace=$WS_OTHER" -o name > "$OUT/jobs-other.txt" 2> "$OUT/jobs-other.err"
rc=$?
set -e
echo "    rc=$rc  (kubectl get jobs -l logweir.dev/workspace=$WS_OTHER -o name) -> $(wc -l < "$OUT/jobs-other.txt" | tr -d ' ') Job(s)"
[ "$rc" -eq 0 ] || die "could not list $WS_OTHER's Jobs (rc=$rc)"
[ ! -s "$OUT/jobs-other.txt" ] || { cat "$OUT/jobs-other.txt"; die "a Job was created in $WS_OTHER for a refused Backup"; }

# ---------------------------------------------------------------------------
# 6/10 THE Restore — from the most recent Backup, its planBytes rendered by the
#      console's own renderer (console/scripts/emit-restore-body.mjs), a SCRATCH
#      restore onto the chart's target broker, whose marker topic the seed
#      created. The emitter labels it with the workspace.
# ---------------------------------------------------------------------------
step "6/10 the Restore: the console's own renderer renders the plan bytes; kubectl create -f the body"
PIT_RFC3339=$("$PYTHON" -c 'import datetime as d
print((d.datetime.now(d.timezone.utc).replace(microsecond=0) + d.timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ"))')
SAMPLE_START_RFC3339=$("$PYTHON" -c 'import datetime as d
print((d.datetime.now(d.timezone.utc).replace(microsecond=0) - d.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ"))')
[ -n "${PIT_RFC3339:-}" ] || die "could not compute the recovery point"
echo "    recovery point: $PIT_RFC3339   sample window from: $SAMPLE_START_RFC3339 (the seed ran at install time)"
cat > "$OUT/plan-fields.json" <<JSON
{
  "workspace": "$WS",
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
node --experimental-strip-types console/scripts/emit-restore-body.mjs --out "$OUT/" > "$OUT/emit.out" 2> "$OUT/emit.err"
rc=$?
set -e
echo "    rc=$rc  (node --experimental-strip-types console/scripts/emit-restore-body.mjs --out $OUT/)"
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
echo "    rc=$rc  (kubectl create -f restore-body.json — Restore/$RESTORE_NAME in $WS, approvalRef -> $APPROVAL_NAME, which does not exist yet)"
[ "$rc" -eq 0 ] || { cat "$OUT/restore-create.log"; die "could not create the Restore (rc=$rc)"; }

# ---------------------------------------------------------------------------
# 7/10 THE APPROVAL, MINTED ON THE HOST WITH THE SHIPPED CLI, AND THE Approval.
# ---------------------------------------------------------------------------
step "7/10 logweir drill approve on the HOST, then the Approval object in $WS (the controller writes the Restore's bundle from it)"
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

"$PYTHON" - "$OUT/approval.json" "$OUT/approval.sig" "$NS" "$WS" "$APPROVAL_NAME" "$RESTORE_NAME" "$APPROVED_HASH" "$OUT/approval-object.yaml" <<'PY'
import sys, textwrap
approval_path, sidecar_path, ns, ws, name, subject, plan_hash, out = sys.argv[1:9]
approval = open(approval_path).read()
sidecar = open(sidecar_path).read()
doc = f"""apiVersion: logweir.dev/v1alpha1
kind: Approval
metadata:
  name: {name}
  namespace: {ns}
  labels:
    logweir.dev/workspace: {ws}
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
echo "    rc=$rust_rc  (logweir drill verify --payload-type scorecard, against the chart's signing key)"
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
# 9/10 THE CONSOLE, THROUGH A PORT-FORWARD: the page 200 and the tree's own
#      app.js, the session 200 in localAdmin mode, both workspaces listed,
#      `default`'s Backup through the product API and NOT through `other`, and
#      a Kubernetes path it does not serve.
# ---------------------------------------------------------------------------
step "9/10 the console: kubectl port-forward deploy/${REL}-api ${CONSOLE_PORT}:${CONSOLE_PORT}, then seven fetches"
[ -n "$CONSOLE_PORT" ] || die "could not read api.console.port from $CHART/values.yaml"
set +e
kubectl --context "$LOGWEIR_KUBE_CONTEXT" -n "$NS" port-forward "deploy/${REL}-api" "${CONSOLE_PORT}:${CONSOLE_PORT}" > "$OUT/port-forward.log" 2>&1 &
rc=$?
set -e
PF_PID=$!
echo "    rc=$rc  (kubectl port-forward deploy/${REL}-api ${CONSOLE_PORT}:${CONSOLE_PORT}, backgrounded; pid $PF_PID — killed by the trap)"
up=""
for _ in $(seq 1 30); do
  set +e
  curl -sS -o /dev/null "$CONSOLE_BASE/ui/" > "$OUT/pf-poll.txt" 2>&1
  rc=$?
  set -e
  [ "$rc" -eq 0 ] && up=yes && break
  sleep 1
done
echo "    rc=$rc  (curl $CONSOLE_BASE/ui/ — the readiness poll, up to 30 s)"
[ -n "$up" ] || die "the port-forward never answered on ${CONSOLE_PORT}; see $OUT/port-forward.log"
echo
echo "    WHOSE AUTHORITY: the console in localAdmin mode binds loopback in its pod; whoever may"
echo "    port-forward to deploy/${REL}-api acts as its one administrator, in every workspace."
echo "    The page holds no credential."
echo
expect_http "$CONSOLE_BASE/ui/" "200" "the page itself"
# The page names its one script under assets/; that file is served, cached as
# immutable (its name carries its hash). `scripts/check-image-api.sh` compares
# every file of the image's /ui with the tree's console build.
script_path=$(sed -n 's/.*<script[^>]* src="\.\/\(assets\/index-[A-Za-z0-9_-]*\.js\)".*/\1/p' "$OUT/http-body.txt" | head -1)
[ -n "$script_path" ] || die "the page served at /ui/ names no ./assets/index-*.js script"
expect_http "$CONSOLE_BASE/ui/$script_path" "200" "the console's script"
set +e
curl -sS -I "$CONSOLE_BASE/ui/$script_path" > "$OUT/script-headers.txt" 2>&1
rc=$?
set -e
[ "$rc" -eq 0 ] || die "curl -I exited $rc for the console's script"
grep -qi '^cache-control: public, max-age=31536000, immutable' "$OUT/script-headers.txt" \
  || die "the console's script is not served as immutable; see $OUT/script-headers.txt"
echo "    ui/$script_path is served immutable"
expect_http "$CONSOLE_BASE/api/v1/session" "200" "the session"
grep -q '"authenticationMode": *"localAdmin"' "$OUT/http-body.txt" || die "the session does not say localAdmin"
echo "    the session is the console's one administrator (localAdmin)"
expect_http "$CONSOLE_BASE/api/v1/workspaces" "200" "the workspaces the administrator holds"
for ws in "$WS" "$WS_OTHER"; do
  grep -q "\"name\": *\"$ws\"" "$OUT/http-body.txt" || die "GET /api/v1/workspaces does not list $ws"
done
echo "    both workspaces are listed: $WS, $WS_OTHER"
expect_http "$CONSOLE_BASE/api/v1/workspaces/$WS/backups/$BACKUP_NAME" "200" "$WS's Backup, through the product API"
expect_http "$CONSOLE_BASE/api/v1/workspaces/$WS_OTHER/backups/$BACKUP_NAME" "404" "the same Backup asked for in $WS_OTHER — another workspace's object is not found"
expect_http "$CONSOLE_BASE/api/v1/workspaces/$WS/secrets" "404" "a Kubernetes core kind — not a product route"

echo
echo "==> HELM DEMO EXIT CRITERION MET"
echo "    release $REL in $NS on $LOGWEIR_KUBE_CONTEXT, workspace $WS"
echo "    Backup  $BACKUP_NAME: exitCode=$b_exit verification=$b_result backupId=$b_id"
echo "                          receiptKey=$b_receipt   wall clock $((BACKUP_T1 - BACKUP_T0)) s from the schedule's apply"
echo "    Backup  helm-other in $WS_OTHER: WorkspaceMismatch, no Job"
echo "    Restore $RESTORE_NAME: phase=$restore_phase exitCode=$r_exit outcome=$r_outcome integrity=$r_integrity"
echo "                          scorecardKey=$r_scorecard"
echo "                          verification=$r_result   wall clock $((RESTORE_T1 - RESTORE_T0)) s from the create"
echo "    Both readers agreed, each exit code read directly: logweir drill verify -> $rust_rc,"
echo "    python3 docs/verify_scorecard.py -> $py_rc."
echo "    The console answered 200 (page, session, workspaces, $WS's Backup), and 404 for that"
echo "    Backup in $WS_OTHER and for a core kind."
echo "    NO PRIVATE KEY LEFT THIS HOST; the approver's never reached the cluster at all."
