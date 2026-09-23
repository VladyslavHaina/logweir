#!/usr/bin/env bash
# PLAT-17.2 live proof driver — see README.md.
#
#   run.sh all       the shared-console proof: namespaces + emulated chart RBAC,
#                    the can-i matrix, the scoped source-built controller, the
#                    shared console behind the TLS stand-in ingress and the local
#                    OIDC mock (live_p172.py), then localAdmin unchanged.
#                    Creates cluster-scoped ClusterRoles/Bindings: HOLD THE LOCK.
#   run.sh expired   the expired-session rows only (live_p172_expired.py):
#                    namespaced, no cluster-scoped object, no lock needed.
#
# Everything is written under $OUT (0700). cleanup.sh runs on exit unless
# P172_KEEP=1, and deletes only objects labelled logweir.dev/test-owner=$OWNER.
set -uo pipefail
mode="${1:-all}"
export TS="${TS:-$(date -u +%Y%m%dt%H%M%Sz)}"
export OUT="${OUT:-${P172_ARTIFACTS:-/tmp/logweir-roadmap-run/claude/artifacts/plat17-2-live}/$TS}"
mkdir -p "$OUT" && chmod 700 "$OUT"
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
exec > >(tee "$OUT/run.log") 2>&1
echo "== PLAT-17.2 live run $TS ($mode) at $(git -C "$REPO" rev-parse HEAD), owner $OWNER, prefix $PREFIX =="
shasum -a 256 "$API_BIN" "$WK_BIN" 2>/dev/null
echo "lab controller (shared fixture, read-only):"
$K -n logweir-scram-local get pods -l app.kubernetes.io/component=control-plane -o jsonpath='{range .items[*]}{.metadata.name} {.status.containerStatuses[0].imageID}{"\n"}{end}'

if [ "$mode" = expired ]; then
  $T 900 "$PY" "$H/live_p172_expired.py"
  exit $?
fi
[ "$mode" = all ] || { echo "usage: run.sh [all|expired]" >&2; exit 2; }

finish() { [ "${P172_KEEP:-0}" = 1 ] && echo "kept on request (P172_KEEP=1)" || bash "$H/cleanup.sh"; }
trap finish EXIT

echo "== 1. namespaces, emulated chart RBAC, ServiceAccount kubeconfigs"
bash "$H/setup.sh" || exit 1

echo "== 2. the auth can-i matrix (cani.sh)"
bash "$H/cani.sh" > "$OUT/cani.txt" 2>&1; cani_rc=$?
cat "$OUT/cani.txt"

echo "== 3. the source-built controller, SCOPED, as the scoped ServiceAccount (40 s)"
KUBECONFIG=$OUT/kubeconfig-weirkeeper LOGWEIR_WATCH_NAMESPACES="$A,$B" \
  LOGWEIR_POLICY_CONFIGMAP="" LOGWEIR_INSTALLATION_NAMESPACE="$CTL" RUST_LOG=info \
  "$WK_BIN" > "$OUT/weirkeeper-scoped.log" 2>&1 &
wk=$!
sleep 40
kill -TERM "$wk" 2>/dev/null
for _ in 1 2 3 4 5 6 7 8 9 10; do kill -0 "$wk" 2>/dev/null || break; sleep 1; done
kill -KILL "$wk" 2>/dev/null
wait "$wk" 2>/dev/null; echo "controller exit: $?"
"$PY" "$H/summarize_controller.py" "$OUT/weirkeeper-scoped.log" > "$OUT/weirkeeper-scoped.summary.txt"
cat "$OUT/weirkeeper-scoped.summary.txt"

echo "== 4. keys, TLS, configuration (all minted for this run, 0600, removed by cleanup.sh)"
umask 077
printf 'version: 1\nkey: "%s"\n' "$(openssl rand -base64 32)" > "$OUT/session.key"
printf 'version: 1\nkey: "%s"\n' "$(openssl rand -base64 32)" > "$OUT/cursor.key"
openssl rand -hex 24 | tr -d '\n' > "$OUT/client-secret"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1 \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost" -keyout "$OUT/tls.key" -out "$OUT/tls.crt" 2>/dev/null
cat > "$OUT/console.yaml" <<YAML
mode: shared
listen: "0.0.0.0:18484"
publicBaseUrl: "https://localhost:18443"
uiDirectory: $REPO/ui
oidc:
  issuer: http://127.0.0.1:18555
  clientId: logweir-console
  clientSecretFile: $OUT/client-secret
  allowedAlgorithms: [ES256]
  scopes: [openid, profile, groups]
  groupsClaim: groups
  displayNameClaim: name
  insecureLoopbackIssuer: true
roles:
  revision: live-p172-1
  bindings:
    - {role: viewer, namespace: $A, groups: [g-viewers-a]}
    - {role: operator, namespace: $A, groups: [g-operators-a]}
    - {role: approver, namespace: $A, groups: [g-approvers-a]}
    - {role: administrator, namespace: $B, groups: [g-admins-b]}
sessionKey: {file: $OUT/session.key, expectedVersion: 1}
cursorKey: {file: $OUT/cursor.key, expectedVersion: 1}
sessionMaxAgeSeconds: 900
trustedProxyCidrs: ["127.0.0.1/32"]
requireTrustedProxy: true
namespaces: [$A, $B]
kubernetes:
  source: kubeconfig
  kubeconfig: $OUT/kubeconfig-logweir-api
  context: p172-logweir-api
  principal: system:serviceaccount:$CTL:logweir-api
YAML

echo "== 5. the shared console through the TLS ingress"
LAN_IP="$(ipconfig getifaddr en0 || true)" $T 900 "$PY" "$H/live_p172.py" > "$OUT/harness.txt" 2>&1; harness_rc=$?
cat "$OUT/harness.txt"

echo "== 6. objects left in the namespaces (no Job/Pod/Secret created by the API)"
$K get kafkaclusters,restores,jobs,pods,secrets -n "$A" --show-managed-fields -o custom-columns='KIND:.kind,NAME:.metadata.name,MANAGERS:.metadata.managedFields[*].manager' > "$OUT/objects-a.txt" 2>&1
cat "$OUT/objects-a.txt"

echo "== 7. localAdmin mode, unchanged: loopback listener, the same ServiceAccount identity"
head -c 32 /dev/urandom > "$OUT/cursor-local.key"
cat > $OUT/local.yaml <<YAML
mode: localAdmin
listen: "127.0.0.1:18486"
publicOrigin: "http://127.0.0.1:18486"
uiDirectory: $REPO/ui
localAdmin: {subject: admin}
namespaces: [$A, $B]
kubernetes: {source: kubeconfig, kubeconfig: $OUT/kubeconfig-logweir-api, context: p172-logweir-api}
cursorKeyFile: $OUT/cursor-local.key
YAML
RUST_LOG=info "$API_BIN" --config "$OUT/local.yaml" > "$OUT/api-local.log" 2>&1 &
lp=$!
$T 120 "$PY" - "$A" "$B" "$Z" > "$OUT/local.txt" 2>&1 <<'PY'
import http.client, json, socket, subprocess, sys, time
A, B, Z = sys.argv[1], sys.argv[2], sys.argv[3]
for _ in range(100):
    try: socket.create_connection(("127.0.0.1", 18486), 1).close(); break
    except OSError: time.sleep(0.2)
def req(m, p, body=None, headers=None):
    c = http.client.HTTPConnection("127.0.0.1", 18486, timeout=15)
    h = {"Host": "127.0.0.1:18486"}; h.update(headers or {})
    c.request(m, p, body=json.dumps(body) if body else None, headers=h)
    r = c.getresponse(); return r.status, json.loads(r.read() or b"{}")
ok = True
s, j = req("GET", "/api/v1/session"); print("session", s, j.get("authenticationMode"), j.get("actor", {}).get("id")); ok &= s == 200 and j["authenticationMode"] == "localAdmin"
s, j = req("GET", f"/api/v1/namespaces/{A}/connections"); print("list A", s, [i["name"] for i in j.get("items", [])]); ok &= s == 200
body = {"role": "source", "bootstrapServers": ["kafka-source.kafka.svc.cluster.local:9096"], "auth": {"mode": "scramSha512", "username": "u", "credentialRef": {"name": "s"}, "tls": True}}
s, j = req("POST", f"/api/v1/namespaces/{B}/connections", body, {"Origin": "http://127.0.0.1:18486", "Content-Type": "application/json", "Idempotency-Key": "p172-local-0001"})
name = j.get("item", {}).get("name"); print("create B", s, name); ok &= s == 201
ann = json.loads(subprocess.run(["kubectl", "--context", "docker-desktop", "get", "kafkaclusters", name, "-n", B, "-o", "json"], capture_output=True, text=True, timeout=30).stdout)["metadata"]["annotations"]
att = {k: v for k, v in ann.items() if k.startswith("api.logweir.dev/")}; print("annotations", json.dumps(att))
ok &= att.get("api.logweir.dev/authentication-mode") == "localAdmin" and att.get("api.logweir.dev/actor") == "urn:logweir:local-admin#admin" and att.get("api.logweir.dev/kubernetes-principal") == "kubeconfig-context:p172-logweir-api"
s, j = req("GET", f"/api/v1/namespaces/{Z}/connections"); print("unconfigured namespace", s, j.get("code")); ok &= s == 403 and j.get("code") == "namespace_forbidden"
s, j = req("GET", f"/api/v1/namespaces/{A}/connections", headers={"Host": "evil.example:18486"}); print("foreign Host", s, j.get("code")); ok &= s == 421
print("LOCALADMIN", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
PY
local_rc=$?
cat "$OUT/local.txt"
kill -TERM $lp; for _ in 1 2 3 4 5; do kill -0 $lp 2>/dev/null || break; sleep 1; done; kill -KILL $lp 2>/dev/null
rm -f "$OUT/cursor-local.key"

echo "cani_rc=$cani_rc harness_rc=$harness_rc local_rc=$local_rc"
[ "$cani_rc" = 0 ] && [ "$harness_rc" = 0 ] && [ "$local_rc" = 0 ]
